# 多代备份库运维说明（circulation_flow.backup）

`circulation_flow.backup` 为图书流通 SQLite 交付物（JSONL 管道普通库、供应商
整包采集库）提供本机多代备份。仅使用标准库。一个备份库 = 一个持久 catalog
（`catalog.sqlite`）+ 一组不可变数据库对象（`objects/<sha256>.db`）。恢复是
纯字节复制，不重算业务；普通库与整包库的表、视图、索引及全部 BLOB（含
`intake_runs.package_bytes` 等来源原文）原样保留。

## 命令与 Python 调用

```sh
python3 -m circulation_flow.backup backup  --store STORE --source DB --request-id REQ
python3 -m circulation_flow.backup restore --store STORE --generation N --target PATH
python3 -m circulation_flow.backup prune   --store STORE --keep N
python3 -m circulation_flow.backup list    --store STORE
python3 -m circulation_flow.backup check   --store STORE
```

Python 入口同名：`backup(store, source, request_id)`、
`restore(store, generation, target)`、`prune(store, keep)`、
`list_generations(store)`、`check_store(store)`，另有上下文管理器
`hold_generation(store, generation)` 可为自定义长操作提供与恢复相同的
防清理保护。stdout 输出一行键排序 JSON；参数错误或失败返回非零，原因打印到
stderr。

## 备份：幂等请求与一致快照

- `request_id` 是幂等键（catalog 唯一约束）。已提交的请求重试直接返回原代
  （`deduplicated=true`），**不会重新打开已变化的源**；两个进程并发提交同一
  `request_id` 时由唯一约束裁决，后到者返回先到者的代，全库恰有一代。
- 快照经 SQLite 在线备份 API 获取：拷贝源的已提交状态，**包括只存在于源
  WAL 中的已提交事务**；未提交事务不会进入快照。源以 `mode=ro` 只读打开；
  若源带热 WAL，其目录需可写（SQLite 恢复 WAL 的常规要求）。
- 代编号 `generation_id` 从 1 起单调递增（`MAX+1` 在发布临界区内分配，
  prune 至少保留 1 代，故编号不会回退重用）。内容相同的快照共享同一不可变
  对象文件（按 SHA-256 寻址）。

## 提交顺序与崩溃语义（设计核心）

backup 的持久化操作严格按序执行：

1. 快照写入 `tmp/obj-*.part` 并 fsync（文件持 flock，标记为活跃）；
2. 获取发布锁 `locks/publish.lock`（flock）；
3. 对象 rename 进 `objects/<sha256>.db` 并 fsync 对象目录 —— **对象先持久**；
4. catalog 单事务 `BEGIN IMMEDIATE; INSERT; COMMIT`
   （`synchronous=FULL`，提交即 fsync）—— **catalog 行是提交点**；
5. 释放发布锁。

因此在任意点 SIGKILL 后重启：

- 有 catalog 行 ⇒ 对象必然存在且完整 ⇒ 该代完全可用；
- 无 catalog 行 ⇒ 该代不存在；残留仅为 `tmp/` 暂存或未引用的孤儿对象，
  由 prune 清扫。**恢复与 list 只读已提交行，永远看不到半代**；
- 无需任何启动恢复扫描：catalog 是唯一事实源，孤儿 = 对象集合减 catalog
  引用（`check` 报告、`prune` 清除）。

restore 的顺序：先取持有锁并复核 catalog 行仍存在 → 对象流式拷到目标目录的
暂存文件，核对 大小+SHA-256 → `PRAGMA integrity_check` → fsync → 删除目标旧
`-wal`/`-shm` 侧车文件 → `os.replace` 原子替换 → fsync 目录。**替换点之前的
任何失败都完整保留旧目标**（含其 WAL）；暂存文件是本工具自有垃圾，下次恢复
同一目标时经 flock 探测清扫。替换前一刻删除侧车文件的极窄崩溃窗口内，旧目标
主文件仍是合法库（最后检查点状态）；新目标绝不可能重放到旧 WAL。

prune 对每代的顺序：试探持有锁 → `DELETE` catalog 行并提交（**先下架**）→
发布锁内复核引用计数为 0 才删对象 → 删持有锁文件。崩溃只会在下架后留下孤儿
对象，下次 prune 清扫，清理可任意重试。

## prune 与恢复持有的并发协议

- `prune --keep N` 保留最近 N 个已提交代（N ≥ 1），其余删除；正在恢复的代
  被跳过并报告在 `skipped_locked`。
- 持有 = 对 `holds/<代>.hold` 文件的 flock 排他锁。restore 全程持有；
  prune 对候选代非阻塞试锁，锁占用即跳过。**持有进程崩溃（含 SIGKILL）后
  锁由内核自动释放，prune 立即可清理该代——不靠任何超时猜存活**。
- 长恢复只持有其单代锁，不持有发布锁或 catalog 写锁，因此恢复期间新备份
  与其他代的清理照常完成（测试覆盖：持代期间 backup 提交新代、prune 跳过
  被持代并清理其余代）。
- 清扫只删本库自有垃圾：对象必须匹配 `objects/<64位小写hex>.db` 且未被
  catalog 引用；暂存必须匹配 `tmp/obj-*.part` 且 flock 空闲（活跃备份的暂存
  文件持锁，不会被误删）。目录内任何外来文件一律不动。

## restore 的安全约束

- 目标必须**离线**（无进程正在使用）；工具以原子替换发布，不处理在线目标。
- 目标不得位于备份库内（按真实路径判定，含 catalog 与对象文件），不得是
  目录，父目录必须已存在。
- 先验证后替换：对象大小、SHA-256 与 catalog 不符（篡改/截断）或
  integrity_check 失败时，恢复失败且旧目标字节不动；对象缺失同样失败。
  失败后可重试；`check` 会报告 `missing`/`mismatched` 的代。

## 日常运维

- `list` 查看各代（编号、request_id、对象摘要、大小、源路径、UTC 时间）。
- `check` 校验 catalog 与对象一致：`ok=false` 时按 `missing`（对象丢失）/
  `mismatched`（对象被改）处置——从其它副本重新备份或 prune 掉受损代；
  `orphan_objects`/`stray_tmp` 为可待清扫垃圾（`prune` 自动清理）。
- 建议节奏：每次发布新交付库后用新的 `request_id` 备份；按保留策略定期
  `prune --keep N`；`prune` 后 `check` 应为 `ok=true` 且无孤儿。
- 备份库可整体复制归档（catalog + objects 目录）；不支持库内嵌套库。

## 故障注入测试钩子（仅测试用）

默认惰性无效。`CIRCULATION_BACKUP_KILL_AT` 逗号分隔的断点名单，命中即
SIGKILL 自杀：`after_snapshot`、`after_object_publish`、`after_catalog_commit`
（backup），`after_hold`、`after_stage`（restore）。
`CIRCULATION_BACKUP_RV_DIR` 指定会合目录后，backup 在 `after_snapshot`、
restore 在 `after_hold` 触碰 `<点>.ready` 并等待 `<点>.go` 出现，用于确定性
并发编排。`tests/test_backup.py` 用它们实跑：WAL 源跨进程备份、并发同请求
且源已变化、各提交边界强杀后重启、持代期间并发备份与清理、持代进程死亡、
对象篡改/丢失，覆盖普通库与整包库两种真实 CLI 产物。
