# data-qd-109 · 图书流通事件管道

从乱序、可能重复的图书借还 JSONL 事件重建借阅状态，保留归一化源事件，
落 SQLite 并计算截止时点的活动逾期与历史逾期。核心是事件清洗、状态机和时点统计，
不提供图书或读者 CRUD 后台。

另提供供应商整包采集入口：一次完整导出的 ZIP（根目录 `manifest.json` 加若干
借还 CSV 分片）经全量校验后映射为同一套事件，业务快照与可逐条追溯的原始字节
一起落库。两条入口共用事件契约、状态机与发布边界；任一分片缺失、损坏或冲突
都整包拒绝，不产生部分导入。

演示6行源事件归一化为5条，1行等价重复、1条未来事件不应用。
L1先借出再续借到1月10日，1月12日归还，closed逾期2天；L2仍在借，open逾期4天。
SQLite 的 events、loans、run_metadata 和 status_summary 可逐层核查；
整包模式另有 intake_runs/intake_parts/intake_occurrences 三张来源表。

## 安装与运行

要求 Python 3.11 或更高；先确认 `python3 --version`。以下命令在仓库根目录执行：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
.venv/bin/python -m circulation_flow.cli --source fixtures/events.jsonl --as-of 2026-01-12T00:00:00Z --database run/circulation.sqlite
.venv/bin/python -m unittest discover -s tests -v
```

### 供应商 ZIP 整包采集

先把 JSONL 演示数据转成原创合成的多分片乱序 ZIP（仅标准库，确定性、可重复），
再用独立的 intake CLI 采集：

```sh
# 生成：manifest.json + 3 个乱序 CSV 分片（--tricky 额外加入含逗号/引号/换行的身份）
python3 tools/generate_package.py run/export.zip \
    --source fixtures/events.jsonl --parts 3 --seed 20261113
# 采集：一个离线完整包 -> 一个自包含 SQLite
python3 -m circulation_flow.intake \
    --package run/export.zip --as-of 2026-01-12T00:00:00Z \
    --database run/intake.sqlite [--max-bytes 67108864]
```

输出 stdout 是一行稳定 JSON（键排序、无时间/机器路径），字段：

- 入口与包身份：`mode=package_intake`、`source_system`、`export_id`、
  `export_kind`、`schema_version`、`part_count`。
- 摘要：`package_sha256`/`package_size`（捕获 ZIP 原始字节）、
  `manifest_sha256`（manifest 原始字节）、`manifest_fingerprint`（逻辑清单指纹，
  与 parts 顺序无关）。
- 原 run 的全部业务统计：`as_of`、`source_rows`、`duplicate_rows`、
  `unique_events`、`future_events`、`summary`（status_summary 视图同口径）。

`collect(package, as_of, target, max_bytes=67108864)` 是对应的 Python 入口；
`max_bytes` 为正整数且 ≤ 1073741824（布尔/浮点/非整数无效），分别约束捕获的
ZIP 字节数与全部成员解压字节总数，任一超限整包失败。

### 来源查询与重建

输出库自包含，移走源 ZIP 后仍可还原全部原始资料：

```sql
-- 包身份、捕获 ZIP 与 manifest 的完整字节和摘要
SELECT source_system,export_id,package_sha256,package_size,
       manifest_sha256,manifest_fingerprint FROM intake_runs;
-- 各分片成员名、声明/实际 sha256 与行数、完整原始字节
SELECT member_name,declared_sha256,actual_sha256,declared_rows,actual_rows
FROM intake_parts ORDER BY part_id;
-- 每次出现（含重复与未来记录）连接回唯一业务事件
SELECT o.member_name,o.record_index,o.event_id,e.event_at,e.applied
FROM intake_occurrences o JOIN events e ON e.event_id=o.event_id
ORDER BY o.occurrence_id;
```

`intake_runs.package_bytes` 即捕获 ZIP 原文，`intake_parts.raw_bytes` 与
`intake_runs.manifest_bytes` 为各成员原文；按成员名写回即可重建原始 ZIP 并
独立复算摘要。JSONL 入口不创建 intake_* 表，也不伪造任何 ZIP/CSV 来源。

## 整包校验口径（摘要）

- ZIP 恰好包含根 `manifest.json` 与其声明的 CSV 成员：无多余、缺失、重名或
  显式目录条目；成员名必须是规范相对 POSIX 路径（无绝对路径、盘符、反斜杠、
  NUL、空段、`.`/`..` 段），拒绝符号链接、加密成员和 STORE/DEFLATE 之外的压缩。
- manifest 为无 BOM 严格 UTF-8 JSON，重复对象键拒绝；顶层字段与类型固定
  （schema_version 仅整数 1、export_kind=full、parts 非空且每项含 path/sha256/rows）。
- CSV 无 BOM、固定六列表头，按真正的 CSV 逻辑记录解析（支持引号、转义引号、
  引号内换行；错误引号拒绝）；空逻辑记录忽略，record_index 从 1 只计数据记录。
- 身份字段（source_event_id/barcode/loan_reference → event_id/copy_id/loan_id）
  不 trim、不 casefold；action CHECKOUT/EXTEND/CHECKIN → borrow/renew/return；
  时间戳与 due 规则完全复用 JSONL 契约。event_id 跨分片判重：同载荷合并且
  全部出现保留，异载荷整包失败；空事件流拒绝。
- 单一发布边界：业务与来源在私有暂存库全部写完、检查并关闭后才原子替换
  target；target 不得是输入 ZIP 的路径/符号链接/硬链接别名。任何失败都保留
  输入与旧目标字节、清理临时文件，修正后可直接重跑。完整口径见 docs/contract.md。

运行依赖与测试仅使用标准库；首次安装构建工具通常需要联网。每个仓库有独立
 `.venv`。安装的是源码的构建副本；编辑源码后须再次执行安装命令。
所有演示输出在忽略的 `run/` 中；演示可重复执行。临时目录中的测试不改变 fixture。
CLI 参数错误或无效数据返回非零；失败原因打印到 stderr。

## 大输入与有界内存

`run()` 对 JSONL 只做一次顺序读取：逐行严格校验后写入同目录的临时 SQLite
暂存库，以 `event_id` 主键做全局重复检测，再由覆盖索引按 `(event_at,event_id)`
排序、用服务端游标逐行驱动状态机。活动副本、全局 loan_id、同副本同秒守卫和
全部贷款状态都在磁盘表中；逾期与 open/closed 在一条 SQL 内物化，不把事件或
贷款集合读回 Python 容器。成功关闭后临时库才原子替换目标，任何失败都保留旧
目标并清理本次临时文件。`read_events()`/`rebuild()` 的内存参照实现保持不变，
仍可供小规模核对与测试对照。

新增两个受控参数（CLI 与 `run(..., work_memory_mb=, temp_dir=)` 同名可用）：

- `--work-memory-mb`：整数 4–256，默认 16。它设置 SQLite `cache_size` 页缓存
  上限、`temp_store=FILE`（排序/索引溢出写盘而非内存）和 `mmap_size=0`。受其
  约束的是 SQLite 页缓存与排序暂存内存；不保证精确限额的是磁盘上的暂存库、
  回滚日志和溢出文件大小（随事件数增长，受磁盘容量约束）、解释器固定开销与
  逐行短生命周期对象、以及进程外的操作系统页缓存。
- `--temp-dir`：暂存库与 SQLite 溢出文件目录，默认取目标数据库所在目录，保证
  发布重命名在同一文件系统上是原子操作。显式指定时会先校验可写并校验与目标
  处于同一文件系统（`st_dev` 一致），不兼容立即失败。

磁盘暂存与排序溢出目录可通过该参数或 `SQLITE_TMPDIR` 定位；磁盘耗尽或 SQLite
报错会明确失败，不会退回无界内存实现。单行输入没有最大行长度限制。

合成大文件与内存对照工具（仅标准库，不属于已安装包）：

```sh
python3 tools/generate_events.py run/big.jsonl --copies 40000 --seed 20261113
python3 tools/memory_benchmark.py pipeline  run/big.jsonl 2027-01-01T00:00:00Z run/big.sqlite 16
python3 tools/memory_benchmark.py reference run/big.jsonl 2027-01-01T00:00:00Z run/ref.sqlite
```

## 目录

- `src/circulation_flow/`：解析、业务转换及命令行入口（`cli` 为 JSONL，
  `intake` 为供应商 ZIP 整包）。
- `fixtures/`：MIT 许可的合成输入；来源声明见 DATA-SOURCES.md。
- `tools/generate_events.py`：合成 JSONL；`tools/generate_package.py`：
  合成供应商导出 ZIP（含清单与乱序分片）。
- `docs/contract.md`：字段和口径、边界及失败约定。
- `tests/`：业务结果、边界与失败测试（`test_intake.py` 覆盖整包采集）。
- `requirements*.lock`、`pyproject.toml`：环境和构建声明。

本基线不承诺实时同步或生产调度。仅验证本机环境的事实见交付记录；本仓库不
 声称已经验证任何特定 Linux 容器。所有路径均由调用参数提供，无宿主机绝对路径。
