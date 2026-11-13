# 事件与状态口径

输入 JSONL 严格字段 event_id/at/kind/copy_id/loan_id/due。标识为非空文本；
kind=borrow|renew|return；at 是 YYYY-MM-DDTHH:MM:SS 格式，后接 Z 或 ±HH:MM 的 ISO 时间。
不接受小数秒或带秒的时区偏移，避免解析器静默损失精度。
归一化到 UTC 后排序，相同副本同一秒多事件拒绝，以免臆测先后顺序。
due 为 YYYY-MM-DD 的 UTC 到期日，借出/续借必须非空且不早于事件日；归还为 null。
忽略空行。同 event_id 规范化后内容一致可去重，不一致拒绝。

完整初始事件流从空状态重建：同一副本不能同时借出；续借/归还必须匹配活动
loan_id；每个 loan_id 全局只用一次。续借必须严格延后到期日。
事件时间<=as_of 纳入；未来事件保存在 events 且 applied=0，不执行状态迁移。
未来事件仍验证字段，但不提前验证状态机。as_of 之前没有事件时允许空结果。
未借副本的归还、二次归还、混用借阅号均拒绝。当前不处理事件撤回/丢失或顺序补录。

逾期天数=max(0,结束UTC日期-最终到期日)。活动借阅结束日期取as_of，已归还取归还日。
到期当天不逾期；日期粒度而非24小时滚动计时。closed/open 分别汇总，不能把
已结清历史与当前欠还混为一谈。无读者身份数据，不计算罚款。

SQLite events 保留规范化源事件；loans 保留重建结果；run_metadata 保存截止时点及
去重统计；status_summary SQL 视图可核验逾期口径。全量临时建库后替换目标，
失败保留旧数据库；不对活跃连接的目标运行，不承诺并发一致性或增量水位。

## 有界内存处理与发布

大输入路径只顺序读取 JSONL 一次：逐行按上述字段规则严格解码，空行忽略、
重复 event_id 按规范载荷判重、冲突立即失败，事件不进入随输入线性增长的
Python 容器。唯一事件及其规范载荷写入临时 SQLite 暂存表（event_id 主键索引），
覆盖索引提供 (event_at,event_id) 全局顺序；状态机由服务端游标逐行喂入，
同副本同秒守卫、活动副本、全局 loan_id 与贷款生命周期全部落在磁盘状态表。
逾期天数与 open/closed 在单条 SQL 内按同一自然日口径物化，不把贷款集合
重新读入 Python。

work_memory_mb（整数 4–256，默认 16）约束 SQLite 页缓存（cache_size）、
强制排序/索引溢出走文件（temp_store=FILE）并关闭 mmap（mmap_size=0）；
它不精确限制磁盘暂存库、回滚日志与溢出文件的体积（受事件数与磁盘容量
约束），也不覆盖解释器固定开销、逐行短生命周期对象或操作系统页缓存。
temp_dir 默认等于目标目录；显式值必须可写且与目标同文件系统，否则拒绝，
以保证发布是同卷原子重命名。临时目录、磁盘耗尽或任何 SQLite 错误都明确
失败，不退回无界内存实现；单行输入不设最大行长度。

所有校验、重建与写入在临时数据库内完成并正常关闭后才替换目标；输入、
暂存、SQL、状态迁移、截止统计或发布前任一失败都非零退出，旧目标字节不变，
本次临时文件清理。相同输入与参数重复执行，事件、贷款与摘要结果稳定。

## 供应商整包采集（package_intake）

供应商交付的是一次完整导出的 ZIP，而不是规范 JSONL。入口为
`collect(package, as_of, target, max_bytes=67108864)` 与独立 CLI
`python -m circulation_flow.intake --package ZIP --as-of T --database FILE
[--max-bytes N]`。只采集一个离线完整包：无 HTTP、无跨进程断点、无跨运行
账本、无修订版本、无部分接收。

### 捕获与字节预算

源 ZIP 只读取一次，捕获为内存中的同一份字节；之后 ZIP 目录解析、成员解压、
校验、摘要计算与原始资料归档全部使用该捕获，不再按路径重新打开。替换源
路径不影响已捕获内容。`max_bytes` 必须是正整数且不超过 1073741824
（布尔值与非整数无效），默认 67108864（64MiB）。该上限分别约束：捕获的 ZIP
原始字节数，以及全部成员（含 manifest）实际解压字节总数；两者都按真实读取
累加，不信任 ZIP 目录声明的大小，超限明确失败。允许在该字节预算内缓存原始
资料，不为新格式重复实现大文件流式内存优化。

### ZIP 成员规则

包内恰好有根目录 `manifest.json` 和它声明的 CSV 成员：不允许多余、缺失或
同名重复成员，不接受显式目录条目。允许子目录文件名，但每个成员名必须是
规范相对 POSIX 路径：禁止绝对路径、反斜杠、盘符、NUL、空段以及 `.`/`..`
段；不会用路径规范化把非法名字变成合法名字。拒绝符号链接成员、加密成员和
STORE/DEFLATE 之外的压缩方式；任何成员都不会被解压到包外目录。ZIP 截断、
CRC 错误或解压错误整包拒绝。

### manifest 规则

manifest 是无 BOM 的严格 UTF-8 JSON；任何层级的重复对象键都拒绝。顶层
精确为 `schema_version`、`source_system`、`export_id`、`export_kind`、
`parts`。`schema_version` 只接受整数 1（布尔值不算整数）；`export_kind`
必须为 `full`；`source_system`/`export_id` 为非空且无首尾空白的字符串，按
原文区分大小写。`parts` 是非空数组，每项精确含 `path`、`sha256`、`rows`：
`path` 是唯一 CSV 成员名且不得等于 `manifest.json`；`sha256` 为 64 位小写
十六进制；`rows` 为非负整数（布尔值不算整数）。parts 的数组顺序不表示事件
先后或分片优先级。

另计算逻辑清单指纹：将合法 manifest 的 parts 按 path 排序、JSON 对象键
排序，用 UTF-8、`ensure_ascii=False`、紧凑分隔符且无末尾换行序列化后取
SHA256。它只描述逻辑清单，不替代原始 ZIP/manifest 字节摘要；改变 ZIP 条目
顺序、压缩元数据或 parts 顺序会改变原始字节摘要，但不改变业务语义与指纹。

### CSV 与记录映射

每个 CSV 为无 BOM 严格 UTF-8，表头精确依次为
`source_event_id,occurred_at,action,barcode,loan_reference,due_on`，列不得
多缺或重复。按真正的 CSV 逻辑记录解析，支持引号、转义引号与引号内换行；
错误引号拒绝，不按行 split。空逻辑记录与所有单元格均为空字符串的记录
忽略；其他数据记录必须恰好六列。每个分片 `record_index` 从 1 开始，只计
非空逻辑数据记录（不计表头，引号内换行不增加序号）；manifest.rows 用同一
口径计数，跨分片的相同记录各占一次。

字段映射与 JSONL 契约完全一致：`source_event_id`/`barcode`/`loan_reference`
→ `event_id`/`copy_id`/`loan_id`（非空文本，不 trim、casefold 或补零）；
`action` CHECKOUT/EXTEND/CHECKIN → borrow/renew/return；`occurred_at` 复用
严格含时区整秒格式并归一 UTC；`due_on` 在借出/续借时必须是合法到期日，
归还时必须为空字符串并映射 null。转换后继续执行原字段、日期与生命周期
约束，不降低 JSONL 路径的严格程度；未来事件仍校验字段、保存 applied=0 与
来源，但不执行截止时点前的状态迁移。

`event_id` 在全部分片中共同判重：规范事件载荷相同才合并为一条业务事件，
全部出现均保留来源；同 ID 不同载荷整包失败。`source_rows` 为全部非空逻辑
数据记录数，`duplicate_rows = source_rows - 唯一事件数`；空总事件流拒绝。
唯一事件继续按 `(event_at,event_id)` 重建，同副本同秒歧义、活动借阅匹配、
loan_id 全局唯一与续借延后规则与 JSONL 入口一致。

### 来源表与发布

输出是一个自包含 SQLite。`events`、`loans`、`run_metadata`、`status_summary`
保持原字段含义；整包模式额外写入（均在同一事务内）：

- `intake_runs`（单行，run_id 固定为 1）：包身份（source_system、export_id、
  schema_version、export_kind）、as_of、捕获 ZIP 的完整原始字节与
  package_sha256/package_size、manifest 原始字节与 manifest_sha256、
  manifest_fingerprint、source/duplicate/unique 计数与分片数。
- `intake_parts`（主键 part_id，外键 run_id）：成员名、声明/实际 sha256
  （CHECK 约束必须相等）、声明/实际行数（CHECK 必须相等）、分片完整原始
  字节与大小；成员名唯一。
- `intake_occurrences`（主键 occurrence_id，外键 part_id 与 event_id）：
  每次出现的成员名、record_index、event_id 与规范载荷；`(part_id,
  record_index)` 唯一，event_id 建索引。重复与未来记录均逐次保留，可经
  event_id 连接回唯一业务事件。

只有一个发布边界：业务数据与完整来源在私有暂存数据库中全部校验、写完、
检查并关闭后，才以同卷原子重命名替换 target；不会先发布业务库再追加来源。
target 不得与输入 ZIP 为同一文件，包含可识别的符号链接或硬链接别名。
参数、读取、格式校验、状态迁移、SQL、来源写入或替换失败都明确报错（信息可
定位到成员、逻辑记录或事件身份），CLI 非零退出，输入与旧目标字节不变，
临时文件清理，修正后可直接重跑；替换成功之后的 stdout 失败不属于数据库
回滚。相同包与参数重复运行，按稳定业务键查询的逻辑内容与 stdout JSON 字节
一致；不写当前时间、耗时或机器绝对路径，不要求 SQLite 二进制一致。
JSONL 模式不创建 intake_* 表，也不伪造它未捕获的 ZIP/CSV 来源。
