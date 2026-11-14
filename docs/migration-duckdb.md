# SQLite 快照 → DuckDB 分析库离线迁移口径

本文档定义 `circulation_flow.migrate_duckdb` 的迁移契约。两个生产入口
（`circulation_flow.cli` 的 JSONL 与 `circulation_flow.intake` 的供应商 ZIP）
保持不变，仍只产出 SQLite；迁移是**只读、离线、一次性**的快照复制，不重新
解释事件、不重算借阅、不回写源库。

## 入口与依赖

- Python 入口：`migrate(source, target, *, batch_size=1000, temp_dir=None,
  after_snapshot=None, before_readback=None)`。
- CLI：`python -m circulation_flow.migrate_duckdb --source 快照.sqlite
  --database 分析.duckdb [--batch-size N] [--temp-dir DIR]`，另有控制台脚本
  `circulation-migrate-duckdb`。stdout 是一行键排序的稳定 JSON。
- 仅迁移功能需要第三方包 `duckdb==1.5.3`（见 `requirements-duckdb.lock`），
  在模块内**懒加载**；不安装 duckdb 不影响两个生产入口。迁移连接显式
  `SET autoinstall_known_extensions=false` 与
  `SET autoload_known_extensions=false`，不安装/加载任何扩展，不访问网络或
  服务，临时排序溢出写入本地 `temp_dir`。

## 可接受的源结构

源必须是已交付的、结构固定的 SQLite 快照，按 `sqlite_master` 精确判定：

- 普通模式（JSONL 入口产物）：基表恰好 `events`、`loans`、`run_metadata`，
  视图恰好 `status_summary`。
- 整包模式：在普通模式基础上**同时**具备 `intake_runs`、`intake_parts`、
  `intake_occurrences` 三张来源表；只出现其中一部分即拒绝。
- 任何多余基表、其他类型对象（触发器等）、缺表、列名/声明类型/列序变化、
  缺失或变化的主键、唯一、外键声明，都整次拒绝。
- 视图必须暴露 `(status, loans, overdue_loans, overdue_days, renewals)` 五列，
  且其聚合结果与直接对 `loans` 的同口径聚合一致，否则按计数矛盾拒绝。

注意 SQLite 的目录怪癖：`PRAGMA table_info` 对主键列（TEXT/BLOB 主键以及
INTEGER 主键）报告 `notnull=0`，主键本身的 NOT NULL 语义由独立的主键声明
承担；迁移按 SQLite 实际目录输出核对，再单独校验主键/唯一/外键声明。

## 类型映射与动态类型拒绝

| SQLite 声明类型 | 可接受运行时存储类 | DuckDB 目标类型 |
|---|---|---|
| `TEXT` | 仅 `TEXT`（Python `str`） | `VARCHAR` |
| `INTEGER` | 仅 `INTEGER`（Python `int`，**拒绝 `bool`**），且落在有符号 64 位区间 | `BIGINT` |
| `BLOB` | 仅 `BLOB`（`bytes`/`bytearray`/`memoryview`，统一为 `bytes`） | `BLOB` |
| 任意可空列的 `NULL` | `NULL` | `NULL`（保留） |

SQLite 是动态类型库，普通绑定会施加列亲和性而静默转换；迁移不信任亲和性，
在任何跨表逻辑前对每个存储值做运行时类型扫描。亲和性无法掩盖的坏值
（`CAST(... AS BLOB)` 写入、历史文件遗留）一律拒绝，错误信息定位到表与列。
身份文本（含 NUL、中文、emoji、引号换行）按 UTF-8 原文保留；事件 `payload`
与来源 `payload` 的 JSON 文本原样保留；`package_bytes`、`manifest_bytes`、
`raw_bytes` 的 BLOB 字节逐字节保留。整数保持 64 位语义，超出
`[-2^63, 2^63-1]` 拒绝。

## 约束在目标实际生效

目标 DDL 显式声明，约束存在性通过 `duckdb_constraints()` 目录核对，并用
autocommit 下的违约探针实证（违约语句自行回滚，探针行不落库）：

- 主键：`events.event_id`、`loans.loan_id`、`intake_runs.run_id`、
  `intake_parts.part_id`、`intake_occurrences.occurrence_id`。
- 唯一：`intake_parts.member_name`、`intake_occurrences(part_id,record_index)`。
- 外键：`intake_parts.run_id→intake_runs.run_id`；
  `intake_occurrences.part_id→intake_parts.part_id`、
  `event_id→events.event_id`。源侧另用 `PRAGMA foreign_key_check` 拒绝孤儿。
- 业务 CHECK：`intake_runs(run_id=1)`、`intake_runs(mode='package_intake')`、
  `intake_parts(declared_sha256=actual_sha256)`、
  `intake_parts(declared_rows=actual_rows)`，以及全部 NOT NULL。
- `status_summary` 以视图重建（DuckDB 方言把布尔比较显式 `CAST(... AS BIGINT)`
  以匹配源视图的整数聚合）。

## 单一源快照

源以 `file:` URI 加 `mode=ro` 只读打开，并 `PRAGMA query_only=ON`，随后显式
`BEGIN` 持有一个读事务直到迁移结束。首次读取即固定读快照：读开始之后其他
连接的提交、或源路径被 `os.replace` 换成另一个文件，都不会混入结果（整包
采集的 ZIP 捕获同理，但那是生产入口的既有保证）。DELETE 日志模式下读写
互斥、WAL 模式下读事务与并发写各自快照一致，两种行为都由测试覆盖。

## 批处理、缓冲与引擎资源边界

- 每张表按显式稳定键排序（事件按 `event_id`、贷款按 `loan_id`、来源按各自
  整数主键等），服务端游标 + `fetchmany(batch_size)` 取批，
  `executemany` 写批；Python 侧只存活当前一批的行与摘要片段，缓冲为
  `O(batch_size × 行宽)`，不随总行数增长。`batch_size` 为 1..1_000_000 的
  整数（布尔/浮点/越界拒绝），默认 1000。
- 单个 BLOB 允许占满其所在行的一个槽位（已用 1 MiB BLOB、`batch_size=1`
  验证字节一致）；不跨批累积 BLOB。
- 回读核验在关闭暂存库后**重开一个只读连接**，同样按批 `fetchmany` 流式
  重算摘要，不把整表读入 Python。
- 不保证由 `batch_size` 精确限定的部分：DuckDB 引擎自身的客户端/执行器
  缓冲、磁盘上的暂存库与临时溢出文件（受磁盘容量约束）、解释器固定开销与
  逐批短生命周期对象、操作系统页缓存。这些是引擎资源边界，不是迁移代码的
  表级容器；迁移代码本身不维护任何随总行数增长的 Python 集合。

## 带类型无歧义编码与逐表摘要

每个值按类型加标签编码：NULL=`N`；整数=`I<长度>:<十进制ASCII>`；
文本=`T<UTF-8字节长度>:<UTF-8字节>`；BLOB=`B<字节长度>:<原始字节>`。每行以
列数帧开头。长度前缀使 NUL、冒号、空串、空 BLOB 与任意二进制都自定界、无
歧义（已用对抗值验证无碰撞）。按稳定键排序后逐行喂入 SHA-256，得到每表
摘要；视图用其五列的声明类型同样编码摘要。源摘要与重开目标后的回读摘要
逐项（行数 + 摘要）比对，任一不符即拒绝发布并报告差异对象。

## 计数矛盾与孤儿来源

在固定快照内额外校验：`run_metadata` 恰一行且 `as_of` 合法；
`source_rows-duplicate_rows = events 行数`；`applied=0` 行数等于
`future_events`；`applied` 只取 0/1；整包模式下 `intake_runs` 恰一行
`run_id=1`，其 source/duplicate/unique/part_count 与 occurrences/parts 的
实际行数、distinct event_id、各分片行数全部吻合；occurrence 的 part_id 与
event_id 都能连接到现存父行。任何矛盾或孤儿整次拒绝。

## 原子发布与失败约定

目标不得是源的路径/符号链接/硬链接别名（`realpath` 与 `st_dev/st_ino`
双重判定）。全部写入、约束探针、关闭、重开回读、摘要核验都在目标同卷的
私有暂存文件内完成；仅当全部通过且连接干净关闭后，才 `os.replace` 原子
替换目标。任何失败都保留旧目标字节不变、清理暂存文件与临时目录，修正后
可直接重跑。复制期失败、回读期失败、发布重命名失败均由注入测试覆盖。
