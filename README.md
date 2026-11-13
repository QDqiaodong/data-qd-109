# data-qd-109 · 图书流通事件管道

从乱序、可能重复的图书借还 JSONL 事件重建借阅状态，保留归一化源事件，
落 SQLite 并计算截止时点的活动逾期与历史逾期。核心是事件清洗、状态机和时点统计，
不提供图书或读者 CRUD 后台。

演示6行源事件归一化为5条，1行等价重复、1条未来事件不应用。
L1先借出再续借到1月10日，1月12日归还，closed逾期2天；L2仍在借，open逾期4天。
SQLite 的 events、loans、run_metadata 和 status_summary 可逐层核查。

## 安装与运行

要求 Python 3.11 或更高；先确认 `python3 --version`。以下命令在仓库根目录执行：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
.venv/bin/python -m circulation_flow.cli --source fixtures/events.jsonl --as-of 2026-01-12T00:00:00Z --database run/circulation.sqlite
.venv/bin/python -m unittest discover -s tests -v
```

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

- `src/circulation_flow/`：解析、业务转换及命令行入口。
- `fixtures/`：MIT 许可的合成输入；来源声明见 DATA-SOURCES.md。
- `docs/contract.md`：字段和口径、边界及失败约定。
- `tests/`：业务结果、边界与失败测试。
- `requirements*.lock`、`pyproject.toml`：环境和构建声明。

本基线不承诺实时同步或生产调度。仅验证本机环境的事实见交付记录；本仓库不
 声称已经验证任何特定 Linux 容器。所有路径均由调用参数提供，无宿主机绝对路径。
