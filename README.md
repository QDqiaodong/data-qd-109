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

## 目录

- `src/circulation_flow/`：解析、业务转换及命令行入口。
- `fixtures/`：MIT 许可的合成输入；来源声明见 DATA-SOURCES.md。
- `docs/contract.md`：字段和口径、边界及失败约定。
- `tests/`：业务结果、边界与失败测试。
- `requirements*.lock`、`pyproject.toml`：环境和构建声明。

本基线不承诺实时同步或生产调度。仅验证本机环境的事实见交付记录；本仓库不
 声称已经验证任何特定 Linux 容器。所有路径均由调用参数提供，无宿主机绝对路径。
