# 异步任务流水线（Python / FastAPI）

本地可运行的任务提交与执行服务：支持幂等提交、有界并发、可区分原因的拒绝、
取消与完成竞争的唯一裁决、执行失败归一化，以及进程中断后的状态恢复。

详细设计见 [`docs/design.md`](docs/design.md)。

## 快速开始

```bash
uv sync
uv run python main.py            # 默认 127.0.0.1:8000
# 另一个终端：
curl -s localhost:8000/openapi.json | head    # 也可访问 /docs 交互式调试
```

可调容量/并发/数据库路径：

```bash
TASKPIPELINE_MAX_CONCURRENCY=2 \
TASKPIPELINE_MAX_CAPACITY=8 \
TASKPIPELINE_DB_PATH=data/demo.db \
uv run python main.py
```

## 使用示例

```bash
# 1) 提交（幂等键 idem-1）
curl -s -X POST localhost:8000/tasks \
  -H 'Content-Type: application/json' -H 'X-Request-ID: demo-1' \
  -d '{"handler":"echo","payload":"hello","idempotency_key":"idem-1"}'

# 2) 相同幂等键重复提交 -> 200 + X-Idempotent-Replay: true，返回同一任务
curl -s -i -X POST localhost:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"handler":"echo","payload":"different","idempotency_key":"idem-1"}'

# 3) 查询
curl -s localhost:8000/tasks/<task_id>

# 4) 取消
curl -s -X POST localhost:8000/tasks/<task_id>/cancel

# 5) 容量满时明确拒绝（429 + reason=CAPACITY_FULL，而不是排队等待）
# 6) handler 抛错 -> state=FAILED, failure_reason=HANDLER_ERROR（不透出堆栈）
```

内置 handler：`echo`、`sleep`（payload 为秒数，响应取消）、`fail`（演示失败归一化）。
可通过 `app.handlers.register_handler(name, fn, timeout_seconds=...)` 注册自定义 handler。

## 运行测试

```bash
uv run pytest -s            # -s 可直接看到含请求标识、状态迁移、判定依据的日志
uv run pytest -q            # 仅看结果
```

测试覆盖：

| 文件 | 场景 |
|---|---|
| `tests/test_state_machine.py` | 终态不可改写、非法迁移原因可区分、异常归一化、超时、协作取消 |
| `tests/test_submission.py` | 8 路并发同幂等键只产生 1 个任务、容量满 429、容量释放后恢复 |
| `tests/test_cancel_race.py` | QUEUED 取消不被复活、20 轮取消/完成 CAS 竞争赢家唯一、服务层竞争一致性 |
| `tests/test_recovery.py` | 真实子进程 SIGKILL 后恢复（RUNNING→INTERRUPTED，QUEUED→重放成功）、优雅停机无悬空 |
| `tests/test_api_concurrent.py` | HTTP 层并发幂等、429/422 原因码、终态取消 409 |

## 中断恢复如何验证

`tests/test_recovery.py` 会：启动子进程并让一个任务处于 RUNNING、一个处于 QUEUED，
随后对进程发 `SIGKILL`（模拟真实崩溃），再用同一个 SQLite 库启动服务：

- 原 RUNNING 任务变为 `FAILED / INTERRUPTED`（确定终态）；
- 原 QUEUED 任务被重新入队并执行成功（未被丢弃）；
- 幂等键在重启后依旧有效。
