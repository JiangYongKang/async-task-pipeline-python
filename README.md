# async-task-pipeline

基于 FastAPI + SQLite 的异步任务提交与执行服务：支持幂等提交、有界并发、
取消与完成竞态裁决、失败归一化以及进程中断后的状态恢复。

## 快速开始

```bash
uv sync
uv run python main.py            # 默认监听 127.0.0.1:8000
uv run pytest tests/ -q          # 运行测试
uv run pytest tests/ -q --log-cli-level=INFO   # 查看请求标识/状态迁移/判定依据日志
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | 提交任务（`idempotency_key` + `payload`，可选 `request_id`） |
| GET | `/tasks/{task_id}` | 查询任务状态 |
| GET | `/tasks` | 列出全部任务 |
| POST | `/tasks/{task_id}/cancel` | 取消任务 |

错误响应统一为 `{"error": {"reason": ..., "message": ...}}`，`reason` 可区分。

## 任务状态机

```
PENDING  -> RUNNING | CANCELLED
RUNNING  -> SUCCEEDED | FAILED | CANCELLED
SUCCEEDED / FAILED / CANCELLED  为终态，不得再被改写
```

- 所有迁移先经状态机校验：终态上的任何迁移以 `TERMINAL_STATE` 拒绝，
  其余非法迁移以 `ILLEGAL_TRANSITION` 拒绝，两者可区分。
- 每次迁移都是乐观锁条件更新（`version` 比对）。取消与完成几乎同时
  发生时，只有一个迁移能赢得版本号，另一方观察到失败并以对方结果为
  最终结论——最终状态唯一且稳定。
- 执行器领取任务时执行 `PENDING -> RUNNING`；排队期间被取消的任务
  领取失败、不会被执行。

## 失败原因分类（FailureReason）

执行失败被归一化为以下可区分的原因，内部异常细节只进服务端日志，
不写入任务记录、不通过 API 透出：

| 原因 | 含义 |
| --- | --- |
| `HANDLER_ERROR` | 业务处理器抛出异常 |
| `HANDLER_TIMEOUT` | 执行超过 `handler_timeout` |
| `INTERRUPTED` | 进程中断时任务处于 RUNNING，重启后判定 |

## 拒绝原因分类（RejectReason）

| 原因 | HTTP | 含义 |
| --- | --- | --- |
| `OVER_CAPACITY` | 503 | PENDING 队列已满，请求被明确拒绝（不无限排队） |
| `ILLEGAL_TRANSITION` | 409 | 非法状态迁移 |
| `TERMINAL_STATE` | 409 | 任务已在终态，不得改写 |
| `TASK_NOT_FOUND` | 404 | 任务不存在 |

## 幂等与容量

- 相同 `idempotency_key` 的重复提交返回首次创建的任务
  （响应中 `duplicated=true`），不产生重复任务，也不占用容量。
- 容量判定与插入在提交锁内原子完成；并发同键提交只会产生一个任务。
- PENDING 任务数达到 `max_pending` 时新请求立即以 `OVER_CAPACITY`
  拒绝；执行并发度由 `max_concurrency` 限定。

## 中断恢复

任务记录持久化于 SQLite（WAL 模式）。进程重启时：

1. 仍处于 `RUNNING` 的任务被判定为 `FAILED / INTERRUPTED`
   （状态确定且可查询，不会被静默丢弃）；
2. 仍处于 `PENDING` 的任务被重新派发执行；
3. 终态任务不受影响（版本号不变）。

## 配置项

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `TASKPIPE_MAX_CONCURRENCY` | 4 | 同时执行的任务数上限 |
| `TASKPIPE_MAX_PENDING` | 16 | PENDING 队列容量上限，超出即拒绝 |
| `TASKPIPE_HANDLER_TIMEOUT` | 30 | 单任务执行超时（秒） |
| `TASKPIPE_DB_PATH` | taskpipe.db | SQLite 存储路径 |
| `TASKPIPE_HOST` / `TASKPIPE_PORT` | 127.0.0.1 / 8000 | 监听地址 |
| `TASKPIPE_LOG_LEVEL` | INFO | 日志级别 |

## 日志与可观测性

日志中每个判定都带请求标识（`request=...`，来自 `X-Request-ID` 头或
请求体 `request_id`，缺省自动生成）、状态迁移
（`transition=PENDING -> RUNNING accepted (version 0 -> 1)`）与判定依据
（`decision=ACCEPT/DEDUP/REJECT/CANCEL_ACCEPTED/COMPLETE_LOST_RACE ...`），
单测日志同样打印上述信息，可用 `--log-cli-level=INFO` 查看。
