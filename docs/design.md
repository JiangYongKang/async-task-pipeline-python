# 异步任务流水线：设计说明

一套可在本地运行的任务提交与执行流程，保证并发提交、重复提交、取消、
进程中断恢复四类场景下任务状态**可解释、可复现**。

## 1. 任务状态机

```
            提交成功
              │
              ▼
         ┌────────┐  worker 出队且 CAS 成功  ┌─────────┐
         │ QUEUED │ ───────────────────────▶ │ RUNNING │
         └────────┘                          └─────────┘
              │                                │   │   │
   用户取消(CAS)│            成功(CAS)──────────┘   │   └──────────────┐
              ▼                    失败/超时(CAS)─┐ │   用户取消(CAS)   │
        ┌───────────┐                              ▼ ▼                  ▼
        │ CANCELLED │                        ┌────────┐          ┌───────────┐
        └───────────┘                        │ FAILED │          │ CANCELLED │
                                             └────────┘          └───────────┘
                                                  ┌───────────┐
                              成功完成(CAS) ────▶ │ SUCCEEDED │
                                                  └───────────┘
```

合法迁移（唯一允许的边）：

| 当前状态 | 允许迁移到 |
|---|---|
| `QUEUED` | `RUNNING`、`CANCELLED` |
| `RUNNING` | `SUCCEEDED`、`FAILED`、`CANCELLED` |
| `SUCCEEDED` / `FAILED` / `CANCELLED` | 无（**终态，永不改写**） |

终态查询：`TaskState.is_terminal`。

### 取消与完成同时发生：结论为什么唯一

所有状态迁移都走持久层的 **CAS（Compare-And-Set）条件更新**：

```sql
UPDATE tasks SET state = :target, ...
WHERE task_id = :id AND state IN (:expected...);
```

- QUEUED 任务被取消后，worker 出队时读到终态 `CANCELLED`，**直接跳过执行**，
  不会将其“复活”为 RUNNING；
- RUNNING 任务上，“完成（SUCCEEDED/FAILED）”与“取消（CANCELLED）”是两条
  `expected = {RUNNING}` 的 CAS。SQLite 单写者语义下只有一条能提交成功，
  先到者赢，输家得到 `ALREADY_TERMINAL`。重试、并发、重复请求都不会改变结论。

非法迁移的**可区分拒绝原因**（`RejectReason`）：

| 原因码 | 含义 | HTTP |
|---|---|---|
| `ILLEGAL_TRANSITION` | 当前状态不允许目标迁移 | 409 |
| `ALREADY_TERMINAL` | 终态任务被再次改写 | 409 |
| `TASK_NOT_FOUND` | 任务不存在 | 404 |
| `CAPACITY_FULL` | 在制任务数已达容量上限 | 429 |
| `INVALID_HANDLER` | 提交了未注册的处理函数 | 422 |
| `INVALID_PAYLOAD` | 载荷不是合法字符串 | 422 |

`ILLEGAL_TRANSITION` 与 `ALREADY_TERMINAL` 分开，调用方可以区分
“操作早了/晚了”还是“对一个已经结束的任务做了写操作”。

## 2. 执行失败的归一化（FailureReason）

处理函数抛出的任何内部异常**都不会把堆栈/内部原文直接透出**，而是归一化为：

| 失败原因 | 触发条件 | 对外可见内容 |
|---|---|---|
| `HANDLER_ERROR` | 处理函数抛出普通异常 | 异常**类名 + 截断后的消息**（上限 200 字符）；堆栈仅写服务日志 |
| `TIMEOUT` | 执行超过 handler 级或全局超时 | 固定说明 `execution exceeded Ns` |
| `INTERRUPTED` | 进程中断时任务停留在 RUNNING，启动恢复后判定 | 固定说明 `worker was interrupted by process stop` |

超时机制：在守护线程中运行 handler，服务线程用 `Event.wait(timeout)` 判定；
超时不强行 kill 线程（Python 无法安全杀线程），daemon 线程不阻止进程退出。

处理函数通过参数拿到 `cancel_event: threading.Event`，长任务可周期性
`cancel_event.is_set()` / `wait()` 实现协作式取消；即使 handler 不响应取消，
服务层 CAS 仍会保证取消/完成只有一个结论。

## 3. 幂等提交与有界并发

### 幂等

- `tasks.idempotency_key` 上有 **UNIQUE 约束**；
- 提交流程在同一把服务锁内完成：handler 校验 → 幂等键查重 → 容量检查 →
  插入 → 入队。并发的相同 key 请求，第一个成功插入（`201 Created`），
  其余返回同一任务并带 `X-Idempotent-Replay: true`（`200 OK`），
  **绝不产生重复任务**；
- 幂等返回的是首次提交的任务（含首次 payload 与最终状态），任务终态后
  再用相同 key 提交也只重放，不会重跑。

### 有界容量

- **并发度** `max_concurrency`：工作线程数；
- **容量上限** `max_capacity`：状态为 `QUEUED` 或 `RUNNING` 的任务总数上限。
  达到上限时新请求**立即**收到 `429 CAPACITY_FULL`，不会无限排队；
- 执行器内部待执行队列也是有界的（与容量联动），双保险。
- 任务进入终态即释放容量。

## 4. 进程中断恢复

持久化使用 SQLite（WAL + `synchronous=FULL`），单连接 + 进程内锁，
所有状态写入均为自动提交的单语句事务：

- 进程被 `SIGKILL` 时，未提交事务自动回滚；已落盘的 QUEUED/RUNNING
  记录重启后必然可查，**不会静默丢失**。
- 启动时扫描全部非终态任务（`_recover_on_startup`）：
  - 残留 `RUNNING` → `FAILED / INTERRUPTED`（结果在硬中断下不可知，
    给出确定终态，避免任务永远挂起）；
  - 残留 `QUEUED` → 重新入队执行，直到走到终态。
- 优雅停机（`shutdown()`）：先把在制任务 CAS 为 `CANCELLED` 并向运行中
  任务发取消信号，再等待 worker 排空，正常退出不留悬空任务。

## 5. 配置项

可通过环境变量覆盖（`app/settings.py`）：

| 环境变量 | 默认 | 说明 |
|---|---|---|
| `TASKPIPELINE_MAX_CONCURRENCY` | `4` | 最大并发执行线程数 |
| `TASKPIPELINE_MAX_CAPACITY` | `32` | 在制任务（QUEUED+RUNNING）容量上限，须 ≥ 并发度 |
| `TASKPIPELINE_DB_PATH` | `data/tasks.db` | SQLite 数据库路径（自动建目录） |
| `TASKPIPELINE_DEFAULT_TIMEOUT` | 不限（`0`） | 单任务默认超时秒数；0 表示不限时 |
| `TASKPIPELINE_POLL_INTERVAL` | `0.05` | 协作轮询间隔秒 |

handler 还可在注册时单独指定超时：
`register_handler("slow", fn, timeout_seconds=5)`，优先于全局默认值。

## 6. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/tasks` | 提交：`{handler, payload, idempotency_key?}`；新建 201，幂等命中 200 + `X-Idempotent-Replay: true` |
| GET | `/tasks/{task_id}` | 查询任务视图（state / result / failure_reason / error_message） |
| POST | `/tasks/{task_id}/cancel` | 取消；终态冲突 409 `ALREADY_TERMINAL` |
| GET | `/healthz` | 存活探针 |

所有请求可用 `X-Request-ID` 头关联日志（未提供则自动生成，并随响应头返回）。
错误体统一为：

```json
{"error": {"reason": "CAPACITY_FULL", "detail": "active=4 capacity=4"}}
```

内置 handler：`echo`（原样返回）、`sleep`（睡眠秒数，响应取消）、
`fail`（始终抛错，演示失败归一化）。

## 7. 日志可观测性

每行日志包含 `[req=<request_id>]`，关键事件打印**状态迁移与判定依据**，例如：

```
... [req=cap-3] submit accepted ... transition=->QUEUED reason=capacity-ok active=4
... [req=cap-ov] submit rejected ... reason=CAPACITY_FULL active=4 cap=4
... [req=cancel] cancel accepted ... transition=QUEUED->CANCELLED reason=cas-won
... recovery task=... transition=RUNNING->FAILED reason=INTERRUPTED detail=process-stop
```

## 8. 模块结构

```
app/enums.py        状态/失败原因/拒绝原因枚举 + 合法迁移表
app/errors.py       携带原因码的业务异常
app/models.py       Task 领域模型与对外视图
app/settings.py     配置项（环境变量）
app/logging_setup.py 带 request_id 的统一日志
app/repository.py   SQLite 持久化 + CAS 条件更新
app/executor.py     有界工作线程池（满则立即拒绝）
app/handlers.py     handler 注册表 + 内置示例
app/service.py      编排：提交/取消/执行/失败归一化/启动恢复/优雅停机
app/api.py          FastAPI HTTP 层
main.py             本地启动入口
tests/              状态机、提交并发、取消竞争、中断恢复、HTTP 并发
```
