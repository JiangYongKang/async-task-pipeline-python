# async-task-pipeline

基于 FastAPI + SQLite 的异步任务提交与执行服务：支持幂等提交、有界并发、
取消与完成竞态裁决、失败归一化、**失败自动重试与逐次尝试追溯**，
以及进程中断后的状态恢复。

## 快速开始

```bash
uv sync
uv run python main.py            # 默认监听 127.0.0.1:8000
uv run pytest tests/ -q          # 运行测试
uv run pytest tests/ -q --log-cli-level=INFO   # 查看请求标识/尝试次序/状态迁移/判定依据日志
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | 提交任务（`idempotency_key` + `payload`，可选 `request_id`、`retry` 重试策略） |
| GET | `/tasks/{task_id}` | 查询任务状态（含当前第几次、剩余重试次数、下次重试时间） |
| GET | `/tasks/{task_id}/attempts` | 查询该任务的逐次尝试记录 |
| GET | `/tasks` | 列出全部任务 |
| POST | `/tasks/{task_id}/cancel` | 取消任务（排队中、执行中、等待重试中均可） |

错误响应统一为 `{"error": {"reason": ..., "message": ...}}`，`reason` 可区分。

### 提交时携带重试策略

`POST /tasks` 的请求体可选 `retry` 字段；**不带该字段（或 `max_retries=0`）
的任务行为与原先完全一致：失败即最终失败，不做任何重试**。

```json
{
  "idempotency_key": "order-123",
  "payload": {"url": "https://example.com"},
  "retry": {
    "max_retries": 3,
    "interval": 2.0,
    "backoff": "EXPONENTIAL"
  }
}
```

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `retry.max_retries` | 0 | 首次执行之外最多再试的次数；总尝试次数 = 1 + max_retries |
| `retry.interval` | 0 | 两次尝试之间的基础间隔（秒） |
| `retry.backoff` | `FIXED` | `FIXED`：固定等待 interval；`EXPONENTIAL`：第 n 次失败后等待 `interval * 2**(n-1)` |

任务视图在原有字段上新增：

* `attempt_count`：已开始的尝试次数（首次执行记为 1，进行中也算）；
* `retries_remaining`：剩余重试次数（不带策略时为 `null`）；
* `next_run_at`：`WAITING_RETRY` 状态下下次预计开始的时间戳（epoch 秒）。

`GET /tasks/{task_id}/attempts` 每次尝试单独可查：

```json
{
  "task_id": "...",
  "attempts": [
    {"attempt_no": 1, "kind": "INITIAL", "started_at": ..., "finished_at": ...,
     "result": "FAILURE", "failure_reason": "HANDLER_ERROR",
     "next_run_at": 1791254241.08},
    {"attempt_no": 2, "kind": "RETRY", "started_at": ..., "finished_at": ...,
     "result": "SUCCESS", "failure_reason": null, "next_run_at": null}
  ]
}
```

## 失败自动重试

* 一次尝试失败后，若失败类型可重试且仍有剩余次数，任务先进入
  `WAITING_RETRY`，等待配置的间隔后由调度线程自动再跑一次；
  次数用尽才落为最终 `FAILED`。
* 幂等语义不变：同一 `idempotency_key` 重复提交（哪怕在等待重试期间、
  哪怕请求体里带了不同的 `retry`）永远只对应首次创建的那一个任务，
  策略沿用首次提交；重试是同一任务的后续尝试，不产生新任务。

### 默认重试规则与可重试失败范围

| 失败原因 | 默认是否重试 | 依据 |
| --- | --- | --- |
| `HANDLER_ERROR` | 重试 | 处理器抛普通异常，多为下游偶发故障 |
| `HANDLER_TIMEOUT` | 重试 | 超时常由瞬时拥塞/抖动引起 |
| `INTERRUPTED` | 重试 | 进程中断打断的那一次，重启后按策略继续 |
| `HANDLER_FATAL` | **不重试** | 处理器显式抛出 `taskpipe.retry.NonRetryableError` 标记的确定性失败（如非法入参），重试无意义 |

处理器可显式放弃重试：

```python
from taskpipe.retry import NonRetryableError

def handler(payload):
    if not valid(payload):
        raise NonRetryableError("bad input")  # 归一化为 HANDLER_FATAL，立即终态
```

可重试范围的判定集中在 `taskpipe/retry.py`（`DEFAULT_RETRYABLE_REASONS`
与 `should_retry`），规则单一、可审阅、可按调用方覆盖。

### 等待重试期间的容量、取消与重启

* **容量**：`PENDING` 与 `WAITING_RETRY` 任务共同占用 `max_pending` 个
  队列槽位。等待重试的任务保留自己的位置，不会被新请求挤掉、也不会被
  容量判定丢掉；槽位占满时**新请求**仍立即以 `OVER_CAPACITY` 拒绝，
  绝不无限堆积。重试是既有任务的后续，不重新经过容量判定，因此重试任务
  不会把自己挤掉；任务结束/取消后释放槽位。
* **取消**：等待期间可以取消（`WAITING_RETRY -> CANCELLED`），取消后
  调度器不会再领取。取消正好撞上某一次重试开始时，与执行中的收尾迁移
  竞争同一把乐观锁，最终结论唯一（要么 CANCELLED，要么本次尝试的
  成功/失败），尝试记录与任务状态在同一事务内保持一致。
* **重启恢复**：
  - 停止时处于 `WAITING_RETRY`：重启后按原 `next_run_at` 由调度线程
    接着跑（到点时刻已过则立即执行），同一次尝试不会重复执行或重复计数；
  - 停止时处于 `RUNNING`：被打断的那一次**确定算作一次尝试**，记为
    `FAILURE/INTERRUPTED`，随后按策略决定进入 `WAITING_RETRY` 继续重试，
    还是次数用尽落为 `FAILED/INTERRUPTED`，不会卡在中间状态；
  - `PENDING` 任务重启后重新派发；终态任务不受影响。

## 任务状态机

```
PENDING       -> RUNNING | CANCELLED
RUNNING       -> SUCCEEDED | FAILED | WAITING_RETRY | CANCELLED
WAITING_RETRY -> RUNNING | CANCELLED
SUCCEEDED / FAILED / CANCELLED  为终态，不得再被改写
```

- 所有迁移先经状态机校验：终态上的任何迁移以 `TERMINAL_STATE` 拒绝，
  其余非法迁移以 `ILLEGAL_TRANSITION` 拒绝，两者可区分。
- 每次迁移都是乐观锁条件更新（`version` 比对）。领取（PENDING/
  WAITING_RETRY -> RUNNING 并登记尝试）与收尾（-> SUCCEEDED/FAILED/
  WAITING_RETRY/CANCELLED 并更新尝试记录）各自在单条 SQLite 事务内原子
  完成，因此"尝试计数"与"状态迁移"不可分割，并发裁决只有一个赢家。
- 执行器领取任务时执行原子领取；排队/等待期间被取消的任务领取失败、
  不会被执行，也不会凭空多记一次尝试。

## 失败原因分类（FailureReason）

执行失败被归一化为以下可区分的原因，内部异常细节只进服务端日志，
不写入任务记录、不通过 API 透出：

| 原因 | 含义 | 默认重试 |
| --- | --- | --- |
| `HANDLER_ERROR` | 业务处理器抛出普通异常 | 是 |
| `HANDLER_FATAL` | 处理器抛出 `NonRetryableError`，确定性失败 | 否 |
| `HANDLER_TIMEOUT` | 执行超过 `handler_timeout` | 是 |
| `INTERRUPTED` | 进程中断时任务处于 RUNNING，重启后判定 | 是（受次数限制） |

## 拒绝原因分类（RejectReason）

| 原因 | HTTP | 含义 |
| --- | --- | --- |
| `OVER_CAPACITY` | 503 | PENDING 队列已满，请求被明确拒绝（不无限排队） |
| `ILLEGAL_TRANSITION` | 409 | 非法状态迁移 |
| `TERMINAL_STATE` | 409 | 任务已在终态，不得改写 |
| `TASK_NOT_FOUND` | 404 | 任务不存在 |

## 幂等与容量

- 相同 `idempotency_key` 的重复提交返回首次创建的任务
  （响应中 `duplicated=true`），不产生重复任务，也不占用容量；
  重试期间重复提交同样命中原任务，重试策略不会被覆盖。
- 容量判定与插入在提交锁内原子完成；并发同键提交只会产生一个任务。
- `PENDING` 与 `WAITING_RETRY` 任务数合计达到 `max_pending` 时，
  **新提交请求**立即以 `OVER_CAPACITY` 拒绝；任务进入等待重试是既有
  任务的状态迁移、不重新占新槽，重试不会突破容量上限。
  执行并发度（含各次重试）由 `max_concurrency` 限定。

## 中断恢复

任务记录与尝试记录持久化于 SQLite（WAL 模式）。进程重启时：

1. 仍处于 `RUNNING` 的任务：被打断的那一次记为 `FAILURE/INTERRUPTED`
   并算作一次尝试；若重试策略允许则转为 `WAITING_RETRY` 到期续跑，
   否则落为 `FAILED / INTERRUPTED`（状态确定且可查询，不会被静默丢弃）；
2. 仍处于 `PENDING` 的任务被重新派发执行；
3. 仍处于 `WAITING_RETRY` 的任务保持原 `next_run_at`，由调度线程到期
   自动续跑（到点已过则立即执行）；
4. 终态任务不受影响（版本号不变）。

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
请求体 `request_id`，缺省自动生成）、尝试次序（`attempt=1/2/...`）、
状态迁移（`claim accepted: WAITING_RETRY -> RUNNING attempt_no=2 kind=RETRY`）
与判定依据（`decision=ACCEPT/DEDUP/RETRY_WAIT/FINAL_FAIL/CANCEL_ACCEPTED/
RETRY_WAIT_LOST_RACE ...`），单测日志同样打印上述信息，可用
`--log-cli-level=INFO` 查看。

## 本地验证重试能力

```bash
# 1) 全量单测（含重试次数用尽、等待期取消、取消撞重试、
#    重启后续跑、重试不突破容量，以及原有提交/取消/恢复用例不回归）
uv run pytest tests/ -q --log-cli-level=INFO

# 2) 起服务手工验证
uv run python main.py

# 提交一个偶发失败、最多重试 3 次、指数退避的任务
curl -s -X POST localhost:8000/tasks -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"demo-1","payload":{"n":1},
       "retry":{"max_retries":3,"interval":1,"backoff":"EXPONENTIAL"}}'

# 观察当前第几次、剩余次数、下次重试时间
curl -s localhost:8000/tasks/<task_id>

# 查看每次尝试的时间、结局、失败分类、是否重试、下次开始时间
curl -s localhost:8000/tasks/<task_id>/attempts

# 等待重试期间取消
curl -s -X POST localhost:8000/tasks/<task_id>/cancel
```

> 存储 schema 为新增列与 `attempts` 表做了轻量幂等迁移，旧版本的
> SQLite 数据库可直接在新版本上打开；不带 `retry` 的历史任务字段取
> 默认值（`max_retries=0`），恢复与执行行为与旧版本一致。
