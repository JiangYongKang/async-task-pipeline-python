# async-task-pipeline

基于 FastAPI + SQLite 的异步任务提交与执行服务：支持幂等提交、有界并发、
取消与完成竞态裁决、失败归一化、**失败自动重试与逐次尝试追溯**，
以及进程中断后的状态恢复。

## 快速开始

```bash
uv sync
uv run python main.py            # 默认监听 127.0.0.1:8000
uv run pytest tests/ -q          # 运行测试
uv run pytest tests/ -q --log-cli-level=INFO   # 查看请求标识/第几次尝试/状态迁移/判定依据日志
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | 提交任务（`idempotency_key` + `payload`，可选 `request_id`、`retry` 重试策略） |
| GET | `/tasks/{task_id}` | 查询任务状态（含当前第几次、剩余重试次数、下次重试时间） |
| GET | `/tasks/{task_id}/attempts` | 查询逐次尝试记录 |
| GET | `/tasks` | 列出全部任务 |
| POST | `/tasks/{task_id}/cancel` | 取消任务（排队中、执行中、等待重试中均可取消） |

错误响应统一为 `{"error": {"reason": ..., "message": ...}}`，`reason` 可区分。

### 提交重试策略

`POST /tasks` 请求体中的 `retry` 对象（省略或 `max_retries=0` 表示不重试，
行为与旧版本完全一致）：

```json
{
  "idempotency_key": "order-1",
  "payload": {"url": "https://example.com"},
  "retry": {
    "max_retries": 3,
    "base_delay": 0.5,
    "strategy": "EXPONENTIAL",
    "factor": 2.0,
    "max_delay": 30.0,
    "retry_on_interruption": true,
    "count_interruption_as_attempt": true
  }
}
```

| 字段 | 默认 | 含义 |
| --- | --- | --- |
| `max_retries` | 0 | 首次执行之外最多再试几次；总尝试次数 = 1 + max_retries |
| `base_delay` | 1.0 | 两次尝试之间的基础间隔（秒） |
| `strategy` | `FIXED` | `FIXED` 固定间隔；`EXPONENTIAL` 指数退避 |
| `factor` | 2.0 | 指数退避乘数：第 n 次尝试前等待 `base_delay * factor**(n-2)`（n≥2） |
| `max_delay` | 60.0 | 单次间隔上限（指数退避截断，秒） |
| `retry_on_interruption` | true | 某次尝试执行期间进程被停掉，重启后是否继续重试 |
| `count_interruption_as_attempt` | true | 被中断的那次执行是否消耗一次重试预算（审计记录始终保留） |

任务视图（`GET /tasks/{id}`）新增：`attempt_no`（已开始的尝试序号）、
`remaining_retries`（剩余重试次数）、`next_run_at`（RETRY_WAITING 时
下一次尝试预计开始的 Unix 时间戳）。

逐次尝试 `GET /tasks/{id}/attempts`，每条记录包含：`attempt_no`（第几次，
从 1 开始）、`is_retry`（false=首次执行 / true=重试）、`started_at`、
`finished_at`、`status`（`IN_PROGRESS`/`SUCCEEDED`/`FAILED`/`CANCELLED`）、
`failure_reason`、`counted`（是否消耗预算）、`next_run_at`
（本次失败后下一次预计开始时间，最后一次尝试为 null）。

## 默认重试规则（哪些失败会自动重试）

* `HANDLER_ERROR`（业务处理器抛异常）：**默认可重试**，通常具有偶发性；
* `HANDLER_TIMEOUT`（执行超时）：**默认可重试**；
* `INTERRUPTED`（进程中断导致执行被打断）：默认**不按运行期失败规则**处理，
  是否重试由策略字段 `retry_on_interruption` 单独决定（默认 true）；
  被中断的那次算不算一次尝试由 `count_interruption_as_attempt` 决定
  （默认计数）。
* 只有携带 `retry`（且 `max_retries > 0`）的任务才会重试；
  原因不可重试或重试次数用尽后，任务落成最终 `FAILED`（`failure_reason`
  为最后一次失败的分类）。
* 重试只在失败发生后进行；成功的任务立即结束，不会再触发重试。

## 任务状态机

```
PENDING       -> RUNNING | CANCELLED
RUNNING       -> SUCCEEDED | FAILED | CANCELLED | RETRY_WAITING
RETRY_WAITING -> RUNNING | CANCELLED
SUCCEEDED / FAILED / CANCELLED  为终态，不得再被改写
```

- 所有迁移先经状态机校验：终态上的任何迁移以 `TERMINAL_STATE` 拒绝，
  其余非法迁移以 `ILLEGAL_TRANSITION` 拒绝，两者可区分。
- 每次迁移都是乐观锁条件更新（`version` 比对）。取消与完成/安排重试几乎
  同时发生时，只有一个迁移能赢得版本号，另一方观察到失败并以对方结果为
  最终结论——最终状态唯一且稳定。
- 执行器领取一次尝试时执行 `(PENDING|RETRY_WAITING) -> RUNNING` 并在同一
  事务写入一条 `IN_PROGRESS` 尝试记录；排队或等待重试期间被取消的任务
  领取失败、不会被执行，也不会多出尝试记录。
- 一次尝试失败且仍有预算时进入 `RETRY_WAITING`，由轻量定时器（不占执行
  并发槽）在间隔到期后自动开始下一次尝试。

## 失败原因分类（FailureReason）

执行失败被归一化为以下可区分的原因，内部异常细节只进服务端日志，
不写入任务记录、不通过 API 透出：

| 原因 | 含义 | 默认可重试 |
| --- | --- | --- |
| `HANDLER_ERROR` | 业务处理器抛出异常 | 是 |
| `HANDLER_TIMEOUT` | 执行超过 `handler_timeout` | 是 |
| `INTERRUPTED` | 进程中断时尝试处于 RUNNING，重启后判定 | 由 `retry_on_interruption` 决定 |

## 拒绝原因分类（RejectReason）

| 原因 | HTTP | 含义 |
| --- | --- | --- |
| `OVER_CAPACITY` | 503 | 系统持有名额（PENDING + RETRY_WAITING）已满，请求被明确拒绝（不无限排队） |
| `ILLEGAL_TRANSITION` | 409 | 非法状态迁移 |
| `TERMINAL_STATE` | 409 | 任务已在终态，不得改写 |
| `TASK_NOT_FOUND` | 404 | 任务不存在 |

## 幂等与容量

- 相同 `idempotency_key` 的重复提交返回首次创建的任务
  （响应中 `duplicated=true`），不产生重复任务，也不占用容量；
  重复提交携带的新重试策略会被忽略，一律以首次提交为准。
- 容量判定与插入在提交锁内原子完成；并发同键提交只会产生一个任务。
- **等待重试的任务保留自己的名额**：容量统计为
  `PENDING + RETRY_WAITING`（RUNNING 占用并发槽而非队列名额）。
  等待重试的任务不会被新请求挤掉，也不会被容量判定丢弃；新请求超过
  `max_pending` 时仍然立即以 `OVER_CAPACITY` 拒绝，而不是无限堆积。
- 执行并发度（含所有重试尝试）由 `max_concurrency` 限定。

## 取消语义（含取消撞上重试）

- PENDING / RETRY_WAITING 中取消：迁移为 CANCELLED；执行器（或已挂起的
  重试定时器）到期领取时乐观锁失败，安全跳过，不会再执行。
- RUNNING 中取消：与"成功 / 最终失败 / 安排重试"迁移竞争同一把乐观锁，
  只有一方生效。取消正好撞上某一次重试时，最终结论要么是 CANCELLED，
  要么继续走向该次尝试的结论，二者唯一且稳定；输掉的一方不再挂定时器。
- 终态任务上的取消以 `TERMINAL_STATE` 拒绝。

## 中断恢复（含等待重试与执行中尝试）

任务记录与尝试记录持久化于 SQLite（WAL 模式）。进程重启时：

1. 上次仍处于 `RUNNING` 的未闭合尝试：在同一事务内闭合为
   `FAILED / INTERRUPTED`。携带重试策略且预算允许
   （`retry_on_interruption=true`，计数尝试要求仍有剩余次数；不计数尝试
   只要求总尝试数未达上限）时转入 `RETRY_WAITING` 按策略继续；
   否则落成最终 `FAILED / INTERRUPTED`——无策略任务与旧版本行为一致。
   被打断的那次不会被重复执行，尝试序号也不会重复计数。
2. 上次处于 `RETRY_WAITING` 的任务：状态与 `next_run_at` 不被改写，
   重启后按原计划重新挂定时器；若等待间隔在停止期间已经到期，
   重启后立即开始下一次尝试（错过的间隔不补等）。
3. 仍处于 `PENDING` 的任务被重新派发执行。
4. 终态任务不受影响（版本号不变）。

## 配置项

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `TASKPIPE_MAX_CONCURRENCY` | 4 | 同时执行的尝试数上限（重试也受限） |
| `TASKPIPE_MAX_PENDING` | 16 | 持有名额上限（PENDING + RETRY_WAITING），超出即拒绝 |
| `TASKPIPE_HANDLER_TIMEOUT` | 30 | 单次尝试执行超时（秒） |
| `TASKPIPE_DB_PATH` | taskpipe.db | SQLite 存储路径（旧库自动迁移补齐重试字段） |
| `TASKPIPE_HOST` / `TASKPIPE_PORT` | 127.0.0.1 / 8000 | 监听地址 |
| `TASKPIPE_LOG_LEVEL` | INFO | 日志级别 |

## 日志与可观测性

日志中每个判定都带请求标识（`request=...`，来自 `X-Request-ID` 头或
请求体 `request_id`，缺省自动生成）、尝试序号（`attempt=1/2/...`，
并区分 `is_retry`）、状态迁移
（`decision=ATTEMPT_STARTED/RETRY_SCHEDULED/ATTEMPT_SUCCEEDED/FINAL_FAILED ...`）
与判定依据（如 `判定依据=失败类型默认可重试且预算未用尽`、
`重试次数已用尽`、`未携带重试策略，保持失败即终态`、
`RETRY_LOST_RACE/COMPLETE_LOST_RACE ...`），单测日志同样打印上述信息，
可用 `--log-cli-level=INFO` 查看。

## 本地验证重试能力

```bash
# 1. 起服务
uv run python main.py

# 2. 提交一个"前两次失败、之后成功"的任务（可用任意会偶发失败的处理器；
#    默认处理器回显 payload 并成功，可用小的 max_retries + 指数退避观察状态流转）
curl -s -X POST localhost:8000/tasks -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"demo-1","retry":{"max_retries":3,"base_delay":1,"strategy":"EXPONENTIAL"}}'

# 3. 观察任务视图：state 会在 RETRY_WAITING / RUNNING 间流转，
#    attempt_no、remaining_retries、next_run_at 随之变化
curl -s localhost:8000/tasks/<task_id>

# 4. 查看逐次尝试记录（第几次、是否重试、结果分类、下次预计开始时间）
curl -s localhost:8000/tasks/<task_id>/attempts

# 5. 等待重试期间取消
curl -s -X POST localhost:8000/tasks/<task_id>/cancel

# 6. 自动化验证（含次数用尽、等待中取消、取消撞重试、重启续跑、容量不突破）
uv run pytest tests/test_retry.py tests/test_retry_cancel_capacity.py \
  tests/test_retry_recovery.py tests/test_retry_api.py -q --log-cli-level=INFO
```
