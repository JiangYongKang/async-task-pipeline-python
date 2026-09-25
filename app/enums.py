"""任务状态机、失败原因、拒绝原因的枚举定义。

状态机（合法迁移）::

    QUEUED    -> RUNNING | CANCELLED
    RUNNING   -> SUCCEEDED | FAILED | CANCELLED
    SUCCEEDED / FAILED / CANCELLED 为终态，不可再改写。

取消与完成几乎同时发生时，所有迁移都通过 repository 的 CAS
（Compare-And-Set，条件更新）裁决：只有一个迁移能成功，因此最终结论唯一。
"""

from __future__ import annotations

from enum import Enum


class TaskState(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def is_terminal(self) -> bool:
        return self in _TERMINAL_STATES


class FailureReason(str, Enum):
    """执行失败的归一化原因（不向客户端透出内部异常原文）。"""

    HANDLER_ERROR = "HANDLER_ERROR"      # 处理函数抛出普通异常
    TIMEOUT = "TIMEOUT"                  # 执行超时（被守护线程判定）
    INTERRUPTED = "INTERRUPTED"          # 进程中断导致任务未走完终态


class RejectReason(str, Enum):
    """请求被拒绝的可区分原因（对应 HTTP 4xx，错误体中带 reason 字段）。"""

    # —— 提交阶段 ——
    CAPACITY_FULL = "CAPACITY_FULL"            # 在制任务数已达容量上限
    INVALID_HANDLER = "INVALID_HANDLER"        # 未知处理函数
    INVALID_PAYLOAD = "INVALID_PAYLOAD"        # 载荷校验失败
    # —— 状态迁移阶段 ——
    ALREADY_TERMINAL = "ALREADY_TERMINAL"      # 终态任务被再次改写
    ILLEGAL_TRANSITION = "ILLEGAL_TRANSITION"  # 当前状态不允许目标迁移
    # —— 查询阶段 ——
    TASK_NOT_FOUND = "TASK_NOT_FOUND"          # 任务不存在


_TERMINAL_STATES = frozenset(
    {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}
)

# 合法状态迁移表。表中不存在的迁移一律拒绝。
ALLOWED_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.QUEUED: frozenset({TaskState.RUNNING, TaskState.CANCELLED}),
    TaskState.RUNNING: frozenset(
        {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}
    ),
    TaskState.SUCCEEDED: frozenset(),
    TaskState.FAILED: frozenset(),
    TaskState.CANCELLED: frozenset(),
}

# 在制任务（占用容量）：已提交但未到终态。
ACTIVE_STATES = frozenset({TaskState.QUEUED, TaskState.RUNNING})
