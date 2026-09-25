"""任务领域模型：状态、失败原因、拒绝原因与任务记录。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class TaskState(str, enum.Enum):
    """任务状态机中的状态。"""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class FailureReason(str, enum.Enum):
    """执行失败的归一化分类（不向外透出内部异常细节）。"""

    HANDLER_ERROR = "HANDLER_ERROR"        # 业务处理器抛出的异常
    HANDLER_TIMEOUT = "HANDLER_TIMEOUT"    # 执行超时
    INTERRUPTED = "INTERRUPTED"            # 进程中断导致执行被打断（重启后判定）


class RejectReason(str, enum.Enum):
    """提交/状态迁移被拒绝的可区分原因。"""

    OVER_CAPACITY = "OVER_CAPACITY"                # 超出承载能力
    ILLEGAL_TRANSITION = "ILLEGAL_TRANSITION"      # 非法状态迁移
    TERMINAL_STATE = "TERMINAL_STATE"              # 终态不可改写
    TASK_NOT_FOUND = "TASK_NOT_FOUND"              # 任务不存在


class TransitionError(Exception):
    """非法状态迁移异常，携带可区分的拒绝原因。"""

    def __init__(self, reason: RejectReason, message: str) -> None:
        super().__init__(message)
        self.reason = reason


@dataclass
class Task:
    """任务记录。"""

    task_id: str
    idempotency_key: str
    payload: dict
    state: TaskState = TaskState.PENDING
    failure_reason: FailureReason | None = None
    result: object | None = None
    version: int = 0  # 乐观锁版本号，用于并发迁移裁决
    created_at: float = 0.0
    updated_at: float = 0.0
