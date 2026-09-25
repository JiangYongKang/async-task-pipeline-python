"""可区分的业务异常。所有业务异常携带可枚举的原因码，
HTTP 层据此映射状态码与结构化错误体，内部异常原文不会直接透出。"""

from __future__ import annotations

from .enums import RejectReason


class TaskPipelineError(Exception):
    """所有业务错误的基类。"""


class RejectedError(TaskPipelineError):
    """请求被拒绝（容量、非法迁移、终态改写等）。

    :param reason: 机器可判定的拒绝原因码
    :param detail: 面向日志/排查的人类可读说明
    """

    def __init__(self, reason: RejectReason, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail or reason.value
        super().__init__(f"{reason.value}: {self.detail}")


class TaskNotFoundError(RejectedError):
    def __init__(self, task_id: str) -> None:
        super().__init__(
            RejectReason.TASK_NOT_FOUND, f"task {task_id!r} does not exist"
        )
