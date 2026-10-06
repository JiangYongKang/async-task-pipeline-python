"""任务领域模型：状态、失败原因、拒绝原因与任务记录。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class TaskState(str, enum.Enum):
    """任务状态机中的状态。"""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    WAITING_RETRY = "WAITING_RETRY"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class FailureReason(str, enum.Enum):
    """执行失败的归一化分类（不向外透出内部异常细节）。"""

    HANDLER_ERROR = "HANDLER_ERROR"        # 业务处理器抛出的异常
    HANDLER_FATAL = "HANDLER_FATAL"        # 处理器显式标记的不可重试失败
    HANDLER_TIMEOUT = "HANDLER_TIMEOUT"    # 执行超时
    INTERRUPTED = "INTERRUPTED"            # 进程中断导致执行被打断（重启后判定）


class BackoffKind(str, enum.Enum):
    """重试间隔增长方式。"""

    FIXED = "FIXED"                # 固定间隔：每次都等待 interval
    EXPONENTIAL = "EXPONENTIAL"    # 指数退避：interval * 2**(已重试次数)


class AttemptKind(str, enum.Enum):
    """一次尝试是首次执行还是重试。"""

    INITIAL = "INITIAL"
    RETRY = "RETRY"


class AttemptResult(str, enum.Enum):
    """单次尝试的结局。"""

    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    CANCELLED = "CANCELLED"        # 尝试进行中任务被取消


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


@dataclass(frozen=True)
class RetryPolicy:
    """失败自动重试策略。

    * ``max_retries``：首次执行之外最多再试的次数；0 表示不重试
      （等价于"没带策略"，保持原有行为）；
    * ``interval``：重试基础间隔（秒）；
    * ``backoff``：固定间隔或指数退避。
    """

    max_retries: int = 0
    interval: float = 0.0
    backoff: BackoffKind = BackoffKind.FIXED


@dataclass
class Attempt:
    """单次尝试记录（尝试追溯）。"""

    task_id: str
    attempt_no: int                       # 从 1 开始，1 为首次执行
    kind: AttemptKind
    started_at: float
    finished_at: float | None = None      # 进行中（RUNNING）时为 None
    result: AttemptResult | None = None
    failure_reason: FailureReason | None = None
    next_run_at: float | None = None      # 该次失败后预计的下一次开始时间


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
    # ---- 重试相关（None/0 表示该任务不带重试策略，行为与原先一致）----
    max_retries: int = 0
    retry_interval: float = 0.0
    backoff: BackoffKind = BackoffKind.FIXED
    attempt_count: int = 0          # 已开始的尝试次数（进行中的也算）
    next_run_at: float | None = None  # WAITING_RETRY 时下次预计开始时间

    def to_policy(self) -> RetryPolicy | None:
        """从持久化字段还原重试策略；无策略时返回 None。"""
        if self.max_retries <= 0:
            return None
        return RetryPolicy(
            max_retries=self.max_retries,
            interval=self.retry_interval,
            backoff=self.backoff,
        )
