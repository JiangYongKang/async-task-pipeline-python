"""领域模型。

任务状态的真源在 SQLite（每次 CAS 条件更新即为裁决），
内存中的 Task 只是某一时刻的快照。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from .enums import FailureReason, TaskState


@dataclass
class Task:
    task_id: str
    idempotency_key: str | None
    handler: str
    payload: str
    state: TaskState
    failure_reason: FailureReason | None = None
    error_message: str | None = None
    result: str | None = None
    created_at: datetime = field(default_factory=datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=datetime.now(timezone.utc))

    def to_view(self) -> dict[str, object]:
        """对外视图。失败时只返回归一化原因与受控消息，不含堆栈。"""
        return {
            "task_id": self.task_id,
            "idempotency_key": self.idempotency_key,
            "handler": self.handler,
            "payload": self.payload,
            "state": self.state.value,
            "failure_reason": (
                self.failure_reason.value if self.failure_reason else None
            ),
            "error_message": self.error_message,
            "result": self.result,
            "created_at": self.created_at.isoformat() + "Z",
            "updated_at": self.updated_at.isoformat() + "Z",
        }
