"""SQLite 持久化层：任务记录的存取与条件更新（乐观锁）。

并发的状态迁移（例如取消与完成同时发生）通过单条
``UPDATE ... WHERE version = ?`` 条件语句裁决：
只有一个迁移的 WHERE 条件能命中，从而保证最终结论唯一。
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from typing import Optional

from .models import FailureReason, Task, TaskState
from .state_machine import validate_transition

logger = logging.getLogger("taskpipe.store")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id          TEXT PRIMARY KEY,
    idempotency_key  TEXT NOT NULL UNIQUE,
    payload          TEXT NOT NULL,
    state            TEXT NOT NULL,
    failure_reason   TEXT,
    result           TEXT,
    version          INTEGER NOT NULL,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL
);
"""


class TaskStore:
    """任务持久化存储（SQLite，进程内多线程共享一个连接）。"""

    def __init__(self, db_path: str) -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        with self._lock:
            self._conn.execute(_SCHEMA)

    # ------------------------------------------------------------------ 读取
    def get(self, task_id: str) -> Optional[Task]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
        return self._row_to_task(row) if row else None

    def get_by_idempotency_key(self, key: str) -> Optional[Task]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE idempotency_key = ?", (key,)
            ).fetchone()
        return self._row_to_task(row) if row else None

    def list_all(self) -> list[Task]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM tasks ORDER BY created_at"
            ).fetchall()
        return [self._row_to_task(r) for r in rows]

    # ------------------------------------------------------------------ 写入
    def insert(self, task: Task) -> bool:
        """插入新任务。

        :returns: True 插入成功；False 表示幂等键冲突（重复提交）。
        """
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO tasks (task_id, idempotency_key, payload, "
                    "state, failure_reason, result, version, created_at, "
                    "updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        task.task_id, task.idempotency_key,
                        json.dumps(task.payload), task.state.value,
                        task.failure_reason.value if task.failure_reason else None,
                        json.dumps(task.result) if task.result is not None else None,
                        task.version, task.created_at, task.updated_at,
                    ),
                )
            except sqlite3.IntegrityError:
                return False
        return True

    def transition(
        self,
        task_id: str,
        expected_version: int,
        target_state: TaskState,
        *,
        failure_reason: Optional[FailureReason] = None,
        result: object = None,
        set_result: bool = False,
    ) -> Optional[Task]:
        """条件状态迁移（乐观锁 CAS）。

        仅当任务当前版本号等于 ``expected_version`` 且
        ``当前状态 -> target_state`` 合法时才更新。

        :returns: 更新后的任务；若版本不匹配（并发竞争失败）或
            任务不存在，返回 ``None``。
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
                ).fetchone()
                if row is None:
                    self._conn.execute("ROLLBACK")
                    logger.info(
                        "task=%s transition=%s refused: TASK_NOT_FOUND",
                        task_id, target_state.value,
                    )
                    return None
                current = TaskState(row["state"])
                if row["version"] != expected_version:
                    self._conn.execute("ROLLBACK")
                    logger.info(
                        "task=%s transition=%s refused: version mismatch "
                        "(expected=%s actual=%s) -> 并发裁决失败，保持 %s",
                        task_id, target_state.value, expected_version,
                        row["version"], current.value,
                    )
                    return None
                validate_transition(current, target_state)  # 终态/非法迁移在此拒绝
                now = time.time()
                if set_result:
                    self._conn.execute(
                        "UPDATE tasks SET state=?, failure_reason=?, result=?, "
                        "version=version+1, updated_at=? WHERE task_id=?",
                        (
                            target_state.value,
                            failure_reason.value if failure_reason else None,
                            json.dumps(result) if result is not None else None,
                            now, task_id,
                        ),
                    )
                else:
                    self._conn.execute(
                        "UPDATE tasks SET state=?, failure_reason=?, "
                        "version=version+1, updated_at=? WHERE task_id=?",
                        (
                            target_state.value,
                            failure_reason.value if failure_reason else None,
                            now, task_id,
                        ),
                    )
                self._conn.execute("COMMIT")
                logger.info(
                    "task=%s transition=%s -> %s accepted (version %s -> %s)",
                    task_id, current.value, target_state.value,
                    expected_version, expected_version + 1,
                )
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return self.get(task_id)

    def recover_interrupted(self) -> list[Task]:
        """启动恢复：将上次进程中断时仍处于 RUNNING 的任务标记为
        FAILED / INTERRUPTED，使其状态确定且可查询。

        已处于终态或 PENDING 的任务不受影响（PENDING 任务可继续被执行）。
        """
        recovered: list[Task] = []
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE state = ?", (TaskState.RUNNING.value,)
            ).fetchall()
        for row in rows:
            task = self._row_to_task(row)
            logger.warning(
                "task=%s recovery: RUNNING 任务在进程中断后恢复，"
                "判定为 FAILED/INTERRUPTED (version=%s)",
                task.task_id, task.version,
            )
            updated = self.transition(
                task.task_id, task.version, TaskState.FAILED,
                failure_reason=FailureReason.INTERRUPTED,
            )
            if updated is not None:
                recovered.append(updated)
        return recovered

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ----------------------------------------------------------------- helpers
    @staticmethod
    def _row_to_task(row: sqlite3.Row) -> Task:
        return Task(
            task_id=row["task_id"],
            idempotency_key=row["idempotency_key"],
            payload=json.loads(row["payload"]),
            state=TaskState(row["state"]),
            failure_reason=(
                FailureReason(row["failure_reason"])
                if row["failure_reason"] else None
            ),
            result=(
                json.loads(row["result"]) if row["result"] is not None else None
            ),
            version=row["version"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
