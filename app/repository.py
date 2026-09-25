"""任务持久化（SQLite）。

并发与崩溃安全要点：

* 单连接 + 进程内 RLock，所有读写串行化；配合 WAL，简单且足够本地使用。
* 状态迁移使用条件 UPDATE（CAS）：
  ``UPDATE ... SET state=:target WHERE task_id=:id AND state IN (:expected...)``。
  取消与完成同时发生时，数据库只可能让一条语句提交成功，最终结论唯一。
* 写入均为单语句事务（自动提交），进程被 kill -9 时未提交事务自动回滚，
  已提交的 QUEUED/RUNNING 记录必然可在重启后查到，不会静默丢失。
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from .enums import (
    ACTIVE_STATES,
    FailureReason,
    RejectReason,
    TaskState,
)
from .errors import RejectedError
from .models import Task
from .settings import Settings

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id          TEXT PRIMARY KEY,
    idempotency_key  TEXT UNIQUE,
    handler          TEXT NOT NULL,
    payload          TEXT NOT NULL,
    state            TEXT NOT NULL,
    failure_reason   TEXT,
    error_message    TEXT,
    result           TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
)
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class TaskRepository:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None

    # ---------- 生命周期 ----------
    def initialize(self) -> None:
        with self._lock:
            path = Path(self._settings.db_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(
                str(path), check_same_thread=False, isolation_level=None
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute(_CREATE_SQL)
            self._conn = conn

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    # ---------- 映射 ----------
    @staticmethod
    def _row_to_task(row: sqlite3.Row) -> Task:
        return Task(
            task_id=row["task_id"],
            idempotency_key=row["idempotency_key"],
            handler=row["handler"],
            payload=row["payload"],
            state=TaskState(row["state"]),
            failure_reason=(
                FailureReason(row["failure_reason"])
                if row["failure_reason"]
                else None
            ),
            error_message=row["error_message"],
            result=row["result"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    # ---------- 写 ----------
    def create_task(
        self,
        task_id: str,
        idempotency_key: str | None,
        handler: str,
        payload: str,
        state: TaskState = TaskState.QUEUED,
    ) -> Task | None:
        """插入任务。幂等键冲突时返回 None（调用方应改读既有任务）。"""
        assert self._conn is not None
        ts = _now()
        try:
            with self._lock:
                self._conn.execute(
                    "INSERT INTO tasks (task_id, idempotency_key, handler, payload,"
                    " state, failure_reason, error_message, result,"
                    " created_at, updated_at)"
                    " VALUES (?,?,?,?,?,NULL,NULL,NULL,?,?)",
                    (task_id, idempotency_key, handler, payload, state.value, ts, ts),
                )
        except sqlite3.IntegrityError:
            return None
        return self.get(task_id)

    def cas_state(
        self,
        task_id: str,
        expected: frozenset[TaskState],
        target: TaskState,
        *,
        failure_reason: FailureReason | None = None,
        error_message: str | None = None,
        result: str | None = None,
    ) -> Task:
        """条件更新状态。

        * 当前状态属于 ``expected`` -> 提交迁移，返回最新任务；
        * 当前状态为终态 -> RejectedError(ALREADY_TERMINAL)；
        * 其余非法迁移 -> RejectedError(ILLEGAL_TRANSITION)。
        """
        assert self._conn is not None
        with self._lock:
            current_row = self._conn.execute(
                "SELECT state FROM tasks WHERE task_id=?", (task_id,)
            ).fetchone()
            if current_row is None:
                raise RejectedError(
                    RejectReason.TASK_NOT_FOUND, f"task {task_id!r} does not exist"
                )
            current = TaskState(current_row["state"])
            if current in expected:
                sets = ["state=?", "updated_at=?"]
                params: list[object] = [target.value, _now()]
                if failure_reason is not None:
                    sets.append("failure_reason=?")
                    params.append(failure_reason.value)
                if error_message is not None:
                    sets.append("error_message=?")
                    params.append(error_message)
                if result is not None:
                    sets.append("result=?")
                    params.append(result)
                params.append(task_id)
                params.extend(s.value for s in expected)
                cur = self._conn.execute(
                    f"UPDATE tasks SET {', '.join(sets)}"
                    " WHERE task_id=? AND state IN ({})".format(
                        ",".join("?" * len(expected))
                    ),
                    params,
                )
                if cur.rowcount != 1:
                    # 并发下被其他迁移抢先；重读后按规则判定
                    return self.cas_state(task_id, expected, target)
                task = self.get(task_id)
                assert task is not None
                return task

            detail = (
                f"task={task_id} current={current.value} target={target.value}"
                f" expected={{{','.join(s.value for s in expected)}}}"
            )
            if current.is_terminal:
                raise RejectedError(RejectReason.ALREADY_TERMINAL, detail)
            # 非终态但不在 expected 集合中：目标迁移当前不合法
            raise RejectedError(RejectReason.ILLEGAL_TRANSITION, detail)

    # ---------- 读 ----------
    def get(self, task_id: str) -> Task | None:
        assert self._conn is not None
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE task_id=?", (task_id,)
            ).fetchone()
            return self._row_to_task(row) if row else None

    def get_by_idempotency_key(self, idempotency_key: str) -> Task | None:
        assert self._conn is not None
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
            return self._row_to_task(row) if row else None

    def list_interrupted_on_startup(self) -> list[Task]:
        """启动恢复：返回所有 QUEUED / RUNNING 任务（确定性快照）。"""
        assert self._conn is not None
        placeholders = ",".join("?" * len(ACTIVE_STATES))
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM tasks WHERE state IN ({placeholders})",
                tuple(s.value for s in ACTIVE_STATES),
            ).fetchall()
            return [self._row_to_task(r) for r in rows]

    def count_active(self) -> int:
        assert self._conn is not None
        placeholders = ",".join("?" * len(ACTIVE_STATES))
        with self._lock:
            row = self._conn.execute(
                f"SELECT COUNT(*) AS c FROM tasks WHERE state IN ({placeholders})",
                tuple(s.value for s in ACTIVE_STATES),
            ).fetchone()
            return int(row["c"])
