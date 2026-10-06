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
from typing import Callable, Optional

from .models import (
    Attempt,
    AttemptKind,
    AttemptResult,
    BackoffKind,
    FailureReason,
    Task,
    TaskState,
)
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
    updated_at       REAL NOT NULL,
    max_retries      INTEGER NOT NULL DEFAULT 0,
    retry_interval   REAL NOT NULL DEFAULT 0,
    backoff          TEXT NOT NULL DEFAULT 'FIXED',
    attempt_count    INTEGER NOT NULL DEFAULT 0,
    next_run_at      REAL
);

CREATE TABLE IF NOT EXISTS attempts (
    task_id        TEXT NOT NULL,
    attempt_no     INTEGER NOT NULL,
    kind           TEXT NOT NULL,
    started_at     REAL NOT NULL,
    finished_at    REAL,
    result         TEXT,
    failure_reason TEXT,
    next_run_at    REAL,
    PRIMARY KEY (task_id, attempt_no)
);
"""

# 旧库升级：列名 -> 建列 DDL 片段（SQLite 不支持一次加多列）
_COLUMN_MIGRATIONS = {
    "max_retries": "ALTER TABLE tasks ADD COLUMN max_retries INTEGER NOT NULL DEFAULT 0",
    "retry_interval": "ALTER TABLE tasks ADD COLUMN retry_interval REAL NOT NULL DEFAULT 0",
    "backoff": "ALTER TABLE tasks ADD COLUMN backoff TEXT NOT NULL DEFAULT 'FIXED'",
    "attempt_count": "ALTER TABLE tasks ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0",
    "next_run_at": "ALTER TABLE tasks ADD COLUMN next_run_at REAL",
}


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
            self._conn.executescript(_SCHEMA)
            self._migrate_columns()

    def _migrate_columns(self) -> None:
        """为旧版本数据库补齐重试相关列（幂等）。"""
        existing = {
            r["name"]
            for r in self._conn.execute("PRAGMA table_info(tasks)").fetchall()
        }
        for name, ddl in _COLUMN_MIGRATIONS.items():
            if name not in existing:
                logger.info("schema migration: tasks.%s 补齐列", name)
                self._conn.execute(ddl)

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
                    "updated_at, max_retries, retry_interval, backoff, "
                    "attempt_count, next_run_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        task.task_id, task.idempotency_key,
                        json.dumps(task.payload), task.state.value,
                        task.failure_reason.value if task.failure_reason else None,
                        json.dumps(task.result) if task.result is not None else None,
                        task.version, task.created_at, task.updated_at,
                        task.max_retries, task.retry_interval,
                        task.backoff.value, task.attempt_count,
                        task.next_run_at,
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

    # ------------------------------------------------------------------ 领取
    def claim(
        self, task_id: str, expected_version: int, *, now: float | None = None
    ) -> Optional[Task]:
        """原子领取：PENDING/WAITING_RETRY -> RUNNING，并登记一次新尝试。

        状态迁移、``attempt_count`` 自增与 ``attempts`` 行插入在同一事务中
        完成，因此"尝试计数"与"进入 RUNNING"不可分割：

        * 重启/并发都不可能把同一次尝试重复执行或重复计数；
        * 排队期间被取消（版本变化）则整体失败、不会登记尝试。

        :returns: 领取后的任务；版本不匹配/任务不存在/状态不允许时返回 None。
        """
        now = time.time() if now is None else now
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
                ).fetchone()
                if row is None:
                    self._conn.execute("ROLLBACK")
                    logger.info(
                        "task=%s claim refused: TASK_NOT_FOUND", task_id
                    )
                    return None
                if row["version"] != expected_version:
                    self._conn.execute("ROLLBACK")
                    logger.info(
                        "task=%s claim refused: version mismatch "
                        "(expected=%s actual=%s)",
                        task_id, expected_version, row["version"],
                    )
                    return None
                current = TaskState(row["state"])
                validate_transition(current, TaskState.RUNNING)
                attempt_no = row["attempt_count"] + 1
                kind = (
                    AttemptKind.INITIAL if attempt_no == 1 else AttemptKind.RETRY
                )
                self._conn.execute(
                    "UPDATE tasks SET state=?, attempt_count=?, "
                    "next_run_at=NULL, version=version+1, updated_at=? "
                    "WHERE task_id=?",
                    (TaskState.RUNNING.value, attempt_no, now, task_id),
                )
                self._conn.execute(
                    "INSERT INTO attempts (task_id, attempt_no, kind, "
                    "started_at) VALUES (?, ?, ?, ?)",
                    (task_id, attempt_no, kind.value, now),
                )
                self._conn.execute("COMMIT")
                logger.info(
                    "task=%s claim accepted: %s -> RUNNING attempt_no=%s kind=%s "
                    "(version %s -> %s)",
                    task_id, current.value, attempt_no, kind.value,
                    expected_version, expected_version + 1,
                )
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return self.get(task_id)

    # ------------------------------------------------------------- 尝试读取
    def list_attempts(self, task_id: str) -> list[Attempt]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM attempts WHERE task_id = ? ORDER BY attempt_no",
                (task_id,),
            ).fetchall()
        return [self._row_to_attempt(r) for r in rows]

    def list_due_retries(self, *, now: float | None = None) -> list[Task]:
        """返回已到点（next_run_at <= now）的 WAITING_RETRY 任务。"""
        now = time.time() if now is None else now
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE state = ? AND next_run_at <= ? "
                "ORDER BY next_run_at",
                (TaskState.WAITING_RETRY.value, now),
            ).fetchall()
        return [self._row_to_task(r) for r in rows]

    def finish_attempt(
        self,
        task_id: str,
        expected_version: int,
        target_state: TaskState,
        *,
        failure_reason: Optional[FailureReason] = None,
        result: object = None,
        set_result: bool = False,
        next_run_at: Optional[float] = None,
        now: float | None = None,
    ) -> Optional[Task]:
        """原子收尾一次尝试：RUNNING -> 目标状态，并更新对应 attempts 行。

        状态迁移、版本号递增与尝试结局（SUCCESS/FAILURE/CANCELLED）
        在同一事务内完成，保证任务状态与尝试追溯永远一致。

        :param next_run_at: 目标为 WAITING_RETRY 时的下次预计开始时间，
            同时写入任务与该次尝试行。
        :returns: 更新后的任务；版本不匹配（与取消等竞争失败）或任务不存在
            返回 None；非法/终态迁移抛 TransitionError。
        """
        now = time.time() if now is None else now
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
                ).fetchone()
                if row is None:
                    self._conn.execute("ROLLBACK")
                    logger.info(
                        "task=%s finish=%s refused: TASK_NOT_FOUND",
                        task_id, target_state.value,
                    )
                    return None
                if row["version"] != expected_version:
                    self._conn.execute("ROLLBACK")
                    logger.info(
                        "task=%s finish=%s refused: version mismatch "
                        "(expected=%s actual=%s) -> 并发裁决失败，保持 %s",
                        task_id, target_state.value, expected_version,
                        row["version"], row["state"],
                    )
                    return None
                current = TaskState(row["state"])
                validate_transition(current, target_state)
                attempt_no = row["attempt_count"]
                if target_state is TaskState.SUCCEEDED:
                    attempt_result = AttemptResult.SUCCESS
                elif target_state is TaskState.CANCELLED:
                    attempt_result = AttemptResult.CANCELLED
                else:
                    attempt_result = AttemptResult.FAILURE
                failure_value = (
                    failure_reason.value if failure_reason else None
                )
                self._conn.execute(
                    "UPDATE tasks SET state=?, failure_reason=?, "
                    "result=CASE WHEN ? THEN ? ELSE result END, "
                    "next_run_at=?, version=version+1, updated_at=? "
                    "WHERE task_id=?",
                    (
                        target_state.value, failure_value,
                        1 if set_result else 0,
                        json.dumps(result) if set_result else None,
                        next_run_at, now, task_id,
                    ),
                )
                self._conn.execute(
                    "UPDATE attempts SET finished_at=?, result=?, "
                    "failure_reason=?, next_run_at=? "
                    "WHERE task_id=? AND attempt_no=?",
                    (
                        now, attempt_result.value, failure_value, next_run_at,
                        task_id, attempt_no,
                    ),
                )
                self._conn.execute("COMMIT")
                logger.info(
                    "task=%s finish accepted: %s -> %s attempt_no=%s "
                    "attempt_result=%s reason=%s (version %s -> %s)",
                    task_id, current.value, target_state.value, attempt_no,
                    attempt_result.value, failure_value,
                    expected_version, expected_version + 1,
                )
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return self.get(task_id)

    def cancel(
        self, task_id: str, expected_version: int, *, now: float | None = None
    ) -> Optional[Task]:
        """原子取消：PENDING/WAITING_RETRY/RUNNING -> CANCELLED。

        若取消时任务在 RUNNING，进行中的那一次尝试一并收尾为 CANCELLED；
        PENDING（尚未开始任何尝试）/WAITING_RETRY（尝试均已收尾）
        不产生尝试行改动。与执行器完成迁移竞争同一版本号，赢家唯一。
        """
        now = time.time() if now is None else now
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
                ).fetchone()
                if row is None:
                    self._conn.execute("ROLLBACK")
                    return None
                if row["version"] != expected_version:
                    self._conn.execute("ROLLBACK")
                    logger.info(
                        "task=%s cancel refused: version mismatch "
                        "(expected=%s actual=%s)",
                        task_id, expected_version, row["version"],
                    )
                    return None
                current = TaskState(row["state"])
                validate_transition(current, TaskState.CANCELLED)
                self._conn.execute(
                    "UPDATE tasks SET state=?, failure_reason=NULL, "
                    "next_run_at=NULL, version=version+1, updated_at=? "
                    "WHERE task_id=?",
                    (TaskState.CANCELLED.value, now, task_id),
                )
                if current is TaskState.RUNNING:
                    # 新版本任务：收尾被取消的这一次进行中尝试。
                    # 旧版本遗留的 RUNNING 任务没有 attempts 行（attempt_no=0），
                    # 更新命中 0 行，无需特殊处理。
                    attempt_no = row["attempt_count"]
                    self._conn.execute(
                        "UPDATE attempts SET finished_at=?, result=?, "
                        "failure_reason=NULL, next_run_at=NULL "
                        "WHERE task_id=? AND attempt_no=?",
                        (now, AttemptResult.CANCELLED.value, task_id, attempt_no),
                    )
                self._conn.execute("COMMIT")
                logger.info(
                    "task=%s cancel accepted: %s -> CANCELLED "
                    "(version %s -> %s)",
                    task_id, current.value, expected_version,
                    expected_version + 1,
                )
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return self.get(task_id)

    def recover_interrupted(
        self,
        retry_decider: (
            Callable[[Task, FailureReason], tuple[bool, float]] | None
        ) = None,
    ) -> list[Task]:
        """启动恢复：为上次进程中断时仍处于 RUNNING 的任务给出确定去向。

        被打断的那一次尝试**确定算作一次尝试**：在同一事务中被收尾为
        FAILURE/INTERRUPTED，随后：

        * 策略允许继续重试（``retry_decider`` 返回 ``(True, delay)``）：
          RUNNING -> WAITING_RETRY，按原策略计算 next_run_at，重启后
          由调度线程到期接着跑；
        * 无重试策略 / 失败不可重试 / 次数已用尽：
          RUNNING -> FAILED / INTERRUPTED，落为最终失败；
        * ``retry_decider=None``（向后兼容）：一律 FAILED / INTERRUPTED。

        已处于终态、PENDING 或 WAITING_RETRY 的任务不受影响。
        PENDING 任务继续可被执行；WAITING_RETRY 任务由执行器调度线程
        按原 next_run_at 接着跑（到期点已过则立即执行）。
        """
        recovered: list[Task] = []
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE state = ?",
                (TaskState.RUNNING.value,),
            ).fetchall()
        for row in rows:
            task = self._row_to_task(row)
            reason = FailureReason.INTERRUPTED
            retry_it, delay = (
                retry_decider(task, reason) if retry_decider else (False, 0.0)
            )
            target = (
                TaskState.WAITING_RETRY if retry_it else TaskState.FAILED
            )
            next_run_at = time.time() + delay if retry_it else None
            logger.warning(
                "task=%s recovery: RUNNING 任务在进程中断后恢复，"
                "被打断的尝试 attempt_no=%s 收尾为 FAILURE/INTERRUPTED"
                "（计入尝试次数），去向=%s%s (version=%s)",
                task.task_id, task.attempt_count, target.value,
                f" delay={delay:.3f}s" if retry_it else "", task.version,
            )
            updated = self.finish_attempt(
                task.task_id, task.version, target,
                failure_reason=reason, next_run_at=next_run_at,
            )
            if updated is not None:
                recovered.append(updated)
        return recovered

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ----------------------------------------------------------------- helpers
    @staticmethod
    def _row_to_attempt(row: sqlite3.Row) -> Attempt:
        return Attempt(
            task_id=row["task_id"],
            attempt_no=row["attempt_no"],
            kind=AttemptKind(row["kind"]),
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            result=AttemptResult(row["result"]) if row["result"] else None,
            failure_reason=(
                FailureReason(row["failure_reason"])
                if row["failure_reason"]
                else None
            ),
            next_run_at=row["next_run_at"],
        )

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
            max_retries=row["max_retries"],
            retry_interval=row["retry_interval"],
            backoff=BackoffKind(row["backoff"]),
            attempt_count=row["attempt_count"],
            next_run_at=row["next_run_at"],
        )
