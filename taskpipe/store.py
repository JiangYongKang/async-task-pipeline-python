"""SQLite 持久化层：任务记录的存取与条件更新（乐观锁）。

并发的状态迁移（例如取消与完成同时发生）通过单条
``UPDATE ... WHERE version = ?`` 条件语句裁决：
只有一个迁移的 WHERE 条件能命中，从而保证最终结论唯一。

重试相关的每次"决策"（领取尝试、成功、最终失败、安排重试、中断恢复）
都在同一个 ``BEGIN IMMEDIATE`` 事务内同时改写任务行并写入/闭合
attempts 表中的尝试记录，保证状态与尝试账目要么一起生效、要么都不生效，
不会出现"状态变了但尝试没记账"或同一次尝试被重复执行/重复计数。
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time

from .models import (
    AttemptRecord,
    AttemptStatus,
    FailureReason,
    RetryPolicy,
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
    retry_policy     TEXT,
    attempt_no       INTEGER NOT NULL DEFAULT 0,
    remaining_retries INTEGER NOT NULL DEFAULT 0,
    next_run_at      REAL
);
CREATE TABLE IF NOT EXISTS attempts (
    task_id      TEXT NOT NULL,
    attempt_no   INTEGER NOT NULL,
    status       TEXT NOT NULL,
    is_retry     INTEGER NOT NULL,
    failure_reason TEXT,
    counted      INTEGER NOT NULL,
    started_at   REAL NOT NULL,
    finished_at  REAL,
    next_run_at  REAL,
    PRIMARY KEY (task_id, attempt_no)
);
"""

# 旧库增量迁移：新增列及其 SQL 默认值（仅对缺失列执行 ALTER TABLE ADD COLUMN）
_COLUMN_DEFAULTS = {
    "retry_policy": "TEXT",
    "attempt_no": "INTEGER NOT NULL DEFAULT 0",
    "remaining_retries": "INTEGER NOT NULL DEFAULT 0",
    "next_run_at": "REAL",
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
            self._migrate()
            self._conn.executescript(_SCHEMA)

    # ------------------------------------------------------------------ 读取
    def get(self, task_id: str) -> Task | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
        return self._row_to_task(row) if row else None

    def get_by_idempotency_key(self, key: str) -> Task | None:
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

    def list_attempts(self, task_id: str) -> list[AttemptRecord]:
        """按尝试序号列出某任务的全部尝试记录（审计追溯，不被改写）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM attempts WHERE task_id = ? ORDER BY attempt_no",
                (task_id,),
            ).fetchall()
        return [self._row_to_attempt(r) for r in rows]

    def list_retry_waiting(self) -> list[Task]:
        """列出所有等待重试的任务（供执行器在重启后重新挂定时器）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE state = ? ORDER BY next_run_at",
                (TaskState.RETRY_WAITING.value,),
            ).fetchall()
        return [self._row_to_task(r) for r in rows]

    # ------------------------------------------------------------------ 写入
    def insert(self, task: Task) -> bool:
        """插入新任务。

        :returns: True 插入成功；False 表示幂等键冲突（重复提交）。
        """
        if task.retry_policy is not None:
            # 接受时即按策略初始化剩余重试预算
            task.remaining_retries = task.retry_policy.max_retries
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO tasks (task_id, idempotency_key, payload, "
                    "state, failure_reason, result, version, created_at, "
                    "updated_at, retry_policy, attempt_no, remaining_retries, "
                    "next_run_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        task.task_id, task.idempotency_key,
                        json.dumps(task.payload), task.state.value,
                        task.failure_reason.value if task.failure_reason else None,
                        json.dumps(task.result) if task.result is not None else None,
                        task.version, task.created_at, task.updated_at,
                        json.dumps(task.retry_policy.to_dict())
                        if task.retry_policy is not None else None,
                        task.attempt_no, task.remaining_retries,
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
        failure_reason: FailureReason | None = None,
        result: object = None,
        set_result: bool = False,
    ) -> Task | None:
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
                        "version=version+1, updated_at=?, next_run_at=NULL "
                        "WHERE task_id=?",
                        (
                            target_state.value,
                            failure_reason.value if failure_reason else None,
                            now, task_id,
                        ),
                    )
                # 取消正好落在某次尝试执行期间：把该尝试闭合为 CANCELLED，
                # 与任务状态在同一事务内落定，最终结论唯一。
                if target_state is TaskState.CANCELLED:
                    self._close_open_attempt_locked(row, AttemptStatus.CANCELLED,
                                                    None, now)
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

    # ----------------------------------------------------- 尝试相关原子决策
    def start_attempt(
        self, task_id: str, expected_version: int, *, now: float | None = None
    ) -> Task | None:
        """领取并开始下一次尝试：(PENDING|RETRY_WAITING) -> RUNNING。

        同一事务内写入一条 ``IN_PROGRESS`` 尝试记录并递增任务上的
        ``attempt_no``。若版本不匹配/任务不存在/迁移非法，返回 ``None``，
        不会留下半成品尝试（取消正好撞上重试时，输的一方安全放弃）。
        """
        now = time.time() if now is None else now
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._get_row_locked(task_id)
                if row is None or row["version"] != expected_version:
                    self._conn.execute("ROLLBACK")
                    logger.info(
                        "task=%s decision=CLAIM_REFUSED "
                        "(任务不存在或版本不匹配 expected=%s)",
                        task_id, expected_version,
                    )
                    return None
                current = TaskState(row["state"])
                validate_transition(current, TaskState.RUNNING)
                attempt_no = row["attempt_no"] + 1
                self._conn.execute(
                    "UPDATE tasks SET state=?, failure_reason=NULL, "
                    "version=version+1, updated_at=?, attempt_no=?, "
                    "next_run_at=NULL WHERE task_id=?",
                    (TaskState.RUNNING.value, now, attempt_no, task_id),
                )
                self._conn.execute(
                    "INSERT INTO attempts (task_id, attempt_no, status, "
                    "is_retry, failure_reason, counted, started_at, finished_at, "
                    "next_run_at) VALUES (?, ?, ?, ?, NULL, 1, ?, NULL, NULL)",
                    (
                        task_id, attempt_no, AttemptStatus.IN_PROGRESS.value,
                        1 if attempt_no > 1 else 0, now,
                    ),
                )
                self._conn.execute("COMMIT")
                logger.info(
                    "task=%s attempt=%s decision=ATTEMPT_STARTED %s -> RUNNING "
                    "is_retry=%s (version %s -> %s)",
                    task_id, attempt_no, current.value, attempt_no > 1,
                    expected_version, expected_version + 1,
                )
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return self.get(task_id)

    def finish_attempt_success(
        self, task_id: str, expected_version: int, result: object,
        *, now: float | None = None,
    ) -> Task | None:
        """本次尝试成功：RUNNING -> SUCCEEDED，闭合尝试记录。"""
        now = time.time() if now is None else now
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._get_row_locked(task_id)
                if row is None or row["version"] != expected_version:
                    self._conn.execute("ROLLBACK")
                    logger.info(
                        "task=%s decision=SUCCESS_REFUSED (任务不存在或版本不匹配)",
                        task_id,
                    )
                    return None
                current = TaskState(row["state"])
                validate_transition(current, TaskState.SUCCEEDED)
                self._conn.execute(
                    "UPDATE tasks SET state=?, failure_reason=NULL, result=?, "
                    "version=version+1, updated_at=?, next_run_at=NULL "
                    "WHERE task_id=?",
                    (
                        TaskState.SUCCEEDED.value,
                        json.dumps(result) if result is not None else None,
                        now, task_id,
                    ),
                )
                self._close_attempt_locked(
                    row, AttemptStatus.SUCCEEDED, None, now, None
                )
                self._conn.execute("COMMIT")
                logger.info(
                    "task=%s attempt=%s decision=ATTEMPT_SUCCEEDED -> SUCCEEDED "
                    "(version %s -> %s)",
                    task_id, row["attempt_no"], expected_version,
                    expected_version + 1,
                )
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return self.get(task_id)

    def fail_attempt_final(
        self,
        task_id: str,
        expected_version: int,
        reason: FailureReason,
        *,
        counted: bool = True,
        now: float | None = None,
    ) -> Task | None:
        """本次尝试失败且不再重试：RUNNING -> FAILED（最终失败），闭合尝试。"""
        now = time.time() if now is None else now
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._get_row_locked(task_id)
                if row is None or row["version"] != expected_version:
                    self._conn.execute("ROLLBACK")
                    logger.info(
                        "task=%s decision=FINAL_FAIL_REFUSED "
                        "(任务不存在或版本不匹配)", task_id,
                    )
                    return None
                current = TaskState(row["state"])
                validate_transition(current, TaskState.FAILED)
                legacy_no = self._ensure_open_attempt_locked(row, now)
                if legacy_no is not None:
                    row = self._get_row_locked(task_id)
                self._conn.execute(
                    "UPDATE tasks SET state=?, failure_reason=?, "
                    "version=version+1, updated_at=?, next_run_at=NULL "
                    "WHERE task_id=?",
                    (TaskState.FAILED.value, reason.value, now, task_id),
                )
                self._close_attempt_locked(
                    row, AttemptStatus.FAILED, reason, now, None, counted=counted
                )
                self._conn.execute("COMMIT")
                logger.info(
                    "task=%s attempt=%s decision=ATTEMPT_FAILED_FINAL reason=%s "
                    "-> FAILED (重试预算已用尽或不重试)",
                    task_id, row["attempt_no"], reason.value,
                )
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return self.get(task_id)

    def schedule_retry(
        self,
        task_id: str,
        expected_version: int,
        reason: FailureReason,
        *,
        next_run_at: float,
        consume_retry: bool,
        counted: bool = True,
        now: float | None = None,
    ) -> Task | None:
        """本次尝试失败但仍有重试预算：RUNNING -> RETRY_WAITING。

        同一事务内闭合本次尝试（``next_run_at`` 给出下一次预计开始时间），
        并在任务行记录剩余次数与下次执行时刻。
        ``consume_retry=False`` 用于"中断不计数"的尝试：预算不递减。
        """
        now = time.time() if now is None else now
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._get_row_locked(task_id)
                if row is None or row["version"] != expected_version:
                    self._conn.execute("ROLLBACK")
                    logger.info(
                        "task=%s decision=RETRY_SCHEDULE_REFUSED "
                        "(任务不存在或版本不匹配)", task_id,
                    )
                    return None
                current = TaskState(row["state"])
                validate_transition(current, TaskState.RETRY_WAITING)
                legacy_no = self._ensure_open_attempt_locked(row, now)
                if legacy_no is not None:
                    row = self._get_row_locked(task_id)
                remaining = row["remaining_retries"] - (1 if consume_retry else 0)
                self._conn.execute(
                    "UPDATE tasks SET state=?, failure_reason=?, "
                    "version=version+1, updated_at=?, remaining_retries=?, "
                    "next_run_at=? WHERE task_id=?",
                    (
                        TaskState.RETRY_WAITING.value, reason.value, now,
                        remaining, next_run_at, task_id,
                    ),
                )
                self._close_attempt_locked(
                    row, AttemptStatus.FAILED, reason, now, next_run_at,
                    counted=counted,
                )
                self._conn.execute("COMMIT")
                logger.info(
                    "task=%s attempt=%s decision=RETRY_SCHEDULED reason=%s "
                    "-> RETRY_WAITING next_run_at=%.3f remaining_retries=%s "
                    "consumed=%s",
                    task_id, row["attempt_no"], reason.value, next_run_at,
                    remaining, consume_retry,
                )
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return self.get(task_id)

    def recover_interrupted(self, *, now: float | None = None) -> list[Task]:
        """启动恢复：为上次进程中断时未闭合的尝试给出确定去向。

        * 上次仍处于 ``RUNNING``（无论有无重试策略）：闭合未闭合的尝试为
          ``FAILED / INTERRUPTED``。带重试策略且预算允许的，转入
          ``RETRY_WAITING`` 按原策略等待下一次尝试；否则落成最终
          ``FAILED / INTERRUPTED``——与无策略任务的历史行为一致。
        * 上次处于 ``RETRY_WAITING``：不动状态与 ``next_run_at``，
          由执行器按原计划重新挂定时器（到点立即执行错过的间隔）。
        * PENDING 任务由执行器重新派发；终态任务不受影响。

        "中断的那次算不算一次尝试"由策略的
        ``count_interruption_as_attempt`` 决定（审计记录始终保留）。
        """
        now = time.time() if now is None else now
        recovered: list[Task] = []
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE state = ?",
                (TaskState.RUNNING.value,),
            ).fetchall()
        for row in rows:
            task = self._row_to_task(row)
            policy = task.retry_policy
            counted = policy is None or policy.count_interruption_as_attempt
            will_retry = (
                policy is not None
                and policy.retry_on_interruption
                and (task.remaining_retries > 0 or not counted)
            )
            if will_retry:
                assert policy is not None
                upcoming = task.attempt_no + 1
                next_run_at = now + policy.delay_for(upcoming)
                logger.warning(
                    "task=%s attempt=%s recovery: RUNNING 尝试被进程中断，"
                    "按策略继续重试 counted=%s next_run_at=%.3f (version=%s)",
                    task.task_id, task.attempt_no, counted, next_run_at,
                    task.version,
                )
                updated = self.schedule_retry(
                    task.task_id, task.version, FailureReason.INTERRUPTED,
                    next_run_at=next_run_at, consume_retry=counted,
                    counted=counted, now=now,
                )
            else:
                reason_extra = (
                    "无重试策略" if policy is None
                    else ("策略不重试中断" if not policy.retry_on_interruption
                          else "重试预算已用尽")
                )
                logger.warning(
                    "task=%s attempt=%s recovery: RUNNING 尝试被进程中断，"
                    "判定为 FAILED/INTERRUPTED counted=%s (%s) (version=%s)",
                    task.task_id, task.attempt_no, counted, reason_extra,
                    task.version,
                )
                updated = self.fail_attempt_final(
                    task.task_id, task.version, FailureReason.INTERRUPTED,
                    counted=counted, now=now,
                )
            if updated is not None:
                recovered.append(updated)
        return recovered

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ----------------------------------------------------------------- helpers
    def _migrate(self) -> None:
        """旧库增量迁移：为已存在的 tasks 表补齐重试相关列。"""
        existing = {
            r["name"]
            for r in self._conn.execute("PRAGMA table_info(tasks)").fetchall()
        }
        if not existing:
            return  # 全新库，直接用完整 schema 建表
        for column, decl in _COLUMN_DEFAULTS.items():
            if column not in existing:
                logger.info("migration: tasks 表补齐列 %s (%s)", column, decl)
                self._conn.execute(
                    f"ALTER TABLE tasks ADD COLUMN {column} {decl}"
                )

    def _get_row_locked(self, task_id: str) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
        ).fetchone()

    def _close_attempt_locked(
        self,
        task_row: sqlite3.Row,
        status: AttemptStatus,
        reason: FailureReason | None,
        finished_at: float,
        next_run_at: float | None,
        *,
        counted: bool = True,
    ) -> None:
        """闭合任务当前（IN_PROGRESS）的尝试记录。调用方须已持事务。"""
        self._conn.execute(
            "UPDATE attempts SET status=?, failure_reason=?, counted=?, "
            "finished_at=?, next_run_at=? WHERE task_id=? AND attempt_no=? "
            "AND status=?",
            (
                status.value, reason.value if reason else None,
                1 if counted else 0, finished_at, next_run_at,
                task_row["task_id"], task_row["attempt_no"],
                AttemptStatus.IN_PROGRESS.value,
            ),
        )

    def _ensure_open_attempt_locked(
        self, task_row: sqlite3.Row, now: float
    ) -> int | None:
        """兼容旧版本遗留数据：RUNNING 但没有 IN_PROGRESS 尝试记录
        （attempt_no=0）时，补记为第 1 次尝试，并把任务行 attempt_no 置 1。

        :returns: 补记的尝试序号；若尝试记录本就存在则返回 None。
        """
        existing = self._conn.execute(
            "SELECT 1 FROM attempts WHERE task_id=? AND attempt_no=? "
            "AND status=?",
            (
                task_row["task_id"], task_row["attempt_no"],
                AttemptStatus.IN_PROGRESS.value,
            ),
        ).fetchone()
        if existing is not None:
            return None
        # 旧库 RUNNING 任务 attempt_no 恒为 0，补成第 1 次（首次执行）
        attempt_no = max(1, task_row["attempt_no"])
        self._conn.execute(
            "UPDATE tasks SET attempt_no=? WHERE task_id=?",
            (attempt_no, task_row["task_id"]),
        )
        self._conn.execute(
            "INSERT INTO attempts (task_id, attempt_no, status, is_retry, "
            "failure_reason, counted, started_at, finished_at, next_run_at) "
            "VALUES (?, ?, ?, 0, NULL, 1, ?, NULL, NULL)",
            (
                task_row["task_id"], attempt_no,
                AttemptStatus.IN_PROGRESS.value,
                task_row["updated_at"] or now,
            ),
        )
        logger.info(
            "task=%s migration: 旧版本遗留 RUNNING 无尝试记录，补记为 "
            "attempt=%s", task_row["task_id"], attempt_no,
        )
        return attempt_no

    def _close_open_attempt_locked(
        self,
        task_row: sqlite3.Row,
        status: AttemptStatus,
        reason: FailureReason | None,
        finished_at: float,
    ) -> None:
        """闭合尝试但保留其原有 counted 取值（用于取消竞态）。"""
        self._conn.execute(
            "UPDATE attempts SET status=?, failure_reason=?, finished_at=? "
            "WHERE task_id=? AND attempt_no=? AND status=?",
            (
                status.value, reason.value if reason else None, finished_at,
                task_row["task_id"], task_row["attempt_no"],
                AttemptStatus.IN_PROGRESS.value,
            ),
        )

    @staticmethod
    def _row_to_attempt(row: sqlite3.Row) -> AttemptRecord:
        return AttemptRecord(
            task_id=row["task_id"],
            attempt_no=row["attempt_no"],
            status=AttemptStatus(row["status"]),
            is_retry=bool(row["is_retry"]),
            failure_reason=(
                FailureReason(row["failure_reason"])
                if row["failure_reason"] else None
            ),
            counted=bool(row["counted"]),
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            next_run_at=row["next_run_at"],
        )

    @staticmethod
    def _row_to_task(row: sqlite3.Row) -> Task:
        keys = row.keys()
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
            retry_policy=(
                RetryPolicy.from_dict(json.loads(row["retry_policy"]))
                if "retry_policy" in keys and row["retry_policy"] else None
            ),
            attempt_no=row["attempt_no"] if "attempt_no" in keys else 0,
            remaining_retries=(
                row["remaining_retries"]
                if "remaining_retries" in keys else 0
            ),
            next_run_at=(
                row["next_run_at"] if "next_run_at" in keys else None
            ),
        )
