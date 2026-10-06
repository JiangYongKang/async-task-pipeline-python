"""FastAPI 路由层与应用装配。

每次请求都带请求标识（优先使用请求体/请求头 ``X-Request-ID``，否则生成），
日志打印请求标识、状态迁移与判定依据；拒绝原因以结构化 JSON 返回。
"""
from __future__ import annotations

import logging
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .config import Settings
from .executor import Executor, Handler
from .models import (
    BackoffKind,
    RejectReason,
    RetryPolicy,
    TaskState,
    TransitionError,
)
from .service import ServiceRejected, TaskService
from .store import TaskStore

logger = logging.getLogger("taskpipe.api")


def configure_logging() -> None:
    """配置日志格式（含请求标识由各模块自行写入消息）。"""
    logging.basicConfig(
        level=os.environ.get("TASKPIPE_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        force=True,
    )


def settings_from_env() -> Settings:
    """从环境变量加载配置。"""
    return Settings(
        max_concurrency=int(os.environ.get("TASKPIPE_MAX_CONCURRENCY", "4")),
        max_pending=int(os.environ.get("TASKPIPE_MAX_PENDING", "16")),
        handler_timeout=float(os.environ.get("TASKPIPE_HANDLER_TIMEOUT", "30")),
        db_path=os.environ.get("TASKPIPE_DB_PATH", "taskpipe.db"),
    )


class RetryBody(BaseModel):
    max_retries: int = Field(default=0, ge=0)
    interval: float = Field(default=0.0, ge=0)
    backoff: BackoffKind = BackoffKind.FIXED


class SubmitBody(BaseModel):
    idempotency_key: str = Field(..., min_length=1)
    payload: dict = Field(default_factory=dict)
    request_id: str | None = None
    retry: RetryBody | None = None


class TaskView(BaseModel):
    task_id: str
    idempotency_key: str
    state: str
    failure_reason: str | None
    result: object | None
    version: int
    attempt_count: int
    retries_remaining: int | None
    next_run_at: float | None


def _task_view(task, duplicated: bool | None = None) -> dict:
    remaining = (
        max(0, task.max_retries - task.attempt_count)
        if task.max_retries > 0
        else None
    )
    view = {
        "task_id": task.task_id,
        "idempotency_key": task.idempotency_key,
        "state": task.state.value,
        "failure_reason": (
            task.failure_reason.value if task.failure_reason else None
        ),
        "result": task.result,
        "version": task.version,
        "attempt_count": task.attempt_count,
        "retries_remaining": remaining,
        "next_run_at": task.next_run_at,
    }
    if duplicated is not None:
        view["duplicated"] = duplicated
    return view


def _attempt_view(attempt) -> dict:
    return {
        "attempt_no": attempt.attempt_no,
        "kind": attempt.kind.value,
        "started_at": attempt.started_at,
        "finished_at": attempt.finished_at,
        "result": attempt.result.value if attempt.result else None,
        "failure_reason": (
            attempt.failure_reason.value if attempt.failure_reason else None
        ),
        "next_run_at": attempt.next_run_at,
    }


def default_handler(payload: dict) -> object:
    """默认处理器：回显 payload。生产中由装配处替换。"""
    return {"echo": payload}


def create_app(
    settings: Settings | None = None,
    *,
    handler: Handler | None = None,
) -> FastAPI:
    """构建应用。默认从环境变量读取配置，测试时可注入。"""
    configure_logging()
    settings = settings or settings_from_env()
    store = TaskStore(settings.db_path)
    service = TaskService(store, settings)
    executor = Executor(store, settings, handler or default_handler)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # 中断恢复：上次 RUNNING 的任务——被打断的一次计为一次尝试，
        # 按重试策略决定 WAITING_RETRY（到期接着跑）或 FAILED/INTERRUPTED；
        # 仍 PENDING 的任务重新派发；WAITING_RETRY 由调度线程自动续跑。
        interrupted = store.recover_interrupted(executor.retry_decider)
        redispatched = executor.dispatch_pending()
        logger.info(
            "startup recovery: resolved_running=%s redispatched_pending=%s",
            [
                {"task": t.task_id, "state": t.state.value,
                 "next_run_at": t.next_run_at}
                for t in interrupted
            ],
            redispatched,
        )
        yield
        executor.shutdown(wait=True)
        store.close()

    app = FastAPI(title="async-task-pipeline", lifespan=lifespan)
    app.state.store = store
    app.state.service = service
    app.state.executor = executor

    @app.middleware("http")
    async def assign_request_id(request: Request, call_next):
        rid = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        logger.info("request=%s %s %s 收到请求", rid, request.method,
                    request.url.path)
        response = await call_next(request)
        response.headers["X-Request-ID"] = rid
        return response

    @app.get("/")
    def root() -> dict:
        return {"service": "async-task-pipeline", "status": "ok"}

    @app.post("/tasks", status_code=200)
    def submit(body: SubmitBody, request: Request) -> dict:
        rid = body.request_id or request.headers.get("X-Request-ID")
        policy = None
        if body.retry is not None:
            policy = RetryPolicy(
                max_retries=body.retry.max_retries,
                interval=body.retry.interval,
                backoff=body.retry.backoff,
            )
        task, duplicated = service.submit(
            body.idempotency_key, body.payload,
            request_id=rid, retry_policy=policy,
        )
        if not duplicated:
            executor.dispatch(task)
        return _task_view(task, duplicated=duplicated)

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str):
        task = service.get(task_id)
        if task is None:
            return JSONResponse(
                status_code=404,
                content={"error": {
                    "reason": RejectReason.TASK_NOT_FOUND.value,
                    "message": f"task {task_id} not found",
                }},
            )
        return _task_view(task)

    @app.get("/tasks/{task_id}/attempts")
    def get_task_attempts(task_id: str):
        try:
            attempts = service.get_attempts(task_id)
        except ServiceRejected as exc:
            return JSONResponse(
                status_code=404,
                content={"error": {
                    "reason": exc.reason.value, "message": str(exc),
                }},
            )
        return {"task_id": task_id, "attempts": [_attempt_view(a) for a in attempts]}

    @app.post("/tasks/{task_id}/cancel")
    def cancel(task_id: str, request: Request):
        rid = request.headers.get("X-Request-ID")
        task = service.cancel(task_id, request_id=rid)
        return _task_view(task)

    @app.get("/tasks")
    def list_tasks() -> dict:
        return {"tasks": [_task_view(t) for t in store.list_all()]}

    # 统一错误映射：拒绝原因结构化、可区分，不泄露内部堆栈
    @app.exception_handler(ServiceRejected)
    async def service_rejected_handler(request: Request, exc: ServiceRejected):
        status = 503 if exc.reason is RejectReason.OVER_CAPACITY else 409
        if exc.reason is RejectReason.TASK_NOT_FOUND:
            status = 404
        return JSONResponse(
            status_code=status,
            content={"error": {"reason": exc.reason.value, "message": str(exc)}},
        )

    @app.exception_handler(TransitionError)
    async def transition_error_handler(request: Request, exc: TransitionError):
        return JSONResponse(
            status_code=409,
            content={"error": {"reason": exc.reason.value, "message": str(exc)}},
        )

    return app
