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
    BackoffStrategy,
    RejectReason,
    RetryPolicy,
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
    """提交时携带的重试策略；省略或 max_retries=0 表示不重试。"""

    max_retries: int = Field(default=0, ge=0)
    base_delay: float = Field(default=1.0, ge=0)
    strategy: BackoffStrategy = BackoffStrategy.FIXED
    factor: float = Field(default=2.0, ge=1)
    max_delay: float = Field(default=60.0, ge=0)
    retry_on_interruption: bool = True
    count_interruption_as_attempt: bool = True


class SubmitBody(BaseModel):
    idempotency_key: str = Field(..., min_length=1)
    payload: dict = Field(default_factory=dict)
    request_id: str | None = None
    retry: RetryBody | None = None


def _retry_policy_from_body(body: RetryBody | None) -> RetryPolicy | None:
    if body is None or body.max_retries == 0:
        # 不带策略或显式 0 次：保持旧行为，不记录重试策略
        return None
    return RetryPolicy(
        max_retries=body.max_retries,
        base_delay=body.base_delay,
        strategy=body.strategy,
        factor=body.factor,
        max_delay=body.max_delay,
        retry_on_interruption=body.retry_on_interruption,
        count_interruption_as_attempt=body.count_interruption_as_attempt,
    )


class TaskView(BaseModel):
    task_id: str
    idempotency_key: str
    state: str
    failure_reason: str | None
    result: object | None
    version: int
    attempt_no: int
    remaining_retries: int
    next_run_at: float | None


def _attempt_view(att) -> dict:
    return {
        "attempt_no": att.attempt_no,
        "status": att.status.value,
        "is_retry": att.is_retry,
        "failure_reason": (
            att.failure_reason.value if att.failure_reason else None
        ),
        "counted": att.counted,
        "started_at": att.started_at,
        "finished_at": att.finished_at,
        "next_run_at": att.next_run_at,
    }


def _task_view(task, duplicated: bool | None = None) -> dict:
    view = {
        "task_id": task.task_id,
        "idempotency_key": task.idempotency_key,
        "state": task.state.value,
        "failure_reason": (
            task.failure_reason.value if task.failure_reason else None
        ),
        "result": task.result,
        "version": task.version,
        # 一眼看到当前第几次、还剩几次、下次重试时间
        "attempt_no": task.attempt_no,
        "remaining_retries": task.remaining_retries,
        "next_run_at": task.next_run_at,
    }
    if duplicated is not None:
        view["duplicated"] = duplicated
    return view


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
        # 中断恢复：上次 RUNNING 的未闭合尝试按策略判定为最终失败或
        # 转入等待重试；RETRY_WAITING 的任务重新挂定时器；
        # 仍 PENDING 的任务重新派发执行。
        interrupted = store.recover_interrupted()
        waiting = executor.schedule_retry_waiting()
        redispatched = executor.dispatch_pending()
        logger.info(
            "startup recovery: interrupted_decided=%s retry_waiting=%s "
            "redispatched_pending=%s",
            [t.task_id for t in interrupted], waiting, redispatched,
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
        policy = _retry_policy_from_body(body.retry)
        task, duplicated = service.submit(
            body.idempotency_key, body.payload,
            retry_policy=policy, request_id=rid,
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
        """逐次尝试追溯：第几次、何时跑、结果/失败分类、首次还是重试、
        下一次预计开始时间。"""
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
