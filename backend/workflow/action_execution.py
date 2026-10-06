"""Reliable action executor contracts and the first production side effects.

Phase 21.3 deliberately keeps execution in the single-node SQLite runtime.  An
executor validates business input, performs one idempotency-keyed side effect,
and optionally reconciles an UNKNOWN result.  Credentials are resolved only by
providers from server configuration and never enter this module's persisted
request metadata.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Protocol

from backend.workflow.models import ActionStatus


_SENSITIVE_KEY_FRAGMENTS = (
    "authorization", "credential", "password", "passwd", "secret",
    "token", "api_key", "apikey", "cookie", "webhook_url", "webhookurl",
)


def contains_sensitive_key(value: Any) -> bool:
    """Reject client/plan supplied credentials before any durable write."""
    if isinstance(value, dict):
        for key, nested in value.items():
            normalized = str(key).lower().replace("-", "_")
            if any(fragment in normalized for fragment in _SENSITIVE_KEY_FRAGMENTS):
                return True
            if contains_sensitive_key(nested):
                return True
    elif isinstance(value, list):
        return any(contains_sensitive_key(item) for item in value)
    return False


def sanitize_public_text(value: Any) -> str:
    """Redact credential material from provider messages and string values."""
    text = str(value or "")
    if not text:
        return ""
    # Providers resolve credentials from environment.  If a buggy adapter
    # includes one in an exception/body, remove the exact configured value.
    for env_name, env_value in os.environ.items():
        normalized = env_name.lower().replace("-", "_")
        if (
            env_value
            and len(env_value) >= 4
            and any(fragment in normalized for fragment in _SENSITIVE_KEY_FRAGMENTS)
        ):
            text = text.replace(env_value, "[REDACTED]")
    text = re.sub(
        r"(?i)\b(authorization|credential|password|passwd|secret|token|api[-_]?key)"
        r"\s*[:=]\s*(?:bearer\s+)?[^\s,;]+",
        "[REDACTED]",
        text,
    )
    text = re.sub(
        r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+",
        "Bearer [REDACTED]",
        text,
    )
    return text


def sanitize_public_value(value: Any) -> Any:
    """Recursively remove secret-shaped fields from DB/audit/frontend DTOs."""
    if isinstance(value, dict):
        clean: Dict[str, Any] = {}
        for key, nested in value.items():
            normalized = str(key).lower().replace("-", "_")
            if any(fragment in normalized for fragment in _SENSITIVE_KEY_FRAGMENTS):
                continue
            clean[str(key)] = sanitize_public_value(nested)
        return clean
    if isinstance(value, list):
        return [sanitize_public_value(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_public_value(item) for item in value]
    if isinstance(value, str):
        return sanitize_public_text(value)
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return str(value)


@dataclass(frozen=True)
class ActionExecutionContext:
    action_execution_id: str
    workflow_run_id: str
    node_id: str
    event_id: str
    action_type: str
    idempotency_key: str
    attempt: int
    params: Dict[str, Any]
    event: Dict[str, Any]
    risk: Dict[str, Any]
    repository: Any
    external_reference: str = ""


@dataclass
class ActionExecutorResult:
    status: ActionStatus
    message: str = ""
    external_reference: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    error: str = ""
    retryable: bool = False
    reconciliation_supported: bool = False
    reconciliation_message: str = ""

    def safe_metadata(self) -> Dict[str, Any]:
        value = sanitize_public_value(self.metadata)
        return value if isinstance(value, dict) else {}


class ActionExecutor(Protocol):
    action_type: str
    reconciliation_supported: bool

    def validate(self, context: ActionExecutionContext) -> Optional[str]: ...

    async def execute(self, context: ActionExecutionContext) -> ActionExecutorResult: ...

    async def reconcile(self, context: ActionExecutionContext) -> ActionExecutorResult: ...


class BaseActionExecutor:
    action_type = ""
    reconciliation_supported = False

    def validate(self, context: ActionExecutionContext) -> Optional[str]:
        return None

    async def reconcile(self, context: ActionExecutionContext) -> ActionExecutorResult:
        return ActionExecutorResult(
            status=ActionStatus.UNKNOWN,
            message="reconciliation unsupported",
            reconciliation_supported=False,
            reconciliation_message="reconciliation unsupported",
        )


_EVENT_STATUS_ORDER = ["待研判", "待派单", "处置中", "已处置", "待复盘", "已归档"]
_EVENT_TRANSITIONS = {
    "待研判": {"待派单"},
    "待派单": {"处置中"},
    "处置中": {"已处置", "待复盘"},
    "已处置": {"待复盘"},
    "待复盘": {"已归档"},
    "已归档": set(),
}


class UpdateEventStatusExecutor(BaseActionExecutor):
    action_type = "update_event_status"
    reconciliation_supported = True

    def validate(self, context: ActionExecutionContext) -> Optional[str]:
        target = str(context.params.get("status") or context.params.get("targetStatus") or "").strip()
        if target not in _EVENT_STATUS_ORDER:
            return "status 不是合法 Event 状态"
        if not context.event_id:
            return "缺少 canonical eventId"
        return None

    async def execute(self, context: ActionExecutionContext) -> ActionExecutorResult:
        from backend.tools.db_tools import advance_event_status, get_event_by_id

        target = str(context.params.get("status") or context.params.get("targetStatus") or "").strip()
        current = get_event_by_id(context.event_id)
        if current is None:
            return ActionExecutorResult(
                status=ActionStatus.FAILED,
                error="canonical Event 不存在",
                retryable=False,
                reconciliation_supported=True,
            )
        previous = str(current.get("status") or "")
        if previous == target:
            return ActionExecutorResult(
                status=ActionStatus.SUCCEEDED,
                message="Event 已处于目标状态，幂等确认",
                external_reference=f"event:{context.event_id}:{target}",
                metadata={"eventId": context.event_id, "previousStatus": previous, "status": target},
                reconciliation_supported=True,
            )
        if target not in _EVENT_TRANSITIONS.get(previous, set()):
            return ActionExecutorResult(
                status=ActionStatus.FAILED,
                error=f"非法 Event 状态转换: {previous} → {target}",
                retryable=False,
                reconciliation_supported=True,
            )
        updated = advance_event_status(context.event_id, target, allowed_from=[previous])
        if not updated:
            reread = get_event_by_id(context.event_id)
            if reread and reread.get("status") == target:
                updated = True
        if not updated:
            return ActionExecutorResult(
                status=ActionStatus.FAILED,
                error="Event 状态在执行期间发生变化",
                retryable=True,
                reconciliation_supported=True,
            )
        return ActionExecutorResult(
            status=ActionStatus.SUCCEEDED,
            message="Event 状态已更新",
            external_reference=f"event:{context.event_id}:{target}",
            metadata={"eventId": context.event_id, "previousStatus": previous, "status": target},
            reconciliation_supported=True,
        )

    async def reconcile(self, context: ActionExecutionContext) -> ActionExecutorResult:
        from backend.tools.db_tools import get_event_by_id

        target = str(context.params.get("status") or context.params.get("targetStatus") or "").strip()
        event = get_event_by_id(context.event_id)
        if event is None:
            return ActionExecutorResult(
                status=ActionStatus.FAILED,
                error="canonical Event 不存在",
                retryable=False,
                reconciliation_supported=True,
            )
        current = str(event.get("status") or "")
        if current == target or (
            current in _EVENT_STATUS_ORDER
            and target in _EVENT_STATUS_ORDER
            and _EVENT_STATUS_ORDER.index(current) > _EVENT_STATUS_ORDER.index(target)
        ):
            return ActionExecutorResult(
                status=ActionStatus.SUCCEEDED,
                message="已从 canonical Event 确认状态更新",
                external_reference=f"event:{context.event_id}:{target}",
                metadata={"eventId": context.event_id, "status": current},
                reconciliation_supported=True,
            )
        return ActionExecutorResult(
            status=ActionStatus.FAILED,
            message="已确认状态更新未发生",
            error="confirmed_not_executed",
            retryable=True,
            metadata={"eventId": context.event_id, "status": current},
            reconciliation_supported=True,
        )


class CreateDispatchTaskExecutor(BaseActionExecutor):
    action_type = "create_dispatch_task"
    reconciliation_supported = True

    def validate(self, context: ActionExecutionContext) -> Optional[str]:
        if not context.event_id:
            return "缺少 canonical eventId"
        instruction = str(context.params.get("instruction") or "").strip()
        if not instruction:
            return "instruction 不能为空"
        return None

    async def execute(self, context: ActionExecutionContext) -> ActionExecutorResult:
        task, created = context.repository.create_dispatch_task(
            event_id=context.event_id,
            run_id=context.workflow_run_id,
            action_execution_id=context.action_execution_id,
            idempotency_key=context.idempotency_key,
            assignee=str(context.params.get("assignee") or "").strip(),
            target=str(context.params.get("target") or "").strip(),
            instruction=str(context.params.get("instruction") or "").strip(),
        )
        task_id = str(task.get("dispatch_task_id") or "")
        return ActionExecutorResult(
            status=ActionStatus.SUCCEEDED,
            message="处置任务已创建" if created else "处置任务已存在，幂等复用",
            external_reference=task_id,
            metadata={
                "dispatchTaskId": task_id,
                "eventId": context.event_id,
                "status": task.get("status") or "created",
                "created": bool(created),
            },
            reconciliation_supported=True,
        )

    async def reconcile(self, context: ActionExecutionContext) -> ActionExecutorResult:
        task = context.repository.get_dispatch_task_by_idempotency_key(context.idempotency_key)
        if task is None:
            return ActionExecutorResult(
                status=ActionStatus.FAILED,
                message="已确认处置任务未创建",
                error="confirmed_not_executed",
                retryable=True,
                reconciliation_supported=True,
            )
        task_id = str(task.get("dispatch_task_id") or "")
        return ActionExecutorResult(
            status=ActionStatus.SUCCEEDED,
            message="已确认处置任务存在",
            external_reference=task_id,
            metadata={"dispatchTaskId": task_id, "status": task.get("status") or "created"},
            reconciliation_supported=True,
        )


@dataclass(frozen=True)
class NotificationRequest:
    idempotency_key: str
    target: str
    channel: str
    message: str


class NotificationProvider(Protocol):
    name: str
    reconciliation_supported: bool

    async def send(self, request: NotificationRequest, repository: Any) -> ActionExecutorResult: ...

    async def reconcile(
        self,
        request: NotificationRequest,
        repository: Any,
        external_reference: str,
    ) -> ActionExecutorResult: ...


class LocalNotificationProvider:
    """Controlled durable provider; never contacts a real external system."""

    name = "local-controlled"
    reconciliation_supported = True

    def __init__(self, outcome: str = ""):
        self._outcome = outcome

    async def send(self, request: NotificationRequest, repository: Any) -> ActionExecutorResult:
        outcome = self._outcome
        if outcome == "failed":
            return ActionExecutorResult(
                status=ActionStatus.FAILED,
                error="controlled provider rejected request before delivery",
                retryable=True,
                reconciliation_supported=True,
            )
        if outcome == "timeout_before_delivery":
            return ActionExecutorResult(
                status=ActionStatus.UNKNOWN,
                message="provider response timed out before delivery could be confirmed",
                reconciliation_supported=True,
                reconciliation_message="可通过 idempotency key 查询",
            )

        digest = hashlib.sha256(request.message.encode("utf-8")).hexdigest()
        receipt_id = f"local-notify-{request.idempotency_key}"
        receipt, _ = repository.record_local_notification_receipt(
            receipt_id=receipt_id,
            idempotency_key=request.idempotency_key,
            channel=request.channel,
            target=request.target,
            message_digest=digest,
        )
        if outcome == "timeout_after_delivery":
            return ActionExecutorResult(
                status=ActionStatus.UNKNOWN,
                message="provider accepted the request but acknowledgement timed out",
                external_reference=str(receipt.get("receipt_id") or receipt_id),
                reconciliation_supported=True,
                reconciliation_message="可通过 receipt/idempotency key 查询",
            )
        return ActionExecutorResult(
            status=ActionStatus.SUCCEEDED,
            message="controlled notification delivered",
            external_reference=str(receipt.get("receipt_id") or receipt_id),
            metadata={"channel": request.channel, "target": request.target, "delivered": True},
            reconciliation_supported=True,
        )

    async def reconcile(
        self,
        request: NotificationRequest,
        repository: Any,
        external_reference: str,
    ) -> ActionExecutorResult:
        receipt = repository.get_local_notification_receipt(
            external_reference=external_reference,
            idempotency_key=request.idempotency_key,
        )
        if receipt is None:
            return ActionExecutorResult(
                status=ActionStatus.FAILED,
                message="provider confirmed notification was not delivered",
                error="confirmed_not_executed",
                retryable=True,
                reconciliation_supported=True,
            )
        return ActionExecutorResult(
            status=ActionStatus.SUCCEEDED,
            message="provider confirmed notification delivery",
            external_reference=str(receipt.get("receipt_id") or external_reference),
            metadata={
                "channel": receipt.get("channel") or request.channel,
                "target": receipt.get("target") or request.target,
                "delivered": True,
                "deliveredAt": receipt.get("delivered_at") or "",
            },
            reconciliation_supported=True,
        )


class WebhookNotificationProvider:
    """Opt-in HTTP provider with server-only credentials and idempotency header."""

    name = "http-webhook"

    @property
    def reconciliation_supported(self) -> bool:
        return bool(os.getenv("TRAFFICMIND_NOTIFICATION_RECONCILE_URL", "").strip())

    def _headers(self, idempotency_key: str) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key,
        }
        token = os.getenv("TRAFFICMIND_NOTIFICATION_TOKEN", "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def send(self, request: NotificationRequest, repository: Any) -> ActionExecutorResult:
        url = os.getenv("TRAFFICMIND_NOTIFICATION_WEBHOOK_URL", "").strip()
        if not url:
            return ActionExecutorResult(
                status=ActionStatus.FAILED,
                error="notification webhook provider is not configured",
                retryable=False,
                reconciliation_supported=self.reconciliation_supported,
            )

        def _post() -> ActionExecutorResult:
            body = json.dumps({
                "target": request.target,
                "channel": request.channel,
                "message": request.message,
            }, ensure_ascii=False).encode("utf-8")
            req = urllib.request.Request(url, data=body, headers=self._headers(request.idempotency_key))
            try:
                with urllib.request.urlopen(req, timeout=float(os.getenv("TRAFFICMIND_NOTIFICATION_TIMEOUT", "10"))) as response:
                    raw = response.read().decode("utf-8", errors="replace")
                    parsed: Dict[str, Any] = {}
                    try:
                        value = json.loads(raw) if raw else {}
                        parsed = value if isinstance(value, dict) else {}
                    except json.JSONDecodeError:
                        parsed = {}
                    external = str(
                        response.headers.get("X-External-Reference")
                        or parsed.get("externalReference")
                        or ""
                    )
                    return ActionExecutorResult(
                        status=ActionStatus.SUCCEEDED,
                        message="notification provider accepted request",
                        external_reference=external,
                        metadata={"providerStatus": response.status, "accepted": True},
                        reconciliation_supported=self.reconciliation_supported,
                    )
            except urllib.error.HTTPError as exc:
                if exc.code in {408, 409, 425, 429, 500, 502, 503, 504}:
                    return ActionExecutorResult(
                        status=ActionStatus.UNKNOWN,
                        message=f"provider returned uncertain HTTP {exc.code}",
                        reconciliation_supported=self.reconciliation_supported,
                        reconciliation_message=(
                            "provider query endpoint available"
                            if self.reconciliation_supported
                            else "reconciliation unsupported"
                        ),
                    )
                return ActionExecutorResult(
                    status=ActionStatus.FAILED,
                    error=f"provider rejected request with HTTP {exc.code}",
                    retryable=True,
                    reconciliation_supported=self.reconciliation_supported,
                )
            except (TimeoutError, socket.timeout, urllib.error.URLError) as exc:
                return ActionExecutorResult(
                    status=ActionStatus.UNKNOWN,
                    message=f"notification outcome unknown: {type(exc).__name__}",
                    reconciliation_supported=self.reconciliation_supported,
                    reconciliation_message=(
                        "provider query endpoint available"
                        if self.reconciliation_supported
                        else "reconciliation unsupported"
                    ),
                )

        return await asyncio.to_thread(_post)

    async def reconcile(
        self,
        request: NotificationRequest,
        repository: Any,
        external_reference: str,
    ) -> ActionExecutorResult:
        url = os.getenv("TRAFFICMIND_NOTIFICATION_RECONCILE_URL", "").strip()
        if not url:
            return await super_reconcile_unsupported()

        def _get() -> ActionExecutorResult:
            query_identity = external_reference or request.idempotency_key
            separator = "&" if "?" in url else "?"
            req = urllib.request.Request(
                f"{url}{separator}identity={urllib.parse.quote(query_identity)}",
                headers=self._headers(request.idempotency_key),
            )
            try:
                with urllib.request.urlopen(req, timeout=float(os.getenv("TRAFFICMIND_NOTIFICATION_TIMEOUT", "10"))) as response:
                    raw = json.loads(response.read().decode("utf-8") or "{}")
                    status = str(raw.get("status") or "unknown").lower()
                    external = str(raw.get("externalReference") or external_reference)
                    if status in {"succeeded", "delivered", "accepted"}:
                        return ActionExecutorResult(
                            status=ActionStatus.SUCCEEDED,
                            message="provider confirmed delivery",
                            external_reference=external,
                            metadata={"delivered": True},
                            reconciliation_supported=True,
                        )
                    if status in {"not_executed", "confirmed_not_executed"}:
                        return ActionExecutorResult(
                            status=ActionStatus.FAILED,
                            message="provider confirmed request was not executed",
                            error="confirmed_not_executed",
                            retryable=True,
                            reconciliation_supported=True,
                        )
                    # ``not_found`` and generic ``failed`` are not proof that
                    # no side effect occurred: many provider query indexes are
                    # eventually consistent, and a failure may happen after
                    # acceptance.  Keep the execution fenced as UNKNOWN.
                    return ActionExecutorResult(
                        status=ActionStatus.UNKNOWN,
                        message="provider still cannot determine outcome",
                        external_reference=external,
                        reconciliation_supported=True,
                    )
            except Exception as exc:
                return ActionExecutorResult(
                    status=ActionStatus.UNKNOWN,
                    message=f"reconciliation query failed: {type(exc).__name__}",
                    external_reference=external_reference,
                    reconciliation_supported=True,
                )

        return await asyncio.to_thread(_get)


async def super_reconcile_unsupported() -> ActionExecutorResult:
    return ActionExecutorResult(
        status=ActionStatus.UNKNOWN,
        message="reconciliation unsupported",
        reconciliation_supported=False,
        reconciliation_message="reconciliation unsupported",
    )


_notification_provider: Optional[NotificationProvider] = None


def get_notification_provider() -> NotificationProvider:
    global _notification_provider
    if _notification_provider is None:
        provider = os.getenv("TRAFFICMIND_NOTIFICATION_PROVIDER", "local").strip().lower()
        _notification_provider = (
            WebhookNotificationProvider() if provider in {"http", "webhook"}
            else LocalNotificationProvider()
        )
    return _notification_provider


def set_notification_provider(provider: NotificationProvider) -> None:
    global _notification_provider
    _notification_provider = provider


def reset_notification_provider() -> None:
    global _notification_provider
    _notification_provider = None


class SendNotificationExecutor(BaseActionExecutor):
    action_type = "send_notification"

    @property
    def reconciliation_supported(self) -> bool:
        return bool(get_notification_provider().reconciliation_supported)

    def validate(self, context: ActionExecutionContext) -> Optional[str]:
        if not str(context.params.get("message") or "").strip():
            return "message 不能为空"
        if not str(context.params.get("target") or "").strip():
            return "target 不能为空"
        return None

    def _request(self, context: ActionExecutionContext) -> NotificationRequest:
        return NotificationRequest(
            idempotency_key=context.idempotency_key,
            target=str(context.params.get("target") or "").strip(),
            channel=str(context.params.get("channel") or "local").strip() or "local",
            message=str(context.params.get("message") or "").strip(),
        )

    async def execute(self, context: ActionExecutionContext) -> ActionExecutorResult:
        return await get_notification_provider().send(self._request(context), context.repository)

    async def reconcile(self, context: ActionExecutionContext) -> ActionExecutorResult:
        provider = get_notification_provider()
        if not provider.reconciliation_supported:
            return await super_reconcile_unsupported()
        return await provider.reconcile(
            self._request(context),
            context.repository,
            context.external_reference,
        )


class ActionExecutorRegistry:
    def __init__(self):
        self._executors: Dict[str, ActionExecutor] = {}
        self.register(UpdateEventStatusExecutor())
        self.register(CreateDispatchTaskExecutor())
        self.register(SendNotificationExecutor())

    @staticmethod
    def canonical(action_type: str) -> str:
        return str(action_type or "").strip().lower()

    def register(self, executor: ActionExecutor) -> None:
        self._executors[self.canonical(executor.action_type)] = executor

    def get(self, action_type: str) -> Optional[ActionExecutor]:
        return self._executors.get(self.canonical(action_type))

    def has(self, action_type: str) -> bool:
        return self.get(action_type) is not None


_executor_registry: Optional[ActionExecutorRegistry] = None


def get_action_executor_registry() -> ActionExecutorRegistry:
    global _executor_registry
    if _executor_registry is None:
        _executor_registry = ActionExecutorRegistry()
    return _executor_registry


def _definition_blocks_actions(repository: Any, run: Any) -> bool:
    if not getattr(run, "definition_id", ""):
        return False
    version = repository.get_definition_version(
        run.definition_id,
        int(getattr(run, "version", 1) or 1),
    )
    if version is not None:
        from backend.workflow.models import WorkflowDefinition
        definition = WorkflowDefinition.from_dict(version.definition_json)
    else:
        definition = repository.get_definition(run.definition_id)
    if definition is None:
        return True
    metadata = definition.metadata if isinstance(definition.metadata, dict) else {}
    plan = metadata.get("plan") if isinstance(metadata.get("plan"), dict) else {}
    plan_metadata = (
        plan.get("metadata") if isinstance(plan.get("metadata"), dict) else {}
    )
    return bool(
        metadata.get("actionExecutionAllowed") is False
        or metadata.get("runKind") == "replay"
        or plan.get("actionExecutionAllowed") is False
        or plan.get("runKind") == "replay"
        or plan_metadata.get("actionExecutionAllowed") is False
        or plan_metadata.get("runKind") == "replay"
    )


def _context_from_record(repository: Any, run: Any, record: Any) -> ActionExecutionContext:
    state = run.state if isinstance(run.state, dict) else {}
    event = state.get("currentEvent") if isinstance(state.get("currentEvent"), dict) else {}
    risk = state.get("riskAssessment") if isinstance(state.get("riskAssessment"), dict) else {}
    return ActionExecutionContext(
        action_execution_id=record.action_id,
        workflow_run_id=record.run_id,
        node_id=record.node_id,
        event_id=record.event_id,
        action_type=record.action_type,
        idempotency_key=record.idempotency_key,
        attempt=int(record.attempt or 0),
        params=dict(record.params or {}),
        event=dict(event or {}),
        risk=dict(risk or {}),
        repository=repository,
        external_reference=record.external_reference,
    )


async def reconcile_action_execution(
    repository: Any,
    run_id: str,
    action_execution_id: str,
) -> Dict[str, Any]:
    """Query an executor/provider for an UNKNOWN outcome; never calls execute."""
    record = repository.get_action_record(action_execution_id)
    if record is None or record.run_id != run_id:
        return {"errorCode": "not_found", "error": "Action Execution 不存在"}
    run = repository.get_run(run_id)
    if run is None:
        return {"errorCode": "not_found", "error": "Workflow Run 不存在"}
    if run.status.value not in {"paused", "running", "failed", "cancelled"}:
        return {
            "errorCode": "invalid_status",
            "error": f"Run 状态为 {run.status.value}，当前不允许 reconciliation",
        }
    if record.status != ActionStatus.UNKNOWN:
        return {
            "errorCode": "invalid_status",
            "error": f"Action 状态为 {record.status.value}，仅 unknown 可 reconciliation",
        }
    if _definition_blocks_actions(repository, run):
        return {"errorCode": "replay_execution_blocked", "error": "Replay-derived Action 禁止操作"}
    state = run.state if isinstance(run.state, dict) else {}
    event = state.get("currentEvent") if isinstance(state.get("currentEvent"), dict) else {}
    if (
        not record.event_id
        or str(event.get("eventId") or "") != record.event_id
        or event.get("actionExecutionAllowed") is False
    ):
        return {"errorCode": "identity_mismatch", "error": "Action/run/event 身份不一致"}

    executor = get_action_executor_registry().get(record.action_type)
    if executor is None:
        outcome = ActionExecutorResult(
            status=ActionStatus.UNKNOWN,
            message="reconciliation unsupported",
            reconciliation_supported=False,
            reconciliation_message="reconciliation unsupported",
        )
    else:
        try:
            outcome = await executor.reconcile(_context_from_record(repository, run, record))
        except Exception as exc:
            outcome = ActionExecutorResult(
                status=ActionStatus.UNKNOWN,
                message=f"reconciliation failed: {type(exc).__name__}",
                external_reference=record.external_reference,
                reconciliation_supported=bool(executor.reconciliation_supported),
            )
    safe_result = outcome.safe_metadata()
    if outcome.message:
        safe_result["message"] = sanitize_public_text(outcome.message)
    applied = repository.apply_action_reconciliation(
        action_execution_id,
        status=outcome.status,
        result=safe_result,
        error=sanitize_public_text(outcome.error),
        external_reference=sanitize_public_text(
            outcome.external_reference or record.external_reference
        ),
        supported=bool(outcome.reconciliation_supported),
        retryable=bool(outcome.retryable),
        message=sanitize_public_text(
            outcome.reconciliation_message or outcome.message
        ),
    )
    if not applied.get("updated"):
        reason = applied.get("reason") or "concurrent_change"
        code = "not_found" if reason in {"not_found", "run_not_found"} else "invalid_status"
        return {"errorCode": code, "error": f"reconciliation 未提交: {reason}"}
    try:
        from backend.observability.logging import log_runtime_event
        log_runtime_event(
            component="workflow.action",
            operation="action_reconciliation_completed",
            status=outcome.status.value,
            event_id=record.event_id,
            workflow_run_id=run_id,
            action_execution_id=action_execution_id,
            actionType=record.action_type,
            attempt=int(record.attempt or 0),
            reconciliationSupported=bool(outcome.reconciliation_supported),
            error=sanitize_public_text(outcome.error) or None,
        )
    except Exception:
        pass
    if not outcome.reconciliation_supported:
        return {
            **applied,
            "errorCode": "reconciliation_unsupported",
            "error": "reconciliation unsupported",
            "status": ActionStatus.UNKNOWN.value,
        }
    return {
        **applied,
        "message": sanitize_public_text(outcome.message),
        "externalReference": sanitize_public_text(
            outcome.external_reference or record.external_reference
        ) or None,
        "retryable": bool(outcome.retryable),
    }


def request_action_execution_retry(
    repository: Any,
    run_id: str,
    action_execution_id: str,
) -> Dict[str, Any]:
    """Schedule, but do not inline execute, a known-safe FAILED action retry."""
    record = repository.get_action_record(action_execution_id)
    if record is None or record.run_id != run_id:
        return {"errorCode": "not_found", "error": "Action Execution 不存在"}
    run = repository.get_run(run_id)
    if run is None:
        return {"errorCode": "not_found", "error": "Workflow Run 不存在"}
    if run.status.value == "cancelled":
        return {"errorCode": "invalid_status", "error": "已取消 Run 不允许 retry"}
    if _definition_blocks_actions(repository, run):
        return {"errorCode": "replay_execution_blocked", "error": "Replay-derived Action 禁止 retry"}
    if record.status == ActionStatus.UNKNOWN:
        return {
            "errorCode": "reconcile_required",
            "error": "UNKNOWN Action 禁止直接 retry；必须先 reconciliation",
        }
    if record.status != ActionStatus.FAILED or not record.retryable:
        return {
            "errorCode": "invalid_status",
            "error": f"Action 状态为 {record.status.value}，当前不可 retry",
        }
    result = repository.request_action_retry(action_execution_id)
    if not result.get("updated"):
        reason = result.get("reason") or "concurrent_change"
        code = "not_found" if reason in {"not_found", "run_not_found"} else "invalid_status"
        return {"errorCode": code, "error": f"Action retry 未调度: {reason}"}
    return {**result, "status": "pending", "scheduled": True}
