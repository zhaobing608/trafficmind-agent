"""Small structured-runtime logging boundary for operations-critical events."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict

from backend.workflow.action_execution import sanitize_public_text, sanitize_public_value


_LOGGER = logging.getLogger("trafficmind.runtime")
_IDENTITY_FIELDS = {
    "eventId", "workflowRunId", "actionExecutionId",
    "component", "operation", "status",
}
_FORBIDDEN_LOG_FRAGMENTS = (
    "chain_of_thought", "chainofthought", "hidden_reasoning",
    "hiddenreasoning", "inner_monologue", "innermonologue",
    "reasoning_trace", "reasoningtrace", "system_prompt", "systemprompt",
    "raw_llm", "rawllm", "raw_provider", "rawprovider",
    "prompt", "provider_response", "providerresponse",
    "raw_response", "rawresponse", "response_body", "responsebody",
)


def _forbidden_log_key(key: Any) -> bool:
    normalized = str(key).lower().replace("-", "_").replace(" ", "")
    return any(fragment in normalized for fragment in _FORBIDDEN_LOG_FRAGMENTS)


def _bounded(value: Any, *, depth: int = 0) -> Any:
    """Keep one malformed provider message from creating an unbounded log."""
    if depth > 6:
        return "[TRUNCATED]"
    if isinstance(value, dict):
        result: Dict[str, Any] = {}
        for key, nested in list(value.items())[:50]:
            if _forbidden_log_key(key):
                continue
            result[str(key)] = _bounded(nested, depth=depth + 1)
        return result
    if isinstance(value, (list, tuple)):
        return [_bounded(item, depth=depth + 1) for item in list(value)[:50]]
    if isinstance(value, str):
        return value[:2000]
    return value


def runtime_log_payload(
    *,
    component: str,
    operation: str,
    status: str,
    event_id: str = "",
    workflow_run_id: str = "",
    action_execution_id: str = "",
    **fields: Any,
) -> Dict[str, Any]:
    """Build a bounded, secret-redacted structured log payload."""
    safe_fields = sanitize_public_value(fields)
    payload: Dict[str, Any] = (
        {
            key: _bounded(value)
            for key, value in safe_fields.items()
            if key not in _IDENTITY_FIELDS and not _forbidden_log_key(key)
        }
        if isinstance(safe_fields, dict)
        else {}
    )
    # Reserved identity fields are written last so arbitrary metadata cannot
    # replace the correlation contract.
    payload.update({
        "eventId": sanitize_public_text(event_id) or None,
        "workflowRunId": sanitize_public_text(workflow_run_id) or None,
        "actionExecutionId": sanitize_public_text(action_execution_id) or None,
        "component": sanitize_public_text(component),
        "operation": sanitize_public_text(operation),
        "status": sanitize_public_text(status),
    })
    return payload


def log_runtime_event(
    *,
    component: str,
    operation: str,
    status: str,
    event_id: str = "",
    workflow_run_id: str = "",
    action_execution_id: str = "",
    level: int = logging.INFO,
    logger: logging.Logger | None = None,
    **fields: Any,
) -> Dict[str, Any]:
    """Emit one JSON log line and return the exact safe payload for tests."""
    payload = runtime_log_payload(
        component=component,
        operation=operation,
        status=status,
        event_id=event_id,
        workflow_run_id=workflow_run_id,
        action_execution_id=action_execution_id,
        **fields,
    )
    (logger or _LOGGER).log(
        level,
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
    )
    return payload
