"""Production runtime trace, audit, metrics, and operational-alert services.

This module is intentionally SQLite-native.  It projects existing durable
facts instead of creating a second execution history and never exposes raw
prompts, provider responses, credentials, or private model reasoning.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import backend.config as _config
from backend.observability.logging import log_runtime_event
from backend.tools.db_tools import get_event_by_id, init_db
from backend.workflow.action_execution import sanitize_public_text, sanitize_public_value
from backend.workflow.repository import SQLiteWorkflowRepository, init_workflow_tables


ACTIVE_ALERT_STATUS = "active"
RESOLVED_ALERT_STATUS = "resolved"
ALERT_TYPES = {
    "ACTION_UNKNOWN_TOO_LONG",
    "WORKFLOW_STUCK",
    "APPROVAL_WAITING_TOO_LONG",
    "ACTION_REPEATED_FAILURE",
}
_RESOLVED_EVENT_STATUSES = {"已处置", "待复盘", "已归档"}
_FORBIDDEN_TRACE_FRAGMENTS = (
    "authorization", "credential", "password", "passwd", "secret",
    "api_key", "apikey", "cookie", "webhook_url", "webhookurl",
    "chain_of_thought", "chainofthought", "hidden_reasoning",
    "hiddenreasoning", "inner_monologue", "innermonologue",
    "reasoning_trace", "reasoningtrace", "system_prompt", "systemprompt",
    "raw_llm", "rawllm", "raw_provider", "rawprovider",
    "prompt", "provider_response", "providerresponse",
    "raw_response", "rawresponse", "response_body", "responsebody",
)


class TraceIntegrityError(RuntimeError):
    """Persisted correlation identities disagree; never guess across Events."""



def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_time(
    value: Any,
    *,
    naive_timezone: Optional[Any] = None,
) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=naive_timezone or timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def _legacy_local_timezone() -> Any:
    """Timezone used by pre-21.4 collaboration rows with naive timestamps."""
    return datetime.now().astimezone().tzinfo or timezone.utc


def _normalized_time(value: Any, *, legacy_local: bool = False) -> Optional[str]:
    parsed = _parse_time(
        value,
        naive_timezone=_legacy_local_timezone() if legacy_local else timezone.utc,
    )
    return _iso(parsed) if parsed is not None else None


def _age_seconds(value: Any, now: datetime) -> Optional[int]:
    parsed = _parse_time(value)
    if parsed is None:
        return None
    return max(0, int((now - parsed).total_seconds()))


def _json(value: Any, default: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value) if value else default
        except (TypeError, json.JSONDecodeError):
            return default
    return default if value is None else value


def _connection() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(_config.DB_PATH), exist_ok=True)
    conn = sqlite3.connect(_config.DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


@dataclass(frozen=True)
class OperationsConfig:
    scan_interval: float
    unknown_alert_after: float
    workflow_stuck_after: float
    approval_attention_after: float
    approval_overdue_after: float
    repeated_failure_threshold: int

    @classmethod
    def current(cls) -> "OperationsConfig":
        return cls(
            scan_interval=max(1.0, float(_config.OPERATIONS_SCAN_INTERVAL)),
            unknown_alert_after=max(0.0, float(_config.UNKNOWN_ALERT_AFTER)),
            workflow_stuck_after=max(0.0, float(_config.WORKFLOW_STUCK_AFTER)),
            approval_attention_after=max(0.0, float(_config.APPROVAL_ATTENTION_AFTER)),
            approval_overdue_after=max(0.0, float(_config.APPROVAL_OVERDUE_AFTER)),
            repeated_failure_threshold=max(
                1, int(_config.ACTION_REPEATED_FAILURE_THRESHOLD)
            ),
        )


def init_operations_tables() -> None:
    """Create only the durable alert projection and its query indexes."""
    init_db()
    init_workflow_tables()
    from backend.agent.collaboration.db_repository import init_collaboration_tables
    init_collaboration_tables()
    conn = _connection()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS operational_alerts (
                alert_id TEXT PRIMARY KEY,
                event_id TEXT DEFAULT '',
                workflow_run_id TEXT DEFAULT '',
                action_execution_id TEXT DEFAULT '',
                approval_id TEXT DEFAULT '',
                resource_type TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                resolved_at TEXT DEFAULT '',
                occurrence_count INTEGER NOT NULL DEFAULT 1,
                message TEXT NOT NULL DEFAULT ''
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_operational_alert_active_identity
                ON operational_alerts(alert_type, resource_type, resource_id)
                WHERE status = 'active';
            CREATE INDEX IF NOT EXISTS idx_operational_alert_status_seen
                ON operational_alerts(status, last_seen_at DESC);
            CREATE INDEX IF NOT EXISTS idx_operational_alert_event
                ON operational_alerts(event_id, status);
            CREATE INDEX IF NOT EXISTS idx_operational_alert_workflow
                ON operational_alerts(workflow_run_id, status);
            CREATE INDEX IF NOT EXISTS idx_operational_alert_action
                ON operational_alerts(action_execution_id, status);
            """
        )
        conn.commit()
    finally:
        conn.close()


def _safe_trace(value: Any, *, depth: int = 0) -> Any:
    """Bound and redact a value before it crosses the observability API."""
    if depth > 8:
        return "[TRUNCATED]"
    if isinstance(value, dict):
        result: Dict[str, Any] = {}
        for key, nested in list(value.items())[:100]:
            normalized = str(key).lower().replace("-", "_").replace(" ", "")
            if any(fragment in normalized for fragment in _FORBIDDEN_TRACE_FRAGMENTS):
                continue
            result[str(key)] = _safe_trace(nested, depth=depth + 1)
        clean = sanitize_public_value(result)
        return clean if isinstance(clean, dict) else {}
    if isinstance(value, (list, tuple)):
        return [_safe_trace(item, depth=depth + 1) for item in list(value)[:100]]
    if isinstance(value, str):
        return sanitize_public_text(value)[:2000]
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return sanitize_public_text(value)[:500]


def _event_summary(record: Dict[str, Any]) -> Dict[str, Any]:
    allowed = (
        "eventId", "eventType", "eventTypeCn", "roadName", "direction",
        "riskScore", "riskLevel", "status", "source", "sourceEventId",
        "occurredAt", "receivedAt", "lastReceivedAt", "revision",
        "createdAt", "updatedAt",
    )
    return _safe_trace({key: record.get(key) for key in allowed})


def _grounding_summary(value: Any) -> Dict[str, Any]:
    context = _json(value, {})
    if not isinstance(context, dict) or not context:
        return {"status": "unavailable", "contextTypes": [], "evidenceRefs": []}
    try:
        from backend.grounding.rendering import grounding_audit_summary
        compact = grounding_audit_summary(context)
    except Exception:
        compact = {
            "groundingStatus": context.get("groundingStatus"),
            "assembledAt": context.get("assembledAt"),
            "refs": list(context.get("groundingRefs") or [])[:12],
        }
    types = [
        name
        for name, key in (
            ("regional", "regionalContext"),
            ("historical", "historicalContext"),
            ("knowledge", "knowledgeContext"),
            ("case_memory", "caseMemoryContext"),
        )
        if isinstance(context.get(key), dict)
    ]
    return _safe_trace({
        "status": compact.get("groundingStatus") or "unavailable",
        "assembledAt": compact.get("assembledAt") or None,
        "contextTypes": types,
        "regionId": compact.get("regionId"),
        "roadId": compact.get("roadId"),
        "intersectionId": compact.get("intersectionId"),
        "historicalEventCount": compact.get("historicalEventCount"),
        "historicalWindow": compact.get("historicalWindow") or {},
        "knowledgeEvidenceRefs": compact.get("knowledgeEvidenceRefs") or [],
        "caseMemoryRefs": compact.get("caseMemoryRefs") or [],
        "evidenceRefs": compact.get("refs") or [],
    })


def _usage_summary(value: Any) -> Dict[str, Any]:
    """Explicit numeric projection; generic token-key sanitizing stays strict."""
    usage = _json(value, {})
    if not isinstance(usage, dict):
        return {"available": False}
    agent_calls = usage.get("used_agent_calls") or usage.get("usedAgentCalls") or {}
    retries = usage.get("used_retries") or usage.get("usedRetries") or {}
    if not isinstance(agent_calls, dict):
        agent_calls = {}
    if not isinstance(retries, dict):
        retries = {}
    return {
        "available": bool(agent_calls or retries),
        "agentCalls": sum(int(v or 0) for v in agent_calls.values() if isinstance(v, (int, float))),
        "retries": sum(int(v or 0) for v in retries.values() if isinstance(v, (int, float))),
        # The collaboration runtime does not durably persist provider token
        # usage/model failures yet; null is more honest than an estimate.
        "tokenUsage": None,
        "modelFailures": None,
    }


def _decision_summary(value: Any) -> str:
    decision = _json(value, {})
    if not isinstance(decision, dict):
        return ""
    for key in ("decisionSummary", "fusionSummary", "summary"):
        text = sanitize_public_text(decision.get(key))
        if text:
            return text[:500]
    return ""


def _planner_audit_summary(value: Any) -> Dict[str, Any]:
    audit = value if isinstance(value, dict) else {}
    usage = audit.get("usageSummary") if isinstance(audit.get("usageSummary"), dict) else {}
    numeric_usage = {
        key: usage.get(key)
        for key in ("promptTokens", "completionTokens", "totalTokens")
        if isinstance(usage.get(key), (int, float))
    }
    return _safe_trace({
        "planningModeRequested": audit.get("planningModeRequested"),
        "planningModeUsed": audit.get("planningModeUsed"),
        "plannerModel": audit.get("plannerModel"),
        "confidence": audit.get("confidence"),
        "attemptCount": audit.get("attemptCount"),
        "latencyMs": audit.get("latencyMs"),
        "usageAvailable": bool(numeric_usage),
        # Reinsert explicit numeric usage after the generic credential
        # sanitizer; no credential-bearing arbitrary token fields are copied.
        "usage": numeric_usage,
        "fallbackReason": audit.get("fallbackReason"),
        "goalCoverage": audit.get("goalCoverage"),
    }) | {"usage": numeric_usage}


def _timeline_entry(
    *,
    occurred_at: str,
    event_type: str,
    source: str,
    source_id: str,
    event_id: str,
    workflow_run_id: str = "",
    action_execution_id: str = "",
    agent_run_id: str = "",
    plan_id: str = "",
    approval_id: str = "",
    attempt_id: str = "",
    status: str = "",
    source_sequence: Optional[int] = None,
    summary: str = "",
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    parsed_time = _parse_time(occurred_at)
    return {
        "sequence": 0,
        "sourceSequence": source_sequence,
        "occurredAt": _iso(parsed_time) if parsed_time is not None else None,
        "eventType": event_type,
        "source": source,
        "sourceId": source_id,
        "eventId": event_id,
        "agentRunId": agent_run_id or None,
        "planId": plan_id or None,
        "workflowRunId": workflow_run_id or None,
        "approvalId": approval_id or None,
        "actionExecutionId": action_execution_id or None,
        "attemptId": attempt_id or None,
        "status": status or None,
        "summary": sanitize_public_text(summary)[:500],
        "details": _safe_trace(details or {}),
    }


def _timeline_sort(entries: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    materialized = list(entries)

    def action_attempt(entry: Dict[str, Any]) -> int:
        details = entry.get("details")
        details = details if isinstance(details, dict) else {}
        raw = (
            entry.get("sourceSequence")
            if entry.get("source") == "action_attempt"
            else details.get("attempt")
        )
        try:
            return max(0, int(raw or 0))
        except (TypeError, ValueError):
            return 0

    # Workflow audit sequence is authoritative within a Run.  Attempt rows
    # live in another table, so anchor their equal-second positions to the
    # corresponding action_started/terminal audit facts.
    action_starts: Dict[Tuple[str, str, int, str], int] = {}
    action_terminals: Dict[Tuple[str, str, int, str], int] = {}
    approval_anchors: Dict[Tuple[str, str, str, str], int] = {}
    for entry in materialized:
        if entry.get("source") != "workflow_audit":
            continue
        source_seq = entry.get("sourceSequence")
        if not isinstance(source_seq, int):
            continue
        run_id = str(entry.get("workflowRunId") or "")
        occurred_at = str(entry.get("occurredAt") or "")
        event_type = str(entry.get("eventType") or "")
        action_id = str(entry.get("actionExecutionId") or "")
        if action_id:
            identity = (run_id, action_id, action_attempt(entry), occurred_at)
            if event_type == "action_started":
                action_starts.setdefault(identity, source_seq)
            elif event_type in {
                "action_succeeded", "action_failed", "action_unknown",
                "action_blocked",
            }:
                action_terminals.setdefault(identity, source_seq)
        approval_id = str(entry.get("approvalId") or "")
        if approval_id:
            approval_anchors.setdefault(
                (run_id, approval_id, event_type, occurred_at), source_seq
            )

    def lifecycle_priority(entry: Dict[str, Any]) -> int:
        """Order equal timestamps by lifecycle semantics, then source order.

        ``sourceSequence`` values are only meaningful within one durable
        source.  Comparing them across Event, Agent and Workflow tables could
        place an Agent result before its run boundary, so cross-source ties use
        this stable lifecycle order first.
        """
        source = str(entry.get("source") or "")
        event_type = str(entry.get("eventType") or "")
        if source in {"event_ingestion", "event_record"}:
            return 0
        if event_type == "agent_run_started":
            return 10
        if source == "agent_audit":
            return 20
        if event_type == "agent_run_completed":
            return 30
        if source in {"plan", "plan_version"}:
            return 40
        if source in {"workflow_audit", "approval", "action_attempt"}:
            return 50
        if source == "event_lifecycle":
            return 80
        return 90

    def runtime_order(entry: Dict[str, Any]) -> int:
        source = str(entry.get("source") or "")
        source_seq = entry.get("sourceSequence")
        if source == "workflow_audit" and isinstance(source_seq, int):
            return source_seq * 10
        run_id = str(entry.get("workflowRunId") or "")
        occurred_at = str(entry.get("occurredAt") or "")
        if source == "action_attempt":
            action_id = str(entry.get("actionExecutionId") or "")
            identity = (run_id, action_id, action_attempt(entry), occurred_at)
            started = action_starts.get(identity)
            terminal = action_terminals.get(identity)
            if entry.get("eventType") == "action_attempt_started":
                if started is not None:
                    return started * 10 + 1
                if terminal is not None:
                    return terminal * 10 - 2
            elif entry.get("eventType") == "action_attempt_finished":
                if terminal is not None:
                    return terminal * 10 - 1
                if started is not None:
                    return started * 10 + 2
        if source == "approval":
            anchor = approval_anchors.get((
                run_id,
                str(entry.get("approvalId") or ""),
                str(entry.get("eventType") or ""),
                occurred_at,
            ))
            if anchor is not None:
                return anchor * 10 + 1
        return 2**31 - 1

    def key(entry: Dict[str, Any]) -> Tuple[float, int, str, int, int, str, str]:
        parsed = _parse_time(entry.get("occurredAt"))
        occurred = parsed.timestamp() if parsed is not None else float("inf")
        source_seq = entry.get("sourceSequence")
        priority = lifecycle_priority(entry)
        return (
            occurred,
            priority,
            (
                str(entry.get("workflowRunId") or "")
                if priority == 50 else ""
            ),
            runtime_order(entry) if priority == 50 else 0,
            int(source_seq) if isinstance(source_seq, int) else 2**31 - 1,
            str(entry.get("source") or ""),
            str(entry.get("sourceId") or ""),
        )

    ordered = sorted(materialized, key=key)
    for sequence, entry in enumerate(ordered):
        entry["sequence"] = sequence
    return ordered


def _agent_runs(
    event_id: str,
    *,
    limit: int = 100,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
    from backend.agent.collaboration.db_repository import SQLiteCollaborationRepository

    repo = SQLiteCollaborationRepository()
    total = repo.count_runs_by_event_id(event_id)
    rows = repo.list_runs_by_event_id(event_id, limit=limit, offset=0)
    agents: List[Dict[str, Any]] = []
    timeline: List[Dict[str, Any]] = []
    for row in rows:
        run_id = str(row.get("run_id") or "")
        started_at = _normalized_time(row.get("started_at"), legacy_local=True)
        updated_at = _normalized_time(row.get("updated_at"), legacy_local=True)
        completed_at = _normalized_time(row.get("completed_at"), legacy_local=True)
        selected = _json(row.get("selected_agents"), [])
        failed = _json(row.get("failed_agents"), [])
        item = {
            "agentRunId": run_id,
            "sessionId": row.get("session_id") or None,
            "traceId": row.get("trace_id") or None,
            "status": row.get("status") or "",
            "selectedAgents": selected if isinstance(selected, list) else [],
            "failedAgents": failed if isinstance(failed, list) else [],
            "startedAt": started_at,
            "updatedAt": updated_at,
            "completedAt": completed_at,
            "usageSummary": _usage_summary(row.get("budget_usage")),
            "context": _grounding_summary(row.get("grounding_context")),
            "decisionSummary": _decision_summary(row.get("final_decision")),
        }
        agents.append(_safe_trace(item))
        if started_at:
            timeline.append(_timeline_entry(
                occurred_at=started_at, event_type="agent_run_started",
                source="agent_run", source_id=run_id, event_id=event_id,
                agent_run_id=run_id, status="running", summary="Agent analysis started",
            ))
        for event in repo.list_events(run_id):
            payload = _json(event.get("payload"), {})
            event_created_at = _normalized_time(
                event.get("created_at"), legacy_local=True
            )
            timeline.append(_timeline_entry(
                occurred_at=event_created_at or "",
                event_type=event.get("event_type") or "agent_event",
                source="agent_audit",
                source_id=event.get("event_id") or f"{run_id}:{event.get('sequence_number', 0)}",
                event_id=event_id,
                agent_run_id=run_id,
                status=str(payload.get("status") or "") if isinstance(payload, dict) else "",
                source_sequence=int(event.get("sequence_number") or 0),
                summary=str(payload.get("message") or payload.get("agentName") or "") if isinstance(payload, dict) else "",
                details=payload if isinstance(payload, dict) else {},
            ))
        if completed_at:
            timeline.append(_timeline_entry(
                occurred_at=completed_at, event_type="agent_run_completed",
                source="agent_run", source_id=run_id, event_id=event_id,
                agent_run_id=run_id, status=str(row.get("status") or ""),
                summary="Agent analysis finished",
            ))
    return agents, timeline, total


def _plans(
    event_id: str,
    *,
    limit: int = 100,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int, bool]:
    repo = SQLiteWorkflowRepository()
    definition_total, definitions = repo.list_planning_definitions_filtered(
        event_id=event_id, limit=limit, offset=0
    )
    runs = repo.list_runs(event_id=event_id, limit=limit, offset=0)
    workflow_total = repo.count_runs(event_id=event_id)
    candidates: List[Dict[str, Any]] = []
    seen: set[Tuple[str, int]] = set()

    # A Run executes an immutable definition version.  Prefer that frozen
    # metadata whenever present so a later replan/current-definition edit does
    # not rewrite history in the trace.
    for run in runs:
        frozen = repo.get_definition_version(run.definition_id, run.version)
        if frozen is None or not isinstance(frozen.definition_json, dict):
            continue
        frozen_json = frozen.definition_json
        metadata = frozen_json.get("metadata")
        metadata = metadata if isinstance(metadata, dict) else {}
        plan = metadata.get("plan") if isinstance(metadata.get("plan"), dict) else {}
        frozen_event_id = str(plan.get("eventId") or "").strip()
        if frozen_event_id and frozen_event_id != event_id:
            continue
        key = (run.definition_id, int(run.version or 0))
        if key in seen:
            continue
        seen.add(key)
        candidates.append({
            "planId": run.definition_id,
            "planVersionId": frozen.id,
            "runVersion": int(run.version or 0),
            "metadata": metadata,
            "status": str(frozen_json.get("status") or "frozen"),
            "createdAt": frozen.created_at or frozen_json.get("createdAt") or "",
            "updatedAt": frozen_json.get("updatedAt") or frozen.created_at or "",
            "source": "plan_version",
        })

    for definition in definitions:
        metadata = definition.metadata if isinstance(definition.metadata, dict) else {}
        plan = metadata.get("plan") if isinstance(metadata.get("plan"), dict) else {}
        version = int(plan.get("version") or metadata.get("version") or 0)
        key = (definition.id, version)
        if key in seen:
            continue
        seen.add(key)
        candidates.append({
            "planId": definition.id,
            "planVersionId": None,
            "runVersion": version or None,
            "metadata": metadata,
            "status": definition.status.value,
            "createdAt": definition.created_at,
            "updatedAt": definition.updated_at,
            "source": "plan",
        })

    plans: List[Dict[str, Any]] = []
    timeline: List[Dict[str, Any]] = []
    for candidate in candidates:
        metadata = candidate["metadata"]
        plan = metadata.get("plan") if isinstance(metadata.get("plan"), dict) else {}
        plan_metadata = plan.get("metadata") if isinstance(plan.get("metadata"), dict) else {}
        source_agent = plan_metadata.get("sourceAgent") if isinstance(plan_metadata.get("sourceAgent"), dict) else {}
        plan_id = str(candidate["planId"])
        item = {
            "planId": plan_id,
            "planVersionId": candidate["planVersionId"],
            "eventId": event_id,
            "agentRunId": source_agent.get("collaborationRunId") or None,
            "status": candidate["status"],
            "version": candidate["runVersion"] or plan.get("version") or metadata.get("version"),
            "goal": sanitize_public_text(plan.get("goal"))[:500] or None,
            "decisionSummary": sanitize_public_text(
                plan_metadata.get("plannerReasonSummary")
                or plan_metadata.get("decisionSummary")
                or ""
            )[:500] or None,
            "evidenceRefs": _safe_trace(plan.get("evidenceRefs") or []),
            "plannerAudit": _planner_audit_summary(plan.get("plannerAudit")),
            "createdAt": candidate["createdAt"] or None,
            "updatedAt": candidate["updatedAt"] or None,
        }
        plans.append(item)
        timeline.append(_timeline_entry(
            occurred_at=candidate["createdAt"] or candidate["updatedAt"],
            event_type="plan_created", source=candidate["source"],
            source_id=str(candidate["planVersionId"] or plan_id),
            event_id=event_id, agent_run_id=str(item.get("agentRunId") or ""),
            plan_id=plan_id, status=str(candidate["status"]),
            summary="Disposition plan persisted",
            details={"version": item.get("version"), "goal": item.get("goal")},
        ))
    sources_truncated = (
        int(definition_total or 0) > len(definitions)
        or int(workflow_total or 0) > len(runs)
    )
    return (
        plans,
        timeline,
        max(int(definition_total or 0), len(plans)),
        sources_truncated,
    )


def _action_projection(repo: SQLiteWorkflowRepository, action: Any, now: datetime) -> Dict[str, Any]:
    attempts = repo.list_action_attempts(action.action_id)
    unknown_since = (
        action.unknown_since
        or (
            action.finished_at or action.completed_at or action.started_at or action.created_at
            if action.status.value == "unknown" else ""
        )
    )
    age = _age_seconds(unknown_since, now) if action.status.value == "unknown" else None
    last_reconciliation_age = (
        _age_seconds(action.last_reconciled_at, now)
        if action.status.value == "unknown" and action.last_reconciled_at
        else None
    )
    return {
        "actionExecutionId": action.action_id,
        "workflowRunId": action.run_id,
        "eventId": action.event_id or None,
        "nodeId": action.node_id,
        "actionType": sanitize_public_text(action.action_type),
        "status": action.status.value,
        "attempt": int(action.attempt or 0),
        "startedAt": action.started_at or action.created_at or None,
        "finishedAt": action.finished_at or action.completed_at or None,
        "unknownSince": unknown_since or None,
        "reconciliationAttempts": int(action.reconciliation_attempts or 0),
        "lastReconciledAt": action.last_reconciled_at or None,
        "unknownAgeSeconds": age,
        "lastReconciliationAgeSeconds": last_reconciliation_age,
        # Compatibility alias retained for the first Phase 21.4 API contract.
        "reconciliationAgeSeconds": age,
        "externalReference": sanitize_public_text(action.external_reference)[:500] or None,
        "retryable": bool(action.retryable),
        "reconciliationSupported": bool(action.reconciliation_supported),
        "result": _safe_trace(action.result or {}),
        "error": sanitize_public_text(action.error)[:500] or None,
        "attemptTotal": len(attempts),
        "attemptsTruncated": len(attempts) > 100,
        "attempts": [
            {
                "attemptId": attempt.attempt_id,
                "attempt": attempt.attempt,
                "status": attempt.status.value,
                "startedAt": attempt.started_at or None,
                "finishedAt": attempt.finished_at or None,
                "lastReconciledAt": attempt.last_reconciled_at or None,
                "externalReference": sanitize_public_text(attempt.external_reference)[:500] or None,
                "result": _safe_trace(attempt.result or {}),
                "error": sanitize_public_text(attempt.error)[:500] or None,
            }
            for attempt in attempts[:100]
        ],
    }


def _workflow_runs(
    event_id: str,
    *,
    limit: int = 100,
    now: Optional[datetime] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
    repo = SQLiteWorkflowRepository()
    current = now or _utc_now()
    total = repo.count_runs(event_id=event_id)
    runs = repo.list_runs(event_id=event_id, limit=limit, offset=0)
    projected: List[Dict[str, Any]] = []
    timeline: List[Dict[str, Any]] = []
    for run in runs:
        frozen_definition = repo.get_definition_version(
            run.definition_id, run.version
        )
        approvals = repo.list_approvals(run.run_id)
        all_actions = repo.list_action_records(run.run_id)
        actions = [
            action for action in all_actions
            if not action.event_id or action.event_id == event_id
        ]
        integrity_issues = [
            {
                "code": "action_event_mismatch",
                "actionExecutionId": action.action_id,
                "expectedEventId": event_id,
                "actualEventId": action.event_id,
            }
            for action in all_actions
            if action.event_id and action.event_id != event_id
        ]
        node_runs = repo.get_node_runs(run.run_id)
        projected_actions = [_action_projection(repo, action, current) for action in actions]
        projected_approvals = []
        for approval in approvals:
            age = _age_seconds(approval.created_at, current) if approval.decision.value == "pending" else None
            projected_approvals.append({
                "approvalId": approval.approval_id,
                "workflowRunId": run.run_id,
                "nodeId": approval.node_id,
                "decision": approval.decision.value,
                "reviewer": sanitize_public_text(approval.reviewer)[:200] or None,
                "comment": sanitize_public_text(approval.comment)[:500] or None,
                "createdAt": approval.created_at or None,
                "decidedAt": approval.decided_at or None,
                "waitingSeconds": age,
            })
            timeline.append(_timeline_entry(
                occurred_at=approval.created_at,
                event_type="approval_requested",
                source="approval", source_id=approval.approval_id,
                event_id=event_id, workflow_run_id=run.run_id,
                approval_id=approval.approval_id, status="pending",
                summary="Operator approval requested",
            ))
            if approval.decided_at:
                timeline.append(_timeline_entry(
                    occurred_at=approval.decided_at,
                    event_type=f"approval_{approval.decision.value}",
                    source="approval", source_id=approval.approval_id,
                    event_id=event_id, workflow_run_id=run.run_id,
                    approval_id=approval.approval_id,
                    status=approval.decision.value,
                    summary="Operator approval decided",
                    details={"reviewer": approval.reviewer, "comment": approval.comment},
                ))

        projected.append({
            "workflowRunId": run.run_id,
            "eventId": event_id,
            "planId": run.definition_id or None,
            "planVersionId": frozen_definition.id if frozen_definition else None,
            "version": int(run.version or 0),
            "status": run.status.value,
            "currentNodeId": run.current_node_id or None,
            "startedAt": run.started_at or None,
            "updatedAt": run.updated_at or None,
            "completedAt": run.completed_at or None,
            "nodeRuns": [
                {
                    "nodeRunId": node.node_run_id,
                    "nodeId": node.node_id,
                    "nodeType": node.node_type.value,
                    "status": node.status.value,
                    "attempt": node.attempt,
                    "durationMs": node.duration_ms,
                    "startedAt": node.started_at or None,
                    "completedAt": node.completed_at or None,
                    "error": sanitize_public_text(node.error)[:500] or None,
                }
                for node in node_runs[:200]
            ],
            "approvals": projected_approvals,
            "actions": projected_actions,
            "integrityIssues": integrity_issues,
            "bounds": {
                "nodeRunsReturned": min(len(node_runs), 200),
                "nodeRunsTotal": len(node_runs),
                "truncated": (
                    len(node_runs) > 200
                    or any(action["attemptsTruncated"] for action in projected_actions)
                ),
            },
        })

        for audit in repo.list_events(run.run_id):
            payload = audit.payload if isinstance(audit.payload, dict) else {}
            action_id = str(payload.get("actionExecutionId") or "")
            timeline.append(_timeline_entry(
                occurred_at=audit.created_at,
                event_type=audit.event_type,
                source="workflow_audit",
                source_id=audit.event_id,
                event_id=event_id,
                workflow_run_id=run.run_id,
                action_execution_id=action_id,
                approval_id=str(payload.get("approvalId") or ""),
                plan_id=run.definition_id,
                status=str(payload.get("status") or payload.get("outcome") or ""),
                source_sequence=audit.sequence,
                summary=str(payload.get("message") or payload.get("reason") or ""),
                details=payload,
            ))
        for action in projected_actions:
            for attempt in action["attempts"]:
                timeline.append(_timeline_entry(
                    occurred_at=attempt.get("startedAt") or action.get("startedAt") or "",
                    event_type="action_attempt_started",
                    source="action_attempt",
                    source_id=attempt["attemptId"],
                    event_id=event_id,
                    workflow_run_id=run.run_id,
                    action_execution_id=action["actionExecutionId"],
                    attempt_id=attempt["attemptId"],
                    status="running",
                    source_sequence=int(attempt.get("attempt") or 0),
                    summary=f"{action['actionType']} attempt started",
                ))
                if attempt.get("finishedAt"):
                    timeline.append(_timeline_entry(
                        occurred_at=attempt["finishedAt"],
                        event_type="action_attempt_finished",
                        source="action_attempt",
                        source_id=attempt["attemptId"],
                        event_id=event_id,
                        workflow_run_id=run.run_id,
                        action_execution_id=action["actionExecutionId"],
                        attempt_id=attempt["attemptId"],
                        status=str(attempt.get("status") or ""),
                        source_sequence=int(attempt.get("attempt") or 0),
                        summary=f"{action['actionType']} attempt finished",
                        details={
                            "lastReconciledAt": attempt.get("lastReconciledAt"),
                            "externalReference": attempt.get("externalReference"),
                            "error": attempt.get("error"),
                        },
                    ))
    return projected, timeline, int(total or 0)


def _ingestion_timeline(
    event_id: str,
    *,
    limit: int = 5000,
) -> Tuple[List[Dict[str, Any]], int]:
    conn = _connection()
    try:
        total = conn.execute(
            "SELECT COUNT(*) AS c FROM event_ingestion_audit WHERE event_id=?",
            (event_id,),
        ).fetchone()["c"]
        rows = conn.execute(
            """SELECT * FROM event_ingestion_audit
               WHERE event_id=? ORDER BY sequence LIMIT ?""",
            (event_id, max(1, int(limit))),
        ).fetchall()
    finally:
        conn.close()
    return [
        _timeline_entry(
            occurred_at=row["created_at"],
            event_type=f"event_ingest_{row['outcome']}",
            source="event_ingestion",
            source_id=f"ingest:{row['sequence']}",
            event_id=event_id,
            source_sequence=int(row["sequence"]),
            status=row["outcome"],
            summary=f"Event ingestion {row['outcome']}",
            details={
                "source": row["source"],
                "sourceEventId": row["source_event_id"],
                "revision": row["revision"],
                "occurredAt": row["occurred_at"],
            },
        )
        for row in rows
    ], int(total or 0)


def _lifecycle_timeline(
    event_id: str,
    *,
    limit: int = 5000,
) -> Tuple[List[Dict[str, Any]], int]:
    conn = _connection()
    try:
        total = conn.execute(
            "SELECT COUNT(*) AS c FROM event_lifecycle_audit WHERE event_id=?",
            (event_id,),
        ).fetchone()["c"]
        rows = conn.execute(
            """SELECT * FROM event_lifecycle_audit
               WHERE event_id=? ORDER BY sequence LIMIT ?""",
            (event_id, max(1, int(limit))),
        ).fetchall()
    finally:
        conn.close()
    return [
        _timeline_entry(
            occurred_at=row["created_at"],
            event_type=row["event_type"] or "event_status_updated",
            source="event_lifecycle",
            source_id=f"lifecycle:{row['sequence']}",
            event_id=event_id,
            source_sequence=int(row["sequence"]),
            status=row["status"] or "",
            summary="Event status changed",
            details={
                "previousStatus": row["previous_status"] or None,
                "status": row["status"] or None,
                "actor": row["actor"] or None,
            },
        )
        for row in rows
    ], int(total or 0)


def build_event_trace(
    event_id: str,
    *,
    timeline_limit: int = 200,
    timeline_offset: int = 0,
    requested_by: str = "event",
    requested_id: str = "",
    _timeline_cap: int = 500,
    _source_limit: int = 100,
) -> Optional[Dict[str, Any]]:
    """Build one bounded DTO from exact durable Event relationships."""
    canonical = str(event_id or "").strip()
    record = get_event_by_id(canonical)
    if record is None:
        return None
    now = _utc_now()
    source_limit = max(1, int(_source_limit))
    agents, agent_timeline, agent_total = _agent_runs(
        canonical, limit=source_limit
    )
    plans, plan_timeline, plan_total, plans_truncated = _plans(
        canonical, limit=source_limit
    )
    workflows, workflow_timeline, workflow_total = _workflow_runs(
        canonical, limit=source_limit, now=now
    )
    ingestion_timeline, ingestion_total = _ingestion_timeline(
        canonical, limit=max(source_limit, 5000)
    )
    lifecycle_timeline, lifecycle_total = _lifecycle_timeline(
        canonical, limit=max(source_limit, 5000)
    )
    entries = list(ingestion_timeline)
    entries.extend(lifecycle_timeline)
    entries.extend(agent_timeline)
    entries.extend(plan_timeline)
    entries.extend(workflow_timeline)
    # Legacy rows predate ingestion audit.  Their persisted Event timestamps
    # still provide an honest lifecycle boundary without inventing a source.
    if not ingestion_timeline and (record.get("createdAt") or record.get("receivedAt")):
        entries.append(_timeline_entry(
            occurred_at=_normalized_time(
                record.get("receivedAt") or record.get("createdAt") or "",
                legacy_local=not bool(record.get("receivedAt")),
            ) or "",
            event_type="event_recorded", source="event_record",
            source_id=canonical, event_id=canonical,
            summary="Event record persisted",
        ))
    timeline = _timeline_sort(entries)
    total = len(timeline)
    offset = max(0, int(timeline_offset))
    limit = min(max(1, int(_timeline_cap)), max(1, int(timeline_limit)))
    source_coverage = {
        "ingestionFacts": {"returned": len(ingestion_timeline), "total": ingestion_total},
        "lifecycleFacts": {"returned": len(lifecycle_timeline), "total": lifecycle_total},
        "agentRuns": {"returned": len(agents), "total": agent_total},
        "plans": {
            "returned": len(plans), "total": plan_total,
            "truncated": plans_truncated,
        },
        "workflowRuns": {"returned": len(workflows), "total": workflow_total},
    }
    nested_truncated = any(
        bool(workflow.get("bounds", {}).get("truncated"))
        for workflow in workflows
    )
    source_coverage["workflowRuns"]["nestedTruncated"] = nested_truncated
    source_truncated = any(
        bool(item.get("truncated"))
        or int(item["returned"]) < int(item["total"])
        for item in source_coverage.values()
    ) or nested_truncated
    return {
        "eventId": canonical,
        "requestedBy": requested_by,
        "requestedId": requested_id or canonical,
        "generatedAt": _iso(now),
        "event": _event_summary(record),
        "agentRuns": agents,
        "plans": plans,
        "workflowRuns": workflows,
        "correlation": {
            "eventId": canonical,
            "agentRunIds": list(dict.fromkeys(item["agentRunId"] for item in agents)),
            "planIds": list(dict.fromkeys(item["planId"] for item in plans)),
            "workflowRunIds": list(dict.fromkeys(item["workflowRunId"] for item in workflows)),
            "approvalIds": list(dict.fromkeys(
                approval["approvalId"]
                for workflow in workflows for approval in workflow["approvals"]
            )),
            "actionExecutionIds": list(dict.fromkeys(
                action["actionExecutionId"]
                for workflow in workflows for action in workflow["actions"]
            )),
            "attemptIds": list(dict.fromkeys(
                attempt["attemptId"]
                for workflow in workflows for action in workflow["actions"]
                for attempt in action["attempts"]
            )),
        },
        "timeline": {
            "total": total,
            "limit": limit,
            "offset": offset,
            "truncated": source_truncated or offset + limit < total,
            "totalIsLowerBound": source_truncated,
            "sourceCoverage": source_coverage,
            "items": timeline[offset:offset + limit],
        },
        "boundaries": {
            "rawPromptsIncluded": False,
            "rawProviderResponsesIncluded": False,
            "chainOfThoughtIncluded": False,
        },
    }


def _workflow_event_identity(
    repo: SQLiteWorkflowRepository,
    run: Any,
) -> str:
    state = run.state if isinstance(run.state, dict) else {}
    current = state.get("currentEvent") if isinstance(state.get("currentEvent"), dict) else {}
    candidates = {
        str(current.get("eventId") or "").strip()
    }
    candidates.update(
        str(action.event_id or "").strip()
        for action in repo.list_action_records(run.run_id)
    )
    frozen = (
        repo.get_definition_version(run.definition_id, run.version)
        if run.definition_id else None
    )
    if frozen is not None and isinstance(frozen.definition_json, dict):
        raw_metadata = frozen.definition_json.get("metadata")
        metadata = raw_metadata if isinstance(raw_metadata, dict) else {}
    else:
        definition = repo.get_definition(run.definition_id) if run.definition_id else None
        metadata = (
            definition.metadata
            if definition and isinstance(definition.metadata, dict)
            else {}
        )
    plan = metadata.get("plan") if isinstance(metadata.get("plan"), dict) else {}
    candidates.add(str(plan.get("eventId") or "").strip())
    candidates.discard("")
    if len(candidates) > 1:
        raise TraceIntegrityError(
            f"Workflow {run.run_id} has conflicting Event identities"
        )
    return next(iter(candidates), "")


def trace_for_workflow(run_id: str, **page: Any) -> Optional[Dict[str, Any]]:
    repo = SQLiteWorkflowRepository()
    run = repo.get_run(run_id)
    if run is None:
        return None
    event_id = _workflow_event_identity(repo, run)
    if not event_id:
        return None
    return build_event_trace(
        event_id, requested_by="workflowRunId", requested_id=run_id, **page
    )


def trace_for_action(action_id: str, **page: Any) -> Optional[Dict[str, Any]]:
    repo = SQLiteWorkflowRepository()
    action = repo.get_action_record(action_id)
    if action is None:
        return None
    run = repo.get_run(action.run_id)
    run_event_id = _workflow_event_identity(repo, run) if run is not None else ""
    action_event_id = str(action.event_id or "").strip()
    if action_event_id and run_event_id and action_event_id != run_event_id:
        raise TraceIntegrityError(
            f"Action {action_id} and Workflow {action.run_id} have conflicting Event identities"
        )
    event_id = action_event_id or run_event_id
    if not event_id:
        return None
    return build_event_trace(
        event_id, requested_by="actionExecutionId", requested_id=action_id, **page
    )


def _filter_audit_entries(
    entries: Sequence[Dict[str, Any]],
    *,
    event_type: str = "",
    status: str = "",
    from_time: str = "",
    to_time: str = "",
) -> List[Dict[str, Any]]:
    start = _parse_time(from_time)
    end = _parse_time(to_time)
    result = []
    for entry in entries:
        if event_type and entry.get("eventType") != event_type:
            continue
        if status and entry.get("status") != status:
            continue
        occurred = _parse_time(entry.get("occurredAt"))
        if start and (occurred is None or occurred < start):
            continue
        if end and (occurred is None or occurred > end):
            continue
        result.append(entry)
    return result


def query_event_audit(
    event_id: str,
    *,
    limit: int = 100,
    offset: int = 0,
    event_type: str = "",
    status: str = "",
    from_time: str = "",
    to_time: str = "",
) -> Optional[Dict[str, Any]]:
    # Audit is allowed a larger bounded projection than the interactive Trace
    # endpoint so pagination and filters are not silently limited to its first
    # 500 entries.  Five thousand entries is an explicit SQLite-era guardrail,
    # not an unbounded log-search contract.
    trace = build_event_trace(
        event_id,
        timeline_limit=5000,
        timeline_offset=0,
        _timeline_cap=5000,
        _source_limit=5000,
    )
    if trace is None:
        return None
    entries = _filter_audit_entries(
        trace["timeline"]["items"], event_type=event_type, status=status,
        from_time=from_time, to_time=to_time,
    )
    page_limit = min(200, max(1, int(limit)))
    page_offset = max(0, int(offset))
    return {
        "scope": "event",
        "eventId": event_id,
        "total": len(entries),
        "limit": page_limit,
        "offset": page_offset,
        "truncated": bool(trace["timeline"]["truncated"]),
        "totalIsLowerBound": bool(trace["timeline"]["truncated"]),
        "sourceTotal": int(trace["timeline"]["total"]),
        "sourceTotalIsLowerBound": bool(trace["timeline"]["totalIsLowerBound"]),
        "items": entries[page_offset:page_offset + page_limit],
    }


def _workflow_audit_entries(
    repo: SQLiteWorkflowRepository,
    run: Any,
) -> Tuple[str, List[Dict[str, Any]]]:
    """Return every durable per-run event in authoritative sequence order."""
    state = run.state if isinstance(run.state, dict) else {}
    current = state.get("currentEvent") if isinstance(state.get("currentEvent"), dict) else {}
    event_id = str(current.get("eventId") or "")
    entries = [
        _timeline_entry(
            occurred_at=audit.created_at,
            event_type=audit.event_type,
            source="workflow_audit", source_id=audit.event_id,
            event_id=event_id, workflow_run_id=run.run_id,
            action_execution_id=str((audit.payload or {}).get("actionExecutionId") or ""),
            approval_id=str((audit.payload or {}).get("approvalId") or ""),
            plan_id=run.definition_id,
            status=str((audit.payload or {}).get("status") or (audit.payload or {}).get("outcome") or ""),
            source_sequence=audit.sequence,
            summary=str((audit.payload or {}).get("message") or (audit.payload or {}).get("reason") or ""),
            details=audit.payload or {},
        )
        for audit in repo.list_events(run.run_id)
    ]
    # ``workflow_events.sequence`` is the durable per-run audit ordering.  It
    # remains authoritative even when a producer supplies an older timestamp
    # (or hosts have slightly skewed clocks), so do not reorder this scope by
    # wall-clock time as the cross-source Event timeline does.
    entries.sort(key=lambda entry: int(entry.get("sourceSequence") or 0))
    for sequence, entry in enumerate(entries):
        entry["sequence"] = sequence
    return event_id, entries


def query_workflow_audit(run_id: str, **filters: Any) -> Optional[Dict[str, Any]]:
    repo = SQLiteWorkflowRepository()
    run = repo.get_run(run_id)
    if run is None:
        return None
    event_id, entries = _workflow_audit_entries(repo, run)
    entries = _filter_audit_entries(
        entries,
        event_type=str(filters.get("event_type") or ""),
        status=str(filters.get("status") or ""),
        from_time=str(filters.get("from_time") or ""),
        to_time=str(filters.get("to_time") or ""),
    )
    limit = min(200, max(1, int(filters.get("limit") or 100)))
    offset = max(0, int(filters.get("offset") or 0))
    return {
        "scope": "workflow",
        "eventId": event_id or None,
        "workflowRunId": run_id,
        "total": len(entries), "limit": limit, "offset": offset,
        "items": entries[offset:offset + limit],
    }


def query_action_audit(action_id: str, **filters: Any) -> Optional[Dict[str, Any]]:
    repo = SQLiteWorkflowRepository()
    action = repo.get_action_record(action_id)
    if action is None:
        return None
    run = repo.get_run(action.run_id)
    _, workflow_entries = (
        _workflow_audit_entries(repo, run) if run is not None else ("", [])
    )
    entries = [
        entry for entry in workflow_entries
        if entry.get("actionExecutionId") == action_id
    ]
    for attempt in repo.list_action_attempts(action_id):
        entries.append(_timeline_entry(
            occurred_at=attempt.started_at,
            event_type="action_attempt_started", source="action_attempt",
            source_id=attempt.attempt_id, event_id=action.event_id,
            workflow_run_id=action.run_id, action_execution_id=action_id,
            attempt_id=attempt.attempt_id, status="running",
            source_sequence=attempt.attempt,
            summary=f"{action.action_type} attempt started",
        ))
        if attempt.finished_at:
            entries.append(_timeline_entry(
                occurred_at=attempt.finished_at,
                event_type="action_attempt_finished", source="action_attempt",
                source_id=attempt.attempt_id, event_id=action.event_id,
                workflow_run_id=action.run_id, action_execution_id=action_id,
                attempt_id=attempt.attempt_id, status=attempt.status.value,
                source_sequence=attempt.attempt,
                summary=f"{action.action_type} attempt finished",
                details={
                    "lastReconciledAt": attempt.last_reconciled_at or None,
                    "externalReference": attempt.external_reference or None,
                    "result": attempt.result or {}, "error": attempt.error or None,
                },
            ))
    entries = _timeline_sort(entries)
    entries = _filter_audit_entries(
        entries,
        event_type=str(filters.get("event_type") or ""),
        status=str(filters.get("status") or ""),
        from_time=str(filters.get("from_time") or ""),
        to_time=str(filters.get("to_time") or ""),
    )
    limit = min(200, max(1, int(filters.get("limit") or 100)))
    offset = max(0, int(filters.get("offset") or 0))
    return {
        "scope": "action", "eventId": action.event_id or None,
        "workflowRunId": action.run_id, "actionExecutionId": action_id,
        "total": len(entries), "limit": limit, "offset": offset,
        "items": entries[offset:offset + limit],
    }


def approval_age(created_at: str, now: datetime, config: OperationsConfig) -> Dict[str, Any]:
    age = _age_seconds(created_at, now)
    if age is None:
        classification = "unknown"
    elif age >= config.approval_overdue_after:
        classification = "overdue"
    elif age >= config.approval_attention_after:
        classification = "attention"
    else:
        classification = "normal"
    return {"waitingSeconds": age, "classification": classification}


def _alert_issue(
    *,
    alert_type: str,
    resource_type: str,
    resource_id: str,
    severity: str,
    message: str,
    event_id: str = "",
    workflow_run_id: str = "",
    action_execution_id: str = "",
    approval_id: str = "",
) -> Dict[str, Any]:
    return {
        "alertType": alert_type,
        "resourceType": resource_type,
        "resourceId": resource_id,
        "severity": severity,
        "message": sanitize_public_text(message)[:500],
        "eventId": event_id,
        "workflowRunId": workflow_run_id,
        "actionExecutionId": action_execution_id,
        "approvalId": approval_id,
    }


def _detect_issues(
    conn: sqlite3.Connection,
    *,
    now: datetime,
    config: OperationsConfig,
) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    issues: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    unknown_rows = conn.execute(
        """SELECT a.* FROM workflow_action_records a
           WHERE a.status='unknown' ORDER BY a.created_at"""
    ).fetchall()
    for row in unknown_rows:
        unknown_since = (
            row["unknown_since"] or row["finished_at"] or row["completed_at"]
            or row["started_at"] or row["created_at"]
        )
        age = _age_seconds(unknown_since, now)
        if age is None or age < config.unknown_alert_after:
            continue
        issue = _alert_issue(
            alert_type="ACTION_UNKNOWN_TOO_LONG",
            resource_type="action", resource_id=row["action_id"],
            severity="critical",
            message=(
                f"Action {row['action_type']} outcome has remained UNKNOWN "
                f"for {age // 60}m"
            ),
            event_id=row["event_id"] or "",
            workflow_run_id=row["run_id"],
            action_execution_id=row["action_id"],
        )
        issues[(issue["alertType"], "action", row["action_id"])] = issue

    running_rows = conn.execute(
        """SELECT * FROM workflow_runs WHERE status='running'
           ORDER BY updated_at"""
    ).fetchall()
    for row in running_rows:
        age = _age_seconds(row["updated_at"], now)
        if age is None or age < config.workflow_stuck_after:
            continue
        lease = _parse_time(row["driver_lease_until"])
        if lease is not None and lease >= now:
            continue
        wake = _parse_time(row["wake_at"])
        if row["wait_type"] and wake is not None and wake >= now:
            continue
        pending = conn.execute(
            "SELECT 1 FROM workflow_approvals WHERE run_id=? AND decision='pending' LIMIT 1",
            (row["run_id"],),
        ).fetchone()
        if pending is not None:
            continue
        recent_action = conn.execute(
            """SELECT started_at FROM workflow_action_records
               WHERE run_id=? AND status IN ('running','executing')
               ORDER BY started_at DESC LIMIT 1""",
            (row["run_id"],),
        ).fetchone()
        if recent_action is not None:
            action_age = _age_seconds(recent_action["started_at"], now)
            if action_age is not None and action_age < config.workflow_stuck_after:
                continue
        state = _json(row["state_json"], {})
        current = state.get("currentEvent") if isinstance(state, dict) else {}
        event_id = str(current.get("eventId") or "") if isinstance(current, dict) else ""
        issue = _alert_issue(
            alert_type="WORKFLOW_STUCK",
            resource_type="workflow", resource_id=row["run_id"],
            severity="high",
            message=f"Workflow has made no durable progress for {age // 60}m",
            event_id=event_id, workflow_run_id=row["run_id"],
        )
        issues[(issue["alertType"], "workflow", row["run_id"])] = issue

    approvals = conn.execute(
        """SELECT p.*, r.state_json FROM workflow_approvals p
           JOIN workflow_runs r ON r.run_id=p.run_id
           WHERE p.decision='pending' ORDER BY p.created_at"""
    ).fetchall()
    for row in approvals:
        aging = approval_age(row["created_at"], now, config)
        if aging["classification"] != "overdue":
            continue
        state = _json(row["state_json"], {})
        current = state.get("currentEvent") if isinstance(state, dict) else {}
        event_id = str(current.get("eventId") or "") if isinstance(current, dict) else ""
        issue = _alert_issue(
            alert_type="APPROVAL_WAITING_TOO_LONG",
            resource_type="approval", resource_id=row["approval_id"],
            severity="medium",
            message=f"Approval has waited {int(aging['waitingSeconds'] or 0) // 60}m",
            event_id=event_id, workflow_run_id=row["run_id"],
            approval_id=row["approval_id"],
        )
        issues[(issue["alertType"], "approval", row["approval_id"])] = issue

    repeated = conn.execute(
        """SELECT a.*, COUNT(t.attempt_id) AS failure_count
           FROM workflow_action_records a
           JOIN workflow_action_attempts t ON t.action_id=a.action_id
           WHERE a.status='failed' AND t.status='failed'
           GROUP BY a.action_id HAVING COUNT(t.attempt_id) >= ?""",
        (config.repeated_failure_threshold,),
    ).fetchall()
    for row in repeated:
        issue = _alert_issue(
            alert_type="ACTION_REPEATED_FAILURE",
            resource_type="action", resource_id=row["action_id"],
            severity="high",
            message=(
                f"Action {row['action_type']} failed "
                f"{int(row['failure_count'] or 0)} times"
            ),
            event_id=row["event_id"] or "",
            workflow_run_id=row["run_id"],
            action_execution_id=row["action_id"],
        )
        issues[(issue["alertType"], "action", row["action_id"])] = issue
    return issues


def scan_operational_alerts(
    *,
    now: Optional[datetime] = None,
    config: Optional[OperationsConfig] = None,
) -> Dict[str, Any]:
    """Deduplicate current issues and resolve active alerts after recovery."""
    init_operations_tables()
    current = (now or _utc_now()).astimezone(timezone.utc)
    settings = config or OperationsConfig.current()
    timestamp = _iso(current)
    conn = _connection()
    created = updated = resolved = 0
    try:
        conn.execute("BEGIN IMMEDIATE")
        issues = _detect_issues(conn, now=current, config=settings)
        active_rows = conn.execute(
            "SELECT * FROM operational_alerts WHERE status='active'"
        ).fetchall()
        active = {
            (row["alert_type"], row["resource_type"], row["resource_id"]): row
            for row in active_rows
        }
        for identity, issue in issues.items():
            existing = active.get(identity)
            if existing is not None:
                conn.execute(
                    """UPDATE operational_alerts SET last_seen_at=?,
                           occurrence_count=occurrence_count + 1,
                           severity=?, message=?, event_id=?, workflow_run_id=?,
                           action_execution_id=?, approval_id=?
                       WHERE alert_id=? AND status='active'""",
                    (
                        timestamp, issue["severity"], issue["message"],
                        issue["eventId"], issue["workflowRunId"],
                        issue["actionExecutionId"], issue["approvalId"],
                        existing["alert_id"],
                    ),
                )
                updated += 1
                continue
            alert_id = f"opalert_{uuid.uuid4().hex[:16]}"
            conn.execute(
                """INSERT INTO operational_alerts (
                       alert_id, event_id, workflow_run_id, action_execution_id,
                       approval_id, resource_type, resource_id, alert_type,
                       severity, status, first_seen_at, last_seen_at,
                       resolved_at, occurrence_count, message
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, '', 1, ?)""",
                (
                    alert_id, issue["eventId"], issue["workflowRunId"],
                    issue["actionExecutionId"], issue["approvalId"],
                    issue["resourceType"], issue["resourceId"],
                    issue["alertType"], issue["severity"], timestamp,
                    timestamp, issue["message"],
                ),
            )
            created += 1
        for identity, row in active.items():
            if identity in issues:
                continue
            conn.execute(
                """UPDATE operational_alerts SET status='resolved',
                       resolved_at=?, last_seen_at=?
                   WHERE alert_id=? AND status='active'""",
                (timestamp, timestamp, row["alert_id"]),
            )
            resolved += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    result = {
        "scannedAt": timestamp,
        "activeIssues": len(issues),
        "created": created, "updated": updated, "resolved": resolved,
    }
    log_runtime_event(
        component="operations.monitor", operation="operational_alert_scan",
        status="completed", **result,
    )
    return result


def _alert_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "alertId": row["alert_id"], "eventId": row["event_id"] or None,
        "workflowRunId": row["workflow_run_id"] or None,
        "actionExecutionId": row["action_execution_id"] or None,
        "approvalId": row["approval_id"] or None,
        "resourceType": row["resource_type"], "resourceId": row["resource_id"],
        "alertType": row["alert_type"], "severity": row["severity"],
        "status": row["status"], "firstSeenAt": row["first_seen_at"],
        "lastSeenAt": row["last_seen_at"], "resolvedAt": row["resolved_at"] or None,
        "occurrenceCount": int(row["occurrence_count"] or 0),
        "message": sanitize_public_text(row["message"])[:500],
    }


def list_operational_alerts(
    *,
    status: str = "active",
    alert_type: str = "",
    severity: str = "",
    event_id: str = "",
    workflow_run_id: str = "",
    action_execution_id: str = "",
    limit: int = 100,
    offset: int = 0,
) -> Dict[str, Any]:
    init_operations_tables()
    clauses = ["1=1"]
    params: List[Any] = []
    for column, value in (
        ("status", status), ("alert_type", alert_type), ("severity", severity),
        ("event_id", event_id), ("workflow_run_id", workflow_run_id),
        ("action_execution_id", action_execution_id),
    ):
        if value:
            clauses.append(f"{column}=?")
            params.append(value)
    where = " AND ".join(clauses)
    page_limit = min(200, max(1, int(limit)))
    page_offset = max(0, int(offset))
    conn = _connection()
    try:
        total = conn.execute(
            f"SELECT COUNT(*) AS c FROM operational_alerts WHERE {where}", params
        ).fetchone()["c"]
        rows = conn.execute(
            f"""SELECT * FROM operational_alerts WHERE {where}
                ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                         WHEN 'medium' THEN 2 ELSE 3 END,
                         last_seen_at DESC, alert_id DESC LIMIT ? OFFSET ?""",
            [*params, page_limit, page_offset],
        ).fetchall()
    finally:
        conn.close()
    return {
        "total": int(total or 0), "limit": page_limit, "offset": page_offset,
        "alerts": [_alert_dict(row) for row in rows],
    }


def runtime_summary(*, now: Optional[datetime] = None) -> Dict[str, Any]:
    init_operations_tables()
    current = (now or _utc_now()).astimezone(timezone.utc)
    config = OperationsConfig.current()
    conn = _connection()
    try:
        ingestion = conn.execute(
            """SELECT COUNT(*) AS received,
                      SUM(CASE WHEN outcome='created' THEN 1 ELSE 0 END) AS created,
                      SUM(CASE WHEN outcome='updated' THEN 1 ELSE 0 END) AS updated,
                      SUM(CASE WHEN outcome='duplicate' THEN 1 ELSE 0 END) AS duplicate
               FROM event_ingestion_audit"""
        ).fetchone()
        event_counts = conn.execute(
            """SELECT COUNT(*) AS total,
                      SUM(CASE WHEN status IN ('已处置','待复盘','已归档') THEN 1 ELSE 0 END) AS resolved,
                      SUM(CASE WHEN status NOT IN ('已处置','待复盘','已归档') THEN 1 ELSE 0 END) AS active
               FROM event_records"""
        ).fetchone()
        agent = conn.execute(
            """SELECT COUNT(*) AS total,
                      SUM(CASE WHEN status IN ('completed','partial_success') THEN 1 ELSE 0 END) AS succeeded,
                      SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
                      AVG(CASE WHEN completed_at IS NOT NULL AND completed_at != ''
                                    AND started_at IS NOT NULL AND started_at != ''
                               THEN (julianday(completed_at)-julianday(started_at))*86400000 END) AS avg_duration_ms
               FROM collaboration_runs"""
        ).fetchone()
        workflow_rows = conn.execute(
            "SELECT status, COUNT(*) AS c FROM workflow_runs GROUP BY status"
        ).fetchall()
        workflow = {row["status"]: int(row["c"] or 0) for row in workflow_rows}
        action = conn.execute(
            """SELECT COUNT(*) AS total,
                      SUM(CASE WHEN status='succeeded' THEN 1 ELSE 0 END) AS succeeded,
                      SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
                      SUM(CASE WHEN status='unknown' THEN 1 ELSE 0 END) AS unknown_count,
                      SUM(CASE WHEN attempt > 1 THEN attempt - 1 ELSE 0 END) AS retries,
                      SUM(reconciliation_attempts) AS reconciliations,
                      SUM(CASE WHEN reconciliation_attempts > 0 AND status='succeeded' THEN 1 ELSE 0 END) AS reconciliation_success,
                      SUM(CASE WHEN reconciliation_attempts > 0 AND status='unknown' THEN 1 ELSE 0 END) AS reconciliation_unresolved
               FROM workflow_action_records"""
        ).fetchone()
        unresolved_alerts = conn.execute(
            "SELECT COUNT(*) AS c FROM operational_alerts WHERE status='active'"
        ).fetchone()["c"]
        approvals = conn.execute(
            """SELECT p.*, r.state_json FROM workflow_approvals p
               JOIN workflow_runs r ON r.run_id=p.run_id
               WHERE p.decision='pending' ORDER BY p.created_at LIMIT 20"""
        ).fetchall()
        approval_count = conn.execute(
            "SELECT COUNT(*) AS c FROM workflow_approvals WHERE decision='pending'"
        ).fetchone()["c"]
        approval_age_rows = conn.execute(
            """SELECT classification, COUNT(*) AS c FROM (
                   SELECT CASE
                       WHEN julianday(created_at) IS NULL THEN 'unknown'
                       WHEN (julianday(?) - julianday(created_at)) * 86400 >= ? THEN 'overdue'
                       WHEN (julianday(?) - julianday(created_at)) * 86400 >= ? THEN 'attention'
                       ELSE 'normal'
                   END AS classification
                   FROM workflow_approvals WHERE decision='pending'
               ) GROUP BY classification""",
            (
                _iso(current), config.approval_overdue_after,
                _iso(current), config.approval_attention_after,
            ),
        ).fetchall()
        unknown_rows = conn.execute(
            """SELECT * FROM workflow_action_records WHERE status='unknown'
               ORDER BY unknown_since, created_at LIMIT 20"""
        ).fetchall()
        recent = conn.execute(
            """SELECT
                 SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS ingests_24h,
                 SUM(CASE WHEN outcome='duplicate' AND created_at >= ? THEN 1 ELSE 0 END) AS duplicates_24h
               FROM event_ingestion_audit""",
            (_iso(current - timedelta(hours=24)), _iso(current - timedelta(hours=24))),
        ).fetchone()
    finally:
        conn.close()

    approval_items = []
    aging_counts = {"normal": 0, "attention": 0, "overdue": 0, "unknown": 0}
    for row in approval_age_rows:
        classification = str(row["classification"] or "unknown")
        if classification in aging_counts:
            aging_counts[classification] = int(row["c"] or 0)
    for row in approvals:
        aging = approval_age(row["created_at"], current, config)
        state = _json(row["state_json"], {})
        event = state.get("currentEvent") if isinstance(state, dict) else {}
        approval_items.append({
            "approvalId": row["approval_id"], "workflowRunId": row["run_id"],
            "eventId": event.get("eventId") if isinstance(event, dict) else None,
            "createdAt": row["created_at"], **aging,
        })
    unknown_items = []
    for row in unknown_rows:
        unknown_since = (
            row["unknown_since"] or row["finished_at"] or row["completed_at"]
            or row["started_at"] or row["created_at"]
        )
        unknown_items.append({
            "actionExecutionId": row["action_id"], "workflowRunId": row["run_id"],
            "eventId": row["event_id"] or None,
            "actionType": sanitize_public_text(row["action_type"]),
            "unknownSince": unknown_since or None,
            "reconciliationAttempts": int(row["reconciliation_attempts"] or 0),
            "lastReconciledAt": row["last_reconciled_at"] or None,
            "unknownAgeSeconds": _age_seconds(unknown_since, current),
            "lastReconciliationAgeSeconds": _age_seconds(
                row["last_reconciled_at"], current
            ),
            "reconciliationAgeSeconds": _age_seconds(unknown_since, current),
        })
    action_total = int(action["total"] or 0)
    action_succeeded = int(action["succeeded"] or 0)
    active_workflows = sum(workflow.get(key, 0) for key in ("pending", "running", "paused", "awaiting_approval"))
    return {
        "generatedAt": _iso(current),
        "activeEvents": int(event_counts["active"] or 0),
        "runningWorkflows": int(workflow.get("running", 0)),
        "waitingApprovals": int(approval_count or 0),
        "failedWorkflows": int(workflow.get("failed", 0)),
        "pausedWorkflows": int(workflow.get("paused", 0)),
        "unknownActions": int(action["unknown_count"] or 0),
        "unresolvedAlerts": int(unresolved_alerts or 0),
        "healthy": not any((
            int(workflow.get("failed", 0)), int(action["unknown_count"] or 0),
            int(unresolved_alerts or 0),
        )),
        "events": {
            "ingestCount": int(ingestion["received"] or 0),
            "createdCount": int(ingestion["created"] or 0),
            "updatedCount": int(ingestion["updated"] or 0),
            "duplicateCount": int(ingestion["duplicate"] or 0),
            "active": int(event_counts["active"] or 0),
            "resolved": int(event_counts["resolved"] or 0),
            "total": int(event_counts["total"] or 0),
            "coverage": "phase_21_4_onward",
        },
        "agents": {
            "total": int(agent["total"] or 0),
            "succeeded": int(agent["succeeded"] or 0),
            "failed": int(agent["failed"] or 0),
            "averageDurationMs": (
                round(float(agent["avg_duration_ms"]), 2)
                if agent["avg_duration_ms"] is not None else None
            ),
            "modelFailures": None,
            "tokenUsage": None,
            "usageAvailable": False,
        },
        "workflows": {
            "active": active_workflows,
            "pending": int(workflow.get("pending", 0)),
            "running": int(workflow.get("running", 0)),
            "paused": int(workflow.get("paused", 0)),
            "awaitingApproval": int(workflow.get("awaiting_approval", 0)),
            "completed": int(workflow.get("completed", 0)),
            "failed": int(workflow.get("failed", 0)),
            "cancelled": int(workflow.get("cancelled", 0)),
            "rejected": int(workflow.get("rejected", 0)),
        },
        "actions": {
            "total": action_total, "succeeded": action_succeeded,
            "successRate": round(action_succeeded / action_total, 4) if action_total else None,
            "failed": int(action["failed"] or 0),
            "unknown": int(action["unknown_count"] or 0),
            "retries": int(action["retries"] or 0),
            "reconciliations": int(action["reconciliations"] or 0),
            "reconciliationSuccess": int(action["reconciliation_success"] or 0),
            "reconciliationUnresolved": int(action["reconciliation_unresolved"] or 0),
        },
        "approvalAging": {"counts": aging_counts, "items": approval_items},
        "unknownActionAging": {"items": unknown_items},
        "trends": {
            "ingests24h": int(recent["ingests_24h"] or 0),
            "duplicates24h": int(recent["duplicates_24h"] or 0),
        },
    }


class OperationsMonitor:
    """Lightweight in-process scanner; alert truth remains durable in SQLite."""

    def __init__(self, config: Optional[OperationsConfig] = None):
        self.config = config or OperationsConfig.current()
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    async def start(self) -> None:
        if self._running:
            return
        init_operations_tables()
        self._running = True
        self._stop_event = asyncio.Event()
        # Scan once before sleeping so restart immediately restores alert truth.
        scan_operational_alerts(config=self.config)
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self.config.scan_interval
                )
                break
            except asyncio.TimeoutError:
                try:
                    scan_operational_alerts(config=self.config)
                except Exception as exc:
                    log_runtime_event(
                        component="operations.monitor",
                        operation="operational_alert_scan",
                        status="failed", error=type(exc).__name__,
                    )


_monitor: Optional[OperationsMonitor] = None


def get_operations_monitor() -> OperationsMonitor:
    global _monitor
    if _monitor is None:
        _monitor = OperationsMonitor()
    return _monitor


def reset_operations_monitor() -> None:
    global _monitor
    _monitor = None
