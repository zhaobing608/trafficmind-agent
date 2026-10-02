"""Canonical, idempotent traffic-event ingestion for Phase 21.

The existing ``event_records`` table remains the event source of truth.  This
module adds an upstream identity (``source`` + ``sourceEventId``), deterministic
normalisation/risk scoring, and read-only relationship projection without
starting an Agent or Workflow as a side effect of ingestion.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from backend.tools.db_tools import get_connection, get_event_by_id, init_db
from backend.tools.event_tools import standardize_event, validate_event
from backend.tools.report_tools import generate_event_report
from backend.tools.risk_tools import calculate_risk_score


class EventIngestionError(ValueError):
    """A stable, client-safe canonical ingestion error."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_event_id() -> str:
    return f"evt_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:10]}"


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _fingerprint(event: Dict[str, Any], occurred_at: str, metadata: Dict[str, Any]) -> str:
    # eventId is system-owned and must not make an otherwise identical retry
    # look like an upstream update.
    business_event = {k: v for k, v in event.items() if k != "eventId"}
    payload = {
        "event": business_event,
        "occurredAt": occurred_at,
        "sourceMetadata": metadata,
    }
    return hashlib.sha256(_stable_json(payload).encode("utf-8")).hexdigest()


def _parse_record(row: sqlite3.Row) -> Dict[str, Any]:
    record = dict(row)
    for field in ("rawEvent", "fullResult", "sourceMetadata"):
        value = record.get(field)
        if isinstance(value, str):
            try:
                record[field] = json.loads(value) if value else ({} if field != "rawEvent" else {})
            except json.JSONDecodeError:
                pass
    return record


def get_event_by_source_identity(source: str, source_event_id: str) -> Optional[Dict[str, Any]]:
    """Read an event by its stable upstream identity."""
    init_db()
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM event_records WHERE source=? AND sourceEventId=?",
            (str(source or "").strip().lower(), str(source_event_id or "").strip()),
        ).fetchone()
        return _parse_record(row) if row is not None else None
    finally:
        conn.close()


def ingest_event(
    *,
    source: str,
    source_event_id: str,
    event: Dict[str, Any],
    occurred_at: str = "",
    source_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Create, deduplicate, or update one canonical event atomically.

    Ingestion never triggers Agent analysis or a Workflow.  The caller must
    explicitly start those stages using the returned internal ``eventId``.
    """
    canonical_source = str(source or "").strip().lower()
    upstream_id = str(source_event_id or "").strip()
    if not canonical_source:
        raise EventIngestionError("missing_source", "source 不能为空")
    if not upstream_id:
        raise EventIngestionError("missing_source_event_id", "sourceEventId 不能为空")
    if len(canonical_source) > 100 or len(upstream_id) > 255:
        raise EventIngestionError("invalid_source_identity", "事件来源身份长度超出限制")
    if not isinstance(event, dict):
        raise EventIngestionError("invalid_event", "event 必须为 JSON 对象")
    metadata = source_metadata or {}
    if not isinstance(metadata, dict):
        raise EventIngestionError("invalid_source_metadata", "sourceMetadata 必须为 JSON 对象")

    raw_event = dict(event)
    # Confidence is an existing model field with an existing default; accepting
    # its omission keeps webhook payloads compatible with the API models.
    raw_event.setdefault("confidence", 0.9)
    ok, error = validate_event(raw_event)
    if not ok:
        raise EventIngestionError("invalid_event", error or "事件校验失败")

    standard_event = standardize_event(raw_event)
    # Preserve only location fields already supplied by the source; no inferred
    # geography is fabricated at ingestion time.
    for key in ("regionId", "region", "location"):
        if key in raw_event:
            standard_event[key] = raw_event[key]
    occurred = str(occurred_at or raw_event.get("occurredAt") or "").strip()
    fingerprint = _fingerprint(standard_event, occurred, metadata)
    received_at = _utc_now_iso()

    init_db()
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            "SELECT * FROM event_records WHERE source=? AND sourceEventId=?",
            (canonical_source, upstream_id),
        ).fetchone()

        if existing is not None and existing["payloadFingerprint"] == fingerprint:
            conn.execute(
                "UPDATE event_records SET lastReceivedAt=? WHERE eventId=?",
                (received_at, existing["eventId"]),
            )
            conn.commit()
            record = get_event_by_id(existing["eventId"]) or _parse_record(existing)
            return {
                "outcome": "duplicate",
                "created": False,
                "updated": False,
                "duplicate": True,
                "eventId": existing["eventId"],
                "revision": int(existing["revision"] or 1),
                "event": record,
            }

        event_id = existing["eventId"] if existing is not None else _new_event_id()
        standard_event["eventId"] = event_id
        risk = calculate_risk_score(standard_event)
        report = generate_event_report(standard_event, risk, {"rule": ""}, [], "")
        existing_status = existing["status"] if existing is not None else "待研判"
        revision = int(existing["revision"] or 1) + 1 if existing is not None else 1
        full_result = {
            "eventId": event_id,
            "standardEvent": standard_event,
            "riskScore": risk.get("riskScore", 0),
            "riskLevel": risk.get("riskLevel", ""),
            "riskReasons": risk.get("riskReasons", []),
            "status": existing_status,
            "report": report,
            "source": canonical_source,
            "sourceEventId": upstream_id,
            "occurredAt": occurred,
            "receivedAt": existing["receivedAt"] if existing is not None else received_at,
            "revision": revision,
        }

        values = (
            standard_event.get("eventType", ""),
            standard_event.get("eventTypeCn", ""),
            standard_event.get("roadName", ""),
            standard_event.get("direction", ""),
            float(standard_event.get("avgSpeed", 0) or 0),
            float(standard_event.get("queueLength", 0) or 0),
            float(standard_event.get("duration", 0) or 0),
            standard_event.get("weather", "clear"),
            standard_event.get("timePeriod", "off_peak"),
            1 if standard_event.get("isMainRoad") else 0,
            1 if standard_event.get("nearbySchool") else 0,
            1 if standard_event.get("nearbyHospital") else 0,
            int(risk.get("riskScore", 0) or 0),
            str(risk.get("riskLevel", "")),
            existing_status,
            report,
            _stable_json(standard_event),
            _stable_json(full_result),
            occurred,
            received_at,
            _stable_json(metadata),
            fingerprint,
            revision,
        )

        if existing is None:
            conn.execute(
                """
                INSERT INTO event_records (
                    eventId, eventType, eventTypeCn, roadName, direction,
                    avgSpeed, queueLength, duration, weather, timePeriod,
                    isMainRoad, nearbySchool, nearbyHospital, riskScore, riskLevel,
                    status, report, rawEvent, fullResult, createdAt, updatedAt,
                    source, sourceEventId, occurredAt, receivedAt, lastReceivedAt,
                    sourceMetadata, payloadFingerprint, revision
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    event_id,
                    *values[:18],
                    received_at,
                    received_at,
                    canonical_source,
                    upstream_id,
                    values[18],
                    received_at,
                    *values[19:],
                ),
            )
            outcome = "created"
        else:
            conn.execute(
                """
                UPDATE event_records SET
                    eventType=?, eventTypeCn=?, roadName=?, direction=?,
                    avgSpeed=?, queueLength=?, duration=?, weather=?, timePeriod=?,
                    isMainRoad=?, nearbySchool=?, nearbyHospital=?, riskScore=?, riskLevel=?,
                    status=?, report=?, rawEvent=?, fullResult=?, occurredAt=?,
                    lastReceivedAt=?, sourceMetadata=?, payloadFingerprint=?, revision=?, updatedAt=?
                WHERE eventId=?
                """,
                (*values, received_at, event_id),
            )
            outcome = "updated"
        conn.commit()
    except sqlite3.IntegrityError as exc:
        conn.rollback()
        # A concurrent first delivery may win the unique source identity.  Read
        # it back and report duplicate truth rather than creating a second event.
        concurrent = get_event_by_source_identity(canonical_source, upstream_id)
        if concurrent is not None:
            return {
                "outcome": "duplicate",
                "created": False,
                "updated": False,
                "duplicate": True,
                "eventId": concurrent["eventId"],
                "revision": int(concurrent.get("revision") or 1),
                "event": concurrent,
            }
        raise EventIngestionError("ingestion_conflict", f"事件接入冲突: {exc}") from exc
    finally:
        conn.close()

    record = get_event_by_id(event_id)
    return {
        "outcome": outcome,
        "created": outcome == "created",
        "updated": outcome == "updated",
        "duplicate": False,
        "eventId": event_id,
        "revision": revision,
        "event": record,
    }


def project_event_relationships(event_id: str) -> Dict[str, Any]:
    """Project only persisted Event → Agent → Plan → Workflow relations."""
    from backend.agent.collaboration.db_repository import SQLiteCollaborationRepository
    from backend.workflow.repository import SQLiteWorkflowRepository

    collab_repo = SQLiteCollaborationRepository()
    workflow_repo = SQLiteWorkflowRepository()

    agent_total = collab_repo.count_runs_by_event_id(event_id)
    agent_rows = collab_repo.list_runs_by_event_id(event_id, limit=500, offset=0)
    agent_runs = [
        {
            "agentRunId": row.get("run_id", ""),
            "sessionId": row.get("session_id", ""),
            "status": row.get("status", ""),
            "startedAt": row.get("started_at", ""),
            "completedAt": row.get("completed_at", ""),
            "runKind": _json_field(row.get("normalized_event"), {}).get("runKind", "live"),
            "replayId": _json_field(row.get("normalized_event"), {}).get("replayId", ""),
        }
        for row in agent_rows
    ]

    plan_total, definitions = workflow_repo.list_planning_definitions_filtered(
        event_id=event_id, limit=500, offset=0
    )
    plans = []
    for definition in definitions:
        plan = (definition.metadata or {}).get("plan", {})
        if not isinstance(plan, dict):
            plan = {}
        source_agent = ((plan.get("metadata") or {}).get("sourceAgent") or {})
        plans.append({
            "planId": definition.id,
            "status": definition.status.value,
            "version": plan.get("version") or (definition.metadata or {}).get("version"),
            "agentRunId": source_agent.get("collaborationRunId", ""),
            "createdAt": definition.created_at,
            "updatedAt": definition.updated_at,
        })

    workflow_runs_raw = workflow_repo.list_runs(event_id=event_id, limit=500, offset=0)
    workflow_total = workflow_repo.count_runs(event_id=event_id)
    workflow_runs = [
        {
            "workflowRunId": run.run_id,
            "planId": run.definition_id,
            "status": run.status.value,
            "currentStep": run.current_node_id,
            "startedAt": run.started_at or None,
            "finishedAt": run.completed_at or None,
        }
        for run in workflow_runs_raw
    ]

    return {
        "agentRuns": {"total": agent_total, "items": agent_runs},
        "plans": {"total": plan_total, "items": plans},
        "workflowRuns": {"total": workflow_total, "items": workflow_runs},
    }


def _json_field(value: Any, default: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value) if value else default
        except json.JSONDecodeError:
            return default
    return value if value is not None else default
