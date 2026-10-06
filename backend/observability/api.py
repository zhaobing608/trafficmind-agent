"""
Phase 14 Observability API — 只读聚合层

GET /observability/workflows/{run_id}
"""
from __future__ import annotations
import json
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query
from backend.workflow.repository import SQLiteWorkflowRepository
from backend.observability.models import (
    WorkflowObservability, NodeObservation, AgentObservation,
    ApprovalObservation, ActionObservation,
    NODE_DISPLAY, NODE_DESCRIPTIONS, sanitize_observability,
)

router = APIRouter(prefix="/observability", tags=["Observability V1"])
operations_router = APIRouter(prefix="/operations", tags=["Runtime Operations"])
event_trace_router = APIRouter(prefix="/events", tags=["Execution Trace"])
_repo = SQLiteWorkflowRepository()


def _node_event_duration(events, node_id: str, attempt: int = 1) -> int:
    """Fallback duration from node_started → node_completed event pair."""
    started_at = ""
    completed_at = ""
    for e in events:
        if e.node_id == node_id:
            if e.event_type == "node_started":
                started_at = e.created_at or ""
            elif e.event_type == "node_completed":
                completed_at = e.created_at or ""
    if started_at and completed_at:
        return _parse_duration(started_at, completed_at)
    return 0


def _parse_duration(start: str, end: str) -> int:
    try:
        from datetime import datetime
        s = datetime.fromisoformat(start.replace("Z", "+00:00"))
        e = datetime.fromisoformat(end.replace("Z", "+00:00"))
        return int((e - s).total_seconds() * 1000)
    except Exception:
        return 0


@router.get("/workflows/{run_id}", summary="Workflow 可观察性聚合视图")
async def get_workflow_observability(run_id: str):
    run = _repo.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Workflow run '{run_id}' 不存在")

    state = run.state if isinstance(run.state, dict) else (json.loads(run.state) if isinstance(run.state, str) else {})
    node_runs = _repo.get_node_runs(run_id)
    events = _repo.list_events(run_id)
    action_records = _repo.list_action_records(run_id)

    # Definition name
    definition = _repo.get_definition(run.definition_id)
    def_name = definition.name if definition else run.definition_id

    # Nodes
    nodes: List[NodeObservation] = []
    for nr in sorted(node_runs, key=lambda n: n.started_at or ""):
        no = NodeObservation(
            node_id=nr.node_id, node_type=nr.node_type.value,
            display_name=NODE_DISPLAY.get(nr.node_type.value, nr.node_type.value),
            description=NODE_DESCRIPTIONS.get(nr.node_type.value, ""),
            status=nr.status.value, attempt=nr.attempt, max_attempts=nr.max_attempts,
            started_at=nr.started_at, completed_at=nr.completed_at,
            # Duration: node_run timestamps or fallback to event pair
            duration_ms=nr.duration_ms or _parse_duration(nr.started_at, nr.completed_at) or _node_event_duration(events, nr.node_id, nr.attempt),
            input_summary=sanitize_observability(nr.input_snapshot or {}),
            output_summary=sanitize_observability(nr.output_snapshot or {}),
            evidence_refs=[],
            error=nr.error,
        )
        # Agent-specific enrichment
        if nr.node_type.value == "agent_task":
            ao = state.get("agentOutputs", {})
            for aname, aout in ao.items():
                if isinstance(aout, dict):
                    no.evidence_refs = aout.get("evidenceRefs", aout.get("evidence_refs", []))
                    if isinstance(no.evidence_refs, str):
                        no.evidence_refs = [no.evidence_refs]
        nodes.append(no)

    # Agent observation
    agent: Optional[AgentObservation] = None
    agent_outputs = state.get("agentOutputs", {})
    proposed_actions = state.get("proposedActions", [])
    for aname, aout in agent_outputs.items():
        if isinstance(aout, dict):
            agent_pas = [pa for pa in proposed_actions if isinstance(pa, dict) and pa.get("source") == aname]
            sim_refs = state.get("simulationRefs", {})
            agent = AgentObservation(
                agent_name=aname,
                summary=str(aout.get("summary", "")),
                urgency=str(aout.get("urgency", "")),
                findings=[],
                proposed_actions=sanitize_observability(agent_pas),
                evidence_refs=aout.get("evidenceRefs", aout.get("evidence_refs", [])),
                spatial_context_summary={
                    "simulationRunId": sim_refs.get("simulationRunId", ""),
                    "decisionSnapshotId": sim_refs.get("decisionSnapshotId", ""),
                } if sim_refs else {},
            )
            break

    # Approval observation
    approval: Optional[ApprovalObservation] = None
    pending = state.get("pendingApproval")
    approval_ids = state.get("approvalIds", [])
    if pending and isinstance(pending, dict):
        approval = ApprovalObservation(
            approval_id=str(pending.get("approvalId", "")),
            decision=str(pending.get("decision", "pending")),
            reviewer=str(pending.get("reviewer", "")),
            comment=str(pending.get("comment", "")),
            proposed_actions=sanitize_observability(pending.get("proposedActions", [])),
            edited_actions=sanitize_observability(pending.get("editedActions", [])),
        )
    elif approval_ids:
        for aid in (approval_ids if isinstance(approval_ids, list) else [approval_ids]):
            appr = _repo.get_approval(str(aid))
            if appr:
                approval = ApprovalObservation(
                    approval_id=appr.approval_id, decision=appr.decision.value,
                    reviewer=appr.reviewer, comment=appr.comment,
                    created_at=appr.created_at, decided_at=appr.decided_at,
                    proposed_actions=sanitize_observability(appr.proposed_actions),
                    edited_actions=sanitize_observability(appr.edited_actions),
                )
                break

    # Action observations
    actions: List[ActionObservation] = []
    for ar in action_records:
        before_snap = {}
        after_snap = {}
        improvement = {}

        # Primary source: ActionRecord.result contains improvements from _execute_simulation_action
        ar_result = ar.result if isinstance(ar.result, dict) else (json.loads(ar.result) if isinstance(ar.result, str) else {})
        if ar_result and isinstance(ar_result, dict):
            improvements = ar_result.get("improvements", {})
            if isinstance(improvements, dict):
                r01 = improvements.get("R01", {})
                if r01 and isinstance(r01, dict):
                    before_snap = {"R01": {
                        "avg_speed": r01.get("speedBefore"), "queue_length": r01.get("queueBefore"),
                        "congestion_level": r01.get("congestionBefore"),
                    }}
                    after_snap = {"R01": {
                        "avg_speed": r01.get("speedAfter"), "queue_length": r01.get("queueAfter"),
                        "congestion_level": r01.get("congestionAfter"),
                    }}
                    improvement = {
                        "speed_before": r01.get("speedBefore"), "speed_after": r01.get("speedAfter"),
                        "speed_delta": r01.get("speedDelta"),
                        "queue_before": r01.get("queueBefore"), "queue_after": r01.get("queueAfter"),
                        "queue_delta": r01.get("queueDelta"),
                        "congestion_before": r01.get("congestionBefore"), "congestion_after": r01.get("congestionAfter"),
                    }

        # Fallback: try simulation snapshots if ActionRecord has no improvement data
        if not improvement:
            sim_refs = state.get("simulationRefs", {})
            sim_run_id = sim_refs.get("simulationRunId", "")
            if sim_run_id:
                try:
                    from backend.simulation.repository import SQLiteSimulationRepository
                    srepo = SQLiteSimulationRepository()
                    db_snaps = srepo.list_run_snapshots(sim_run_id)
                    sorted_snaps = sorted(db_snaps, key=lambda s: s.get("sequence", 0))
                    if len(sorted_snaps) >= 3:
                        for key in ["road_states_json"]:
                            before_raw = sorted_snaps[-2].get(key, "{}")
                            after_raw = sorted_snaps[-1].get(key, "{}")
                            if isinstance(before_raw, str): before_raw = json.loads(before_raw)
                            if isinstance(after_raw, str): after_raw = json.loads(after_raw)
                            if "R01" in before_raw: before_snap = {"R01": before_raw["R01"]}
                            if "R01" in after_raw: after_snap = {"R01": after_raw["R01"]}
                except Exception:
                    pass
            if before_snap and after_snap:
                bs = before_snap.get("R01", {})
                as_ = after_snap.get("R01", {})
                improvement = {
                    "speed_before": bs.get("avg_speed"), "speed_after": as_.get("avg_speed"),
                    "speed_delta": round(as_.get("avg_speed", 0) - bs.get("avg_speed", 0), 1),
                    "queue_before": bs.get("queue_length"), "queue_after": as_.get("queue_length"),
                    "queue_delta": round(as_.get("queue_length", 0) - bs.get("queue_length", 0), 0),
                    "congestion_before": bs.get("congestion_level"), "congestion_after": as_.get("congestion_level"),
                }

        actions.append(ActionObservation(
            action_id=ar.action_id, action_type=ar.action_type, status=ar.status.value,
            idempotency_key=ar.idempotency_key,
            before_snapshot_summary=sanitize_observability(before_snap),
            after_snapshot_summary=sanitize_observability(after_snap),
            improvement=sanitize_observability(improvement),
        ))

    # Duration: use persisted timestamps, fallback to event timestamps
    total_duration = _parse_duration(run.started_at, run.completed_at)
    if total_duration == 0:
        # Fallback: first and last event timestamps
        sorted_events = sorted(events, key=lambda e: e.created_at or "")
        if len(sorted_events) >= 2:
            total_duration = _parse_duration(sorted_events[0].created_at, sorted_events[-1].created_at)
        elif len(sorted_events) == 1:
            total_duration = 0  # single event, no meaningful duration

    # Metrics
    node_count = len(nodes)
    succeeded = sum(1 for n in nodes if n.status == "succeeded")
    failed = sum(1 for n in nodes if n.status == "failed")
    retried = sum(1 for n in nodes if n.attempt > 1)

    obs = WorkflowObservability(
        run_id=run_id, definition_id=run.definition_id, definition_name=def_name,
        status=run.status.value, started_at=run.started_at, completed_at=run.completed_at,
        total_duration_ms=total_duration, current_node=run.current_node_id,
        nodes=nodes, agent=agent, approval=approval, actions=actions,
        simulation_refs=sanitize_observability(state.get("simulationRefs", {})),
        metrics={
            "node_count": node_count, "succeeded": succeeded, "failed": failed,
            "retried": retried, "total_duration_ms": total_duration,
            "action_count": len(actions),
        },
    )

    from dataclasses import asdict
    return asdict(obs)


# ---------------------------------------------------------------------------
# Phase 21.4 — unified trace, audit, health, and operational alerts
# ---------------------------------------------------------------------------


def _audit_filters(
    *,
    limit: int,
    offset: int,
    event_type: Optional[str],
    status: Optional[str],
    from_time: Optional[str],
    to_time: Optional[str],
) -> Dict[str, Any]:
    return {
        "limit": limit,
        "offset": offset,
        "event_type": event_type or "",
        "status": status or "",
        "from_time": from_time or "",
        "to_time": to_time or "",
    }


def _trace_page(limit: int, offset: int) -> Dict[str, int]:
    return {"timeline_limit": limit, "timeline_offset": offset}


@operations_router.get("/summary", summary="Runtime health summary")
async def get_operations_summary():
    from backend.observability.operations import runtime_summary
    return runtime_summary()


@operations_router.post("/alerts/scan", summary="Run one idempotent alert scan")
async def scan_operations_alerts():
    from backend.observability.operations import scan_operational_alerts
    return scan_operational_alerts()


@operations_router.get("/alerts", summary="List durable operational alerts")
async def get_operations_alerts(
    status: Optional[str] = Query("active"),
    alertType: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    eventId: Optional[str] = Query(None),
    workflowRunId: Optional[str] = Query(None),
    actionExecutionId: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    from backend.observability.operations import (
        ALERT_TYPES, list_operational_alerts,
    )
    if status and status not in {"active", "resolved"}:
        raise HTTPException(status_code=400, detail="status must be active or resolved")
    if alertType and alertType not in ALERT_TYPES:
        raise HTTPException(status_code=400, detail="unsupported operational alert type")
    return list_operational_alerts(
        status=status or "", alert_type=alertType or "",
        severity=severity or "", event_id=eventId or "",
        workflow_run_id=workflowRunId or "",
        action_execution_id=actionExecutionId or "",
        limit=limit, offset=offset,
    )


@event_trace_router.get("/{event_id}/trace", summary="Unified Event execution trace")
async def get_event_execution_trace(
    event_id: str,
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    from backend.observability.operations import build_event_trace
    trace = build_event_trace(event_id, **_trace_page(limit, offset))
    if trace is None:
        raise HTTPException(status_code=404, detail=f"Event '{event_id}' 不存在")
    return trace


@operations_router.get(
    "/workflows/{run_id}/trace", summary="Reverse Workflow Run to Event trace"
)
async def get_workflow_event_trace(
    run_id: str,
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    from backend.observability.operations import TraceIntegrityError, trace_for_workflow
    try:
        trace = trace_for_workflow(run_id, **_trace_page(limit, offset))
    except TraceIntegrityError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if trace is None:
        raise HTTPException(status_code=404, detail="Workflow Run or Event relation not found")
    return trace


@operations_router.get(
    "/actions/{action_execution_id}/trace",
    summary="Reverse Action Execution to Event trace",
)
async def get_action_event_trace(
    action_execution_id: str,
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    from backend.observability.operations import TraceIntegrityError, trace_for_action
    try:
        trace = trace_for_action(
            action_execution_id, **_trace_page(limit, offset)
        )
    except TraceIntegrityError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if trace is None:
        raise HTTPException(status_code=404, detail="Action Execution or Event relation not found")
    return trace


@operations_router.get("/audit/events/{event_id}", summary="Query Event audit timeline")
async def get_event_audit(
    event_id: str,
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
    eventType: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    from_time: Optional[str] = Query(None, alias="from"),
    to_time: Optional[str] = Query(None, alias="to"),
):
    from backend.observability.operations import query_event_audit
    result = query_event_audit(
        event_id,
        **_audit_filters(
            limit=limit, offset=offset, event_type=eventType, status=status,
            from_time=from_time, to_time=to_time,
        ),
    )
    if result is None:
        raise HTTPException(status_code=404, detail=f"Event '{event_id}' 不存在")
    return result


@operations_router.get(
    "/audit/workflows/{run_id}", summary="Query Workflow runtime audit"
)
async def get_workflow_audit(
    run_id: str,
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
    eventType: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    from_time: Optional[str] = Query(None, alias="from"),
    to_time: Optional[str] = Query(None, alias="to"),
):
    from backend.observability.operations import query_workflow_audit
    result = query_workflow_audit(
        run_id,
        **_audit_filters(
            limit=limit, offset=offset, event_type=eventType, status=status,
            from_time=from_time, to_time=to_time,
        ),
    )
    if result is None:
        raise HTTPException(status_code=404, detail=f"Workflow Run '{run_id}' 不存在")
    return result


@operations_router.get(
    "/audit/actions/{action_execution_id}", summary="Query Action attempt audit"
)
async def get_action_audit(
    action_execution_id: str,
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
    eventType: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    from_time: Optional[str] = Query(None, alias="from"),
    to_time: Optional[str] = Query(None, alias="to"),
):
    from backend.observability.operations import query_action_audit
    result = query_action_audit(
        action_execution_id,
        **_audit_filters(
            limit=limit, offset=offset, event_type=eventType, status=status,
            from_time=from_time, to_time=to_time,
        ),
    )
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"Action Execution '{action_execution_id}' 不存在",
        )
    return result
