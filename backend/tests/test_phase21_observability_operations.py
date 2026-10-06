"""Phase 21.4 observability, audit, operations, and acceptance coverage."""

from __future__ import annotations

import json
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import backend.config as config
import backend.agent.collaboration.db_repository as collab_db
import backend.chat.chat_db as chat_db
import backend.observability.operations as operations
import backend.tools.db_tools as db_tools
from backend.event_ingestion import ingest_event
from backend.observability.logging import runtime_log_payload
from backend.observability.api import event_trace_router, operations_router
from backend.observability.operations import (
    OperationsConfig,
    approval_age,
    build_event_trace,
    init_operations_tables,
    list_operational_alerts,
    query_action_audit,
    query_event_audit,
    query_workflow_audit,
    reset_operations_monitor,
    runtime_summary,
    scan_operational_alerts,
    trace_for_action,
    trace_for_workflow,
)
from backend.tools.db_tools import update_event_status
from backend.workflow.models import (
    ActionStatus,
    ApprovalDecision,
    DefinitionStatus,
    NodeConfig,
    NodeStatus,
    NodeType,
    WorkflowActionRecord,
    WorkflowApproval,
    WorkflowDefinition,
    WorkflowDefinitionVersion,
    WorkflowNodeRun,
    WorkflowRun,
    WorkflowRunStatus,
)
from backend.workflow.repository import SQLiteWorkflowRepository, init_workflow_tables


NOW = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)
OLD = "2026-10-05T10:00:00Z"


@pytest.fixture(autouse=True)
def isolated_runtime(tmp_path, monkeypatch):
    path = str(tmp_path / "phase21_observability.db")
    monkeypatch.setattr(config, "DB_PATH", path)
    monkeypatch.setattr(db_tools, "DB_PATH", path)
    monkeypatch.setattr(collab_db, "DB_PATH", path)
    monkeypatch.setattr(chat_db, "DB_PATH", path)
    chat_db.reset_initialized()
    reset_operations_monitor()
    db_tools.init_db()
    collab_db.init_collaboration_tables()
    init_workflow_tables()
    init_operations_tables()
    yield path
    reset_operations_monitor()
    chat_db.reset_initialized()


def _settings(**overrides) -> OperationsConfig:
    values = {
        "scan_interval": 60,
        "unknown_alert_after": 60,
        "workflow_stuck_after": 60,
        "approval_attention_after": 30,
        "approval_overdue_after": 60,
        "repeated_failure_threshold": 3,
    }
    values.update(overrides)
    return OperationsConfig(**values)


def _event(source_id: str = "event-a") -> dict:
    event_id = ingest_event(
        source="phase21-observability-test",
        source_event_id=source_id,
        occurred_at="2026-10-05T17:00:00+08:00",
        event={
            "eventType": "congestion",
            "roadName": f"运维测试路-{source_id}",
            "direction": "东向西",
            "avgSpeed": 9,
            "queueLength": 300,
            "duration": 600,
            "confidence": 0.98,
        },
    )["eventId"]
    return db_tools.get_event_by_id(event_id)


def _seed_agent(event_id: str, run_id: str = "agent-run-a") -> str:
    collab_db.SQLiteCollaborationRepository().save_run({
        "run_id": run_id,
        "session_id": f"session-{run_id}",
        "trace_id": f"trace-{run_id}",
        "status": "completed",
        "normalized_event": {"eventId": event_id, "runKind": "live"},
        "selected_agents": ["CongestionAgent", "DispatchAgent"],
        "failed_agents": [],
        "budget_usage": {"used_agent_calls": {"CongestionAgent": 1}},
        "final_decision": {
            "fusionSummary": "基于真实证据生成处置建议",
            "groundingAudit": {"groundingStatus": "FULL"},
        },
        "grounding_context": {
            "groundingStatus": "FULL",
            "assembledAt": "2026-10-05T09:00:01Z",
            "currentEvent": {"eventId": event_id},
            "knowledgeContext": {
                "status": "READY",
                "evidence": [{"evidenceId": "evidence-a", "documentId": "doc-a", "chunkId": "chunk-a"}],
            },
            "caseMemoryContext": {
                "status": "READY",
                "cases": [{"caseId": "case-a", "sourceWorkflowRunId": "past-run"}],
            },
            "groundingRefs": [
                {"type": "knowledge_evidence", "evidenceId": "evidence-a", "documentId": "doc-a"}
            ],
        },
        "started_at": "2026-10-05T09:00:00Z",
        "completed_at": "2026-10-05T09:00:05Z",
    })
    collab_db.SQLiteCollaborationRepository().save_event(
        run_id,
        {
            "event_id": f"agent-audit-{run_id}",
            "event_type": "agent_result",
            "status": "succeeded",
            "agentName": "CongestionAgent",
        },
        0,
    )
    return run_id


def _seed_plan(repo: SQLiteWorkflowRepository, event_id: str, agent_run_id: str, plan_id: str = "plan-a") -> str:
    plan = {
        "planId": plan_id,
        "planFingerprint": f"fingerprint-{plan_id}",
        "eventId": event_id,
        "version": 1,
        "goal": "恢复道路通行并安全闭环",
        "metadata": {
            "sourceAgent": {"collaborationRunId": agent_run_id},
            "plannerReasonSummary": "依据研判建议生成最小处置步骤",
        },
        "plannerAudit": {
            "planningModeRequested": "llm",
            "planningModeUsed": "llm",
            "plannerModel": "planner-model",
            "attemptCount": 1,
            "latencyMs": 20,
            "usageSummary": {"promptTokens": 12, "completionTokens": 8, "totalTokens": 20},
        },
        "evidenceRefs": ["knowledge:doc-a"],
    }
    repo.save_definition(WorkflowDefinition(
        id=plan_id,
        name="Phase 21.4 plan",
        status=DefinitionStatus.ACTIVE,
        nodes=[
            NodeConfig("trigger", NodeType.TRIGGER, next_nodes=["action"]),
            NodeConfig("action", NodeType.ACTION, next_nodes=["close"]),
            NodeConfig("close", NodeType.CLOSE),
        ],
        entry_node_id="trigger",
        metadata={"planFingerprint": plan["planFingerprint"], "plan": plan},
        created_at="2026-10-05T09:00:06Z",
        updated_at="2026-10-05T09:00:06Z",
    ))
    return plan_id


def _seed_workflow(
    repo: SQLiteWorkflowRepository,
    event_id: str,
    plan_id: str,
    *,
    run_id: str = "workflow-run-a",
    status: WorkflowRunStatus = WorkflowRunStatus.RUNNING,
    updated_at: str = OLD,
) -> str:
    repo.save_run(WorkflowRun(
        run_id=run_id,
        definition_id=plan_id,
        status=status,
        current_node_id="action",
        state={
            "workflowRunId": run_id,
            "workflowDefinitionId": plan_id,
            "currentEvent": {"eventId": event_id, "roadName": "运维测试路"},
            "status": status.value,
            "currentNode": "action",
        },
        started_at="2026-10-05T09:00:07Z",
        updated_at=updated_at,
        completed_at=("2026-10-05T09:10:00Z" if status == WorkflowRunStatus.COMPLETED else ""),
    ))
    repo.append_event(run_id, "workflow_started", payload={"status": "running"}, created_at="2026-10-05T09:00:07Z")
    return run_id


def _seed_approval(repo: SQLiteWorkflowRepository, run_id: str, *, approval_id: str = "approval-a", decision: ApprovalDecision = ApprovalDecision.PENDING) -> str:
    repo.save_approval(WorkflowApproval(
        approval_id=approval_id,
        run_id=run_id,
        node_id="approval",
        proposed_actions=[{"actionType": "send_notification"}],
        decision=decision,
        reviewer="operator" if decision != ApprovalDecision.PENDING else "",
        created_at=OLD,
        decided_at=("2026-10-05T11:00:00Z" if decision != ApprovalDecision.PENDING else ""),
    ))
    return approval_id


def _seed_action(
    repo: SQLiteWorkflowRepository,
    event_id: str,
    run_id: str,
    *,
    action_id: str = "action-a",
    action_type: str = "send_notification",
    status: ActionStatus = ActionStatus.UNKNOWN,
) -> str:
    record = WorkflowActionRecord(
        action_id=action_id,
        run_id=run_id,
        node_id="action",
        action_type=action_type,
        idempotency_key=f"idem-{action_id}",
        event_id=event_id,
        status=ActionStatus.PENDING,
        created_at=OLD,
        params={"target": "值班群", "message": "事件通知"},
    )
    claimed = repo.claim_action_execution(record)
    assert claimed["claimed"] is True
    assert repo.finalize_action_execution({
        "actionExecutionId": action_id,
        "attempt": 1,
        "status": status.value,
        "result": {"message": "provider result"},
        "error": ("outcome unknown" if status == ActionStatus.UNKNOWN else ""),
        "externalReference": f"external-{action_id}",
        "finishedAt": OLD,
        "retryable": status == ActionStatus.FAILED,
        "reconciliationSupported": True,
    }) is True
    return action_id


def _chain(*, source_id: str = "event-a", unknown: bool = True):
    event = _event(source_id)
    repo = SQLiteWorkflowRepository()
    agent_run_id = _seed_agent(event["eventId"], f"agent-{source_id}")
    plan_id = _seed_plan(repo, event["eventId"], agent_run_id, f"plan-{source_id}")
    run_id = _seed_workflow(repo, event["eventId"], plan_id, run_id=f"workflow-{source_id}")
    approval_id = _seed_approval(repo, run_id, approval_id=f"approval-{source_id}", decision=ApprovalDecision.APPROVED)
    action_id = _seed_action(
        repo, event["eventId"], run_id, action_id=f"action-{source_id}",
        status=ActionStatus.UNKNOWN if unknown else ActionStatus.SUCCEEDED,
    )
    return repo, event, agent_run_id, plan_id, run_id, approval_id, action_id


def _active_alerts() -> list[dict]:
    return list_operational_alerts(status="active")["alerts"]


def test_event_agent_plan_workflow_action_full_trace():
    _, event, agent_id, plan_id, run_id, approval_id, action_id = _chain()
    trace = build_event_trace(event["eventId"])
    assert trace["correlation"] == {
        "eventId": event["eventId"],
        "agentRunIds": [agent_id], "planIds": [plan_id],
        "workflowRunIds": [run_id], "approvalIds": [approval_id],
        "actionExecutionIds": [action_id],
        "attemptIds": [f"{action_id}:attempt:1"],
    }
    assert trace["agentRuns"][0]["context"]["contextTypes"] == ["knowledge", "case_memory"]
    assert trace["plans"][0]["plannerAudit"]["usage"]["totalTokens"] == 20


def test_different_events_never_cross_trace():
    _chain(source_id="event-a")
    _, event_b, agent_b, plan_b, run_b, _, action_b = _chain(source_id="event-b")
    trace = build_event_trace(event_b["eventId"])
    serialized = json.dumps(trace, ensure_ascii=False)
    assert all(identity in serialized for identity in (agent_b, plan_b, run_b, action_b))
    assert all(identity not in serialized for identity in ("agent-event-a", "plan-event-a", "workflow-event-a", "action-event-a"))


def test_workflow_reverse_finds_exact_event():
    _, event, _, _, run_id, _, _ = _chain()
    trace = trace_for_workflow(run_id)
    assert trace["eventId"] == event["eventId"]
    assert trace["requestedBy"] == "workflowRunId"


def test_action_reverse_finds_exact_event():
    _, event, _, _, _, _, action_id = _chain()
    trace = trace_for_action(action_id)
    assert trace["eventId"] == event["eventId"]
    assert trace["requestedBy"] == "actionExecutionId"


def test_trace_dto_redacts_secrets_and_private_reasoning(monkeypatch):
    monkeypatch.setenv("TRAFFICMIND_NOTIFICATION_TOKEN", "SECRET_VALUE_123")
    repo, event, _, _, run_id, _, _ = _chain()
    repo.append_event(run_id, "unsafe_event", payload={
        "authorization": "Bearer SECRET_VALUE_123",
        "nested": {
            "apiKey": "SECRET_VALUE_123",
            "chain_of_thought": "private",
            "rawPrompt": "private prompt",
            "providerResponse": "private provider response",
            "summary": "token=SECRET_VALUE_123",
        },
    })
    serialized = json.dumps(build_event_trace(event["eventId"]), ensure_ascii=False)
    assert "SECRET_VALUE_123" not in serialized
    assert "chain_of_thought" not in serialized
    assert "private prompt" not in serialized
    assert "private provider response" not in serialized
    assert "rawEvent" not in serialized and "fullResult" not in serialized


def test_audit_sequence_is_stable():
    repo, _, _, _, run_id, _, _ = _chain()
    repo.append_event(run_id, "custom_one")
    repo.append_event(run_id, "custom_two")
    audit = query_workflow_audit(run_id)
    source_sequences = [item["sourceSequence"] for item in audit["items"]]
    assert source_sequences == sorted(source_sequences)
    assert len(source_sequences) == len(set(source_sequences))


def test_audit_pagination_is_correct():
    repo, event, _, _, run_id, _, _ = _chain()
    for index in range(6):
        repo.append_event(run_id, f"page_{index}", created_at=f"2026-10-05T09:01:{index:02d}Z")
    first = query_event_audit(event["eventId"], limit=3, offset=0)
    second = query_event_audit(event["eventId"], limit=3, offset=3)
    assert first["total"] >= 6
    assert len(first["items"]) == len(second["items"]) == 3
    assert {x["sourceId"] for x in first["items"]}.isdisjoint({x["sourceId"] for x in second["items"]})


def test_audit_filters_by_type_time_and_status():
    repo, _, _, _, run_id, _, _ = _chain()
    repo.append_event(run_id, "filter_me", payload={"status": "failed"}, created_at="2026-10-05T10:00:00Z")
    repo.append_event(run_id, "filter_me", payload={"status": "succeeded"}, created_at="2026-10-05T11:00:00Z")
    audit = query_workflow_audit(
        run_id, event_type="filter_me", status="failed",
        from_time="2026-10-05T09:30:00Z", to_time="2026-10-05T10:30:00Z",
    )
    assert audit["total"] == 1
    assert audit["items"][0]["status"] == "failed"


def test_concurrent_audit_append_never_overwrites():
    repo, _, _, _, run_id, _, _ = _chain()
    before = len(repo.list_events(run_id))
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda index: repo.append_event(run_id, f"concurrent_{index}"), range(20)))
    events = repo.list_events(run_id)
    assert len(events) == before + 20
    assert [event.sequence for event in events] == list(range(len(events)))


def test_unknown_is_first_class_runtime_metric():
    _chain()
    summary = runtime_summary(now=NOW)
    assert summary["unknownActions"] == 1
    assert summary["actions"]["unknown"] == 1
    assert summary["pausedWorkflows"] == 1


def test_unknown_timeout_creates_one_alert():
    _chain()
    result = scan_operational_alerts(now=NOW, config=_settings())
    assert result["created"] == 1
    assert _active_alerts()[0]["alertType"] == "ACTION_UNKNOWN_TOO_LONG"


def test_second_unknown_scan_deduplicates_alert():
    _chain()
    first = scan_operational_alerts(now=NOW, config=_settings())
    second = scan_operational_alerts(now=NOW + timedelta(minutes=1), config=_settings())
    alerts = _active_alerts()
    assert first["created"] == 1 and second["created"] == 0
    assert len(alerts) == 1 and alerts[0]["occurrenceCount"] == 2


def test_reconciliation_success_resolves_unknown_alert():
    repo, _, _, _, _, _, action_id = _chain()
    scan_operational_alerts(now=NOW, config=_settings())
    result = repo.apply_action_reconciliation(action_id, status=ActionStatus.SUCCEEDED, result={"delivered": True})
    assert result["updated"] is True
    scan = scan_operational_alerts(now=NOW + timedelta(minutes=1), config=_settings())
    assert scan["resolved"] == 1
    assert _active_alerts() == []


def test_recurrent_unknown_creates_new_alert_generation():
    repo, _, _, _, _, _, action_id = _chain()
    scan_operational_alerts(now=NOW, config=_settings())
    repo.apply_action_reconciliation(action_id, status=ActionStatus.SUCCEEDED)
    scan_operational_alerts(now=NOW + timedelta(minutes=1), config=_settings())
    with sqlite3.connect(config.DB_PATH) as conn:
        conn.execute(
            "UPDATE workflow_action_records SET status='unknown', unknown_since=? WHERE action_id=?",
            (OLD, action_id),
        )
        conn.commit()
    scan_operational_alerts(now=NOW + timedelta(minutes=2), config=_settings())
    history = list_operational_alerts(status="", action_execution_id=action_id)["alerts"]
    assert len(history) == 2
    assert {item["status"] for item in history} == {"active", "resolved"}


def test_genuinely_stuck_workflow_is_detected():
    event = _event("stuck")
    repo = SQLiteWorkflowRepository()
    plan = _seed_plan(repo, event["eventId"], "agent-none", "plan-stuck")
    run_id = _seed_workflow(repo, event["eventId"], plan, run_id="workflow-stuck")
    scan_operational_alerts(now=NOW, config=_settings())
    assert [(a["alertType"], a["workflowRunId"]) for a in _active_alerts()] == [("WORKFLOW_STUCK", run_id)]


def test_running_workflow_with_valid_lease_is_not_stuck():
    event = _event("lease")
    repo = SQLiteWorkflowRepository()
    plan = _seed_plan(repo, event["eventId"], "agent-none", "plan-lease")
    run_id = _seed_workflow(repo, event["eventId"], plan, run_id="workflow-lease")
    with sqlite3.connect(config.DB_PATH) as conn:
        conn.execute(
            "UPDATE workflow_runs SET driver_owner='owner', driver_lease_until=? WHERE run_id=?",
            ("2026-10-05T13:00:00Z", run_id),
        )
        conn.commit()
    scan_operational_alerts(now=NOW, config=_settings())
    assert _active_alerts() == []


def test_waiting_approval_is_not_misreported_as_stuck():
    event = _event("waiting-approval")
    repo = SQLiteWorkflowRepository()
    plan = _seed_plan(repo, event["eventId"], "agent-none", "plan-waiting-approval")
    run_id = _seed_workflow(
        repo, event["eventId"], plan, run_id="workflow-waiting-approval",
        status=WorkflowRunStatus.AWAITING_APPROVAL,
    )
    _seed_approval(repo, run_id, approval_id="approval-recent")
    scan_operational_alerts(now=NOW, config=_settings(approval_overdue_after=10000))
    assert _active_alerts() == []


def test_wait_scheduler_pause_is_not_misreported_as_stuck():
    event = _event("waiting-timer")
    repo = SQLiteWorkflowRepository()
    plan = _seed_plan(repo, event["eventId"], "agent-none", "plan-waiting-timer")
    run_id = _seed_workflow(
        repo, event["eventId"], plan, run_id="workflow-waiting-timer",
        status=WorkflowRunStatus.PAUSED,
    )
    with sqlite3.connect(config.DB_PATH) as conn:
        conn.execute(
            "UPDATE workflow_runs SET wait_type='time_delay', wake_at=? WHERE run_id=?",
            ("2026-10-05T13:00:00Z", run_id),
        )
        conn.commit()
    scan_operational_alerts(now=NOW, config=_settings())
    assert _active_alerts() == []


def test_approval_aging_classification_is_exact():
    settings = _settings(approval_attention_after=60, approval_overdue_after=120)
    assert approval_age("2026-10-05T11:59:30Z", NOW, settings)["classification"] == "normal"
    assert approval_age("2026-10-05T11:58:30Z", NOW, settings)["classification"] == "attention"
    assert approval_age("2026-10-05T11:58:00Z", NOW, settings)["classification"] == "overdue"


def test_overdue_approval_alert_deduplicates_and_resolves():
    event = _event("approval-aging")
    repo = SQLiteWorkflowRepository()
    plan = _seed_plan(repo, event["eventId"], "agent-none", "plan-approval-aging")
    run_id = _seed_workflow(
        repo, event["eventId"], plan, run_id="workflow-approval-aging",
        status=WorkflowRunStatus.AWAITING_APPROVAL,
    )
    approval_id = _seed_approval(repo, run_id, approval_id="approval-aging")
    scan_operational_alerts(now=NOW, config=_settings())
    scan_operational_alerts(now=NOW + timedelta(minutes=1), config=_settings())
    alerts = _active_alerts()
    assert len(alerts) == 1 and alerts[0]["approvalId"] == approval_id
    with sqlite3.connect(config.DB_PATH) as conn:
        conn.execute(
            "UPDATE workflow_approvals SET decision='approved', decided_at=? WHERE approval_id=?",
            ("2026-10-05T12:02:00Z", approval_id),
        )
        conn.commit()
    scan_operational_alerts(now=NOW + timedelta(minutes=2), config=_settings())
    assert _active_alerts() == []


def test_approval_summary_counts_all_rows_but_bounds_detail_items():
    repo, event, agent_run_id, _, _, _, _ = _chain()
    plan_id = _seed_plan(repo, event["eventId"], agent_run_id, "plan-many-approvals")
    run_id = _seed_workflow(
        repo,
        event["eventId"],
        plan_id,
        run_id="workflow-many-approvals",
        status=WorkflowRunStatus.AWAITING_APPROVAL,
    )
    for index in range(25):
        _seed_approval(
            repo,
            run_id,
            approval_id=f"approval-many-{index:02d}",
            decision=ApprovalDecision.PENDING,
        )

    summary = runtime_summary(now=NOW)
    assert summary["waitingApprovals"] == 25
    assert summary["approvalAging"]["counts"]["overdue"] == 25
    assert len(summary["approvalAging"]["items"]) == 20


def test_trace_never_contains_credentials_headers_or_tokens(monkeypatch):
    monkeypatch.setenv("TRAFFICMIND_NOTIFICATION_TOKEN", "HEADER_SECRET_456")
    repo, event, _, _, run_id, _, _ = _chain()
    repo.append_event(run_id, "provider", payload={
        "headers": {"Authorization": "Bearer HEADER_SECRET_456"},
        "credential": "HEADER_SECRET_456", "message": "api_key=HEADER_SECRET_456",
    })
    text = json.dumps(build_event_trace(event["eventId"]), ensure_ascii=False)
    assert "HEADER_SECRET_456" not in text
    assert "Authorization" not in text and "credential" not in text


def test_operational_alert_message_never_contains_action_error_secret(monkeypatch):
    monkeypatch.setenv("TRAFFICMIND_NOTIFICATION_TOKEN", "ALERT_SECRET_789")
    repo, _, _, _, _, _, action_id = _chain()
    with sqlite3.connect(config.DB_PATH) as conn:
        conn.execute(
            "UPDATE workflow_action_records SET error=? WHERE action_id=?",
            ("Bearer ALERT_SECRET_789", action_id),
        )
        conn.commit()
    scan_operational_alerts(now=NOW, config=_settings())
    text = json.dumps(_active_alerts(), ensure_ascii=False)
    assert "ALERT_SECRET_789" not in text


def test_structured_log_sanitizer_redacts_nested_secrets(monkeypatch):
    monkeypatch.setenv("TRAFFICMIND_NOTIFICATION_TOKEN", "LOG_SECRET_123")
    payload = runtime_log_payload(
        component="workflow.action", operation="action_execution_failed",
        status="unknown", event_id="event-a", workflow_run_id="run-a",
        action_execution_id="action-a",
        headers={"Authorization": "Bearer LOG_SECRET_123"},
        error="token=LOG_SECRET_123",
        eventId="spoofed-event",
        raw_prompt="private prompt",
        providerResponse={"body": "private provider response"},
        chain_of_thought="private reasoning",
    )
    text = json.dumps(payload)
    assert "LOG_SECRET_123" not in text
    assert "private prompt" not in text
    assert "private provider response" not in text
    assert "private reasoning" not in text
    assert payload["eventId"] == "event-a" and payload["operation"] == "action_execution_failed"


def test_unresolved_alert_survives_repository_restart():
    _chain()
    scan_operational_alerts(now=NOW, config=_settings())
    init_operations_tables()  # simulate a fresh process initialization
    alerts = list_operational_alerts(status="active")["alerts"]
    assert len(alerts) == 1 and alerts[0]["status"] == "active"


def test_trace_survives_repository_restart():
    _, event, _, _, run_id, _, action_id = _chain()
    restarted = SQLiteWorkflowRepository()
    assert restarted.get_run(run_id) is not None
    trace = build_event_trace(event["eventId"])
    assert run_id in trace["correlation"]["workflowRunIds"]
    assert action_id in trace["correlation"]["actionExecutionIds"]


def test_legacy_event_keeps_received_boundary_when_runtime_rows_exist():
    _, event, _, _, _, _, _ = _chain()
    with sqlite3.connect(config.DB_PATH) as conn:
        conn.execute(
            "DELETE FROM event_ingestion_audit WHERE event_id=?",
            (event["eventId"],),
        )
        conn.commit()

    trace = build_event_trace(event["eventId"])
    recorded = [
        item for item in trace["timeline"]["items"]
        if item["eventType"] == "event_recorded"
    ]
    assert len(recorded) == 1
    assert recorded[0]["source"] == "event_record"


def test_legacy_naive_agent_times_are_normalized_from_server_local_timezone(monkeypatch):
    _, event, agent_id, _, _, _, _ = _chain()
    monkeypatch.setattr(
        operations,
        "_legacy_local_timezone",
        lambda: timezone(timedelta(hours=8)),
    )
    with sqlite3.connect(config.DB_PATH) as conn:
        conn.execute(
            """UPDATE collaboration_runs
               SET started_at='2026-10-05T17:00:00',
                   updated_at='2026-10-05T17:00:05',
                   completed_at='2026-10-05T17:00:05'
               WHERE run_id=?""",
            (agent_id,),
        )
        conn.execute(
            """UPDATE collaboration_events SET created_at='2026-10-05T17:00:02'
               WHERE run_id=?""",
            (agent_id,),
        )
        conn.commit()

    trace = build_event_trace(event["eventId"])
    started = next(
        item for item in trace["timeline"]["items"]
        if item["eventType"] == "agent_run_started"
    )
    assert started["occurredAt"] == "2026-10-05T09:00:00Z"


def test_event_status_timeline_comes_from_durable_lifecycle_audit():
    _, event, _, _, _, _, _ = _chain()
    assert update_event_status(event["eventId"], "处置中")
    assert update_event_status(event["eventId"], "已处置")

    trace = build_event_trace(event["eventId"])
    lifecycle = [
        item for item in trace["timeline"]["items"]
        if item["source"] == "event_lifecycle"
    ]
    assert [item["status"] for item in lifecycle][-2:] == ["处置中", "已处置"]
    assert lifecycle[-1]["details"]["previousStatus"] == "处置中"


def test_legacy_analysis_status_change_writes_durable_lifecycle_audit():
    event_id = "legacy-analysis-event"
    base = {
        "eventId": event_id,
        "standardEvent": {
            "eventId": event_id,
            "eventType": "congestion",
            "eventTypeCn": "交通拥堵",
            "roadName": "旧版分析路",
        },
        "riskScore": 60,
        "riskLevel": "中风险",
        "status": "待研判",
        "report": "legacy analysis",
        "analyzedAt": "2026-10-05T09:00:00Z",
    }
    assert db_tools.save_event_analysis(base)
    assert db_tools.save_event_analysis({**base, "status": "待派单"})

    trace = build_event_trace(event_id)
    lifecycle = [
        item for item in trace["timeline"]["items"]
        if item["source"] == "event_lifecycle"
    ]
    assert len(lifecycle) == 1
    assert lifecycle[0]["status"] == "待派单"
    assert lifecycle[0]["details"] == {
        "previousStatus": "待研判",
        "status": "待派单",
        "actor": "analysis_upsert",
    }


def test_workflow_completion_writes_event_lifecycle_in_same_repository_path():
    repo, event, _, _, run_id, _, _ = _chain(unknown=False)
    node_run = WorkflowNodeRun(
        node_run_id=f"{run_id}:close:1",
        run_id=run_id,
        node_id="close",
        node_type=NodeType.CLOSE,
        status=NodeStatus.RUNNING,
        attempt=1,
        started_at="2026-10-05T09:00:30Z",
    )
    repo.save_node_run(node_run)
    node_run.status = NodeStatus.SUCCEEDED
    node_run.completed_at = "2026-10-05T09:00:31Z"
    run = repo.get_run(run_id)
    run.status = WorkflowRunStatus.COMPLETED
    run.state["status"] = "completed"
    run.completed_at = node_run.completed_at
    run.updated_at = node_run.completed_at

    assert repo.finalize_node_run(node_run, checkpoint_run=run) is True
    assert db_tools.get_event_by_id(event["eventId"])["status"] == "已处置"
    lifecycle = [
        item for item in build_event_trace(event["eventId"])["timeline"]["items"]
        if item["source"] == "event_lifecycle"
    ]
    assert lifecycle[-1]["status"] == "已处置"
    assert lifecycle[-1]["details"]["previousStatus"] == "待派单"
    assert lifecycle[-1]["details"]["actor"] == "workflow_runtime"


def test_conflicting_caller_correlation_cannot_cross_runs():
    repo_a, event_a, _, _, _, _, action_a = _chain(source_id="event-a")
    repo_b, event_b, _, _, run_b, _, _ = _chain(source_id="event-b")
    appended = repo_b.append_event(
        run_b,
        "bad_correlation_attempt",
        payload={
            "eventId": event_a["eventId"],
            "workflowRunId": "wrong-run",
            "actionExecutionId": action_a,
        },
    )
    assert appended.payload["eventId"] == event_b["eventId"]
    assert appended.payload["workflowRunId"] == run_b
    assert "actionExecutionId" not in appended.payload
    assert action_a not in json.dumps(build_event_trace(event_b["eventId"]))


def test_action_with_same_run_but_different_event_is_not_correlated():
    repo, event_a, _, _, run_a, _, action_a = _chain(source_id="event-a")
    _, event_b, _, _, _, _, _ = _chain(source_id="event-b")
    with sqlite3.connect(config.DB_PATH) as conn:
        conn.execute(
            "UPDATE workflow_action_records SET event_id=? WHERE action_id=?",
            (event_b["eventId"], action_a),
        )
        conn.commit()

    appended = repo.append_event(
        run_a,
        "corrupt_action_correlation",
        payload={"actionExecutionId": action_a},
    )
    assert appended.payload["eventId"] == event_a["eventId"]
    assert appended.payload["workflowRunId"] == run_a
    assert "actionExecutionId" not in appended.payload
    assert "actionId" not in appended.payload


def test_equal_timestamp_timeline_uses_lifecycle_semantics_before_source_sequence():
    _, event, agent_id, _, _, _, _ = _chain()
    instant = "2026-10-05T09:00:00Z"
    with sqlite3.connect(config.DB_PATH) as conn:
        conn.execute(
            "DELETE FROM event_ingestion_audit WHERE event_id=?",
            (event["eventId"],),
        )
        conn.execute(
            "UPDATE event_records SET createdAt=?, receivedAt='' WHERE eventId=?",
            (instant, event["eventId"]),
        )
        conn.execute(
            """UPDATE collaboration_runs
               SET started_at=?, updated_at=?, completed_at=? WHERE run_id=?""",
            (instant, instant, instant, agent_id),
        )
        conn.execute(
            "UPDATE collaboration_events SET created_at=? WHERE run_id=?",
            (instant, agent_id),
        )
        conn.commit()

    trace = build_event_trace(event["eventId"])
    equal_time_agent_boundary = [
        item["eventType"] for item in trace["timeline"]["items"]
        if item["occurredAt"] == instant
        and item["eventType"] in {
            "event_recorded", "agent_run_started", "agent_result",
            "agent_run_completed",
        }
    ]
    assert equal_time_agent_boundary == [
        "event_recorded", "agent_run_started", "agent_result",
        "agent_run_completed",
    ]


def test_equal_timestamp_action_timeline_preserves_attempt_causality():
    repo, event, _, _, run_id, _, action_id = _chain()
    instant = "2026-10-05T09:00:20Z"
    repo.append_event(run_id, "node_completed", payload={"status": "succeeded"})
    repo.append_event(run_id, "workflow_completed", payload={"status": "completed"})
    with sqlite3.connect(config.DB_PATH) as conn:
        conn.execute(
            """UPDATE workflow_events SET created_at=?
               WHERE run_id=? AND (
                   event_type LIKE 'action_%'
                   OR event_type IN ('node_completed', 'workflow_completed')
               )""",
            (instant, run_id),
        )
        conn.execute(
            """UPDATE workflow_action_attempts
               SET started_at=?, finished_at=? WHERE action_id=?""",
            (instant, instant, action_id),
        )
        conn.commit()

    trace = build_event_trace(event["eventId"])
    action_events = [
        item["eventType"] for item in trace["timeline"]["items"]
        if item["occurredAt"] == instant
        and item["eventType"] in {
            "action_created", "action_started", "action_attempt_started",
            "action_attempt_finished", "action_unknown", "node_completed",
            "workflow_completed",
        }
    ]
    assert action_events == [
        "action_created",
        "action_started",
        "action_attempt_started",
        "action_attempt_finished",
        "action_unknown",
        "node_completed",
        "workflow_completed",
    ]


def test_trace_reports_source_truncation_and_audit_uses_larger_bound():
    _, event, _, _, _, _, _ = _chain()
    rows = []
    for index in range(100):
        run_id = f"agent-extra-{index:03d}"
        rows.append((
            run_id,
            f"session-{run_id}",
            f"trace-{run_id}",
            "completed",
            "1.0",
            json.dumps({"eventId": event["eventId"]}),
            "[]", "[]", "[]", "{}", "{}",
            "2026-10-05T09:00:00Z",
            "2026-10-05T09:00:01Z",
            "2026-10-05T09:00:01Z",
            "null", "{}",
        ))
    with sqlite3.connect(config.DB_PATH) as conn:
        conn.executemany(
            """INSERT INTO collaboration_runs (
                   run_id, session_id, trace_id, status, protocol_version,
                   normalized_event, selected_agents, skipped_agents,
                   failed_agents, budget_usage, final_decision, started_at,
                   updated_at, completed_at, previous_run_context,
                   grounding_context
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        conn.commit()

    interactive = build_event_trace(event["eventId"])
    agent_coverage = interactive["timeline"]["sourceCoverage"]["agentRuns"]
    assert agent_coverage == {"returned": 100, "total": 101}
    assert interactive["timeline"]["truncated"] is True
    assert interactive["timeline"]["totalIsLowerBound"] is True

    audit = query_event_audit(event["eventId"], limit=200, offset=0)
    assert audit["totalIsLowerBound"] is False
    assert audit["sourceTotal"] >= 101


def test_replanned_trace_uses_frozen_definition_version():
    repo, event, _, plan_id, run_id, _, _ = _chain()
    current = repo.get_definition(plan_id)
    frozen_json = current.to_dict()
    frozen_json["metadata"] = json.loads(json.dumps(frozen_json["metadata"]))
    frozen_json["metadata"]["plan"]["goal"] = "frozen version two goal"
    frozen_json["metadata"]["plan"]["version"] = 2
    repo.save_definition_version(WorkflowDefinitionVersion(
        id="wfver-frozen-two",
        definition_id=plan_id,
        version=2,
        definition_json=frozen_json,
        created_at="2026-10-05T09:00:08Z",
    ))
    run = repo.get_run(run_id)
    run.version = 2
    repo.save_run(run)

    trace = trace_for_workflow(run_id)
    frozen = next(plan for plan in trace["plans"] if plan["version"] == 2)
    assert frozen["planVersionId"] == "wfver-frozen-two"
    assert frozen["goal"] == "frozen version two goal"
    workflow = next(item for item in trace["workflowRuns"] if item["workflowRunId"] == run_id)
    assert workflow["planVersionId"] == "wfver-frozen-two"


def test_metrics_recover_from_durable_data_after_restart():
    _chain()
    before = runtime_summary(now=NOW)
    init_operations_tables()
    after = runtime_summary(now=NOW)
    assert after["unknownActions"] == before["unknownActions"] == 1
    assert after["workflows"] == before["workflows"]


def test_repeated_action_failure_alert_and_recovery():
    repo, event, _, _, run_id, _, action_id = _chain(unknown=False)
    with sqlite3.connect(config.DB_PATH) as conn:
        conn.execute("UPDATE workflow_action_records SET status='failed' WHERE action_id=?", (action_id,))
        conn.execute("DELETE FROM workflow_action_attempts WHERE action_id=?", (action_id,))
        for attempt in range(1, 4):
            conn.execute(
                """INSERT INTO workflow_action_attempts (
                       attempt_id, action_id, attempt, status, started_at, finished_at,
                       request_metadata_json, external_reference, result_json, error,
                       last_reconciled_at
                   ) VALUES (?, ?, ?, 'failed', ?, ?, '{}', '', '{}', 'failed', '')""",
                (f"{action_id}:attempt:{attempt}", action_id, attempt, OLD, OLD),
            )
        conn.commit()
    scan_operational_alerts(now=NOW, config=_settings())
    assert "ACTION_REPEATED_FAILURE" in {
        alert["alertType"] for alert in _active_alerts()
    }
    with sqlite3.connect(config.DB_PATH) as conn:
        conn.execute("UPDATE workflow_action_records SET status='succeeded' WHERE action_id=?", (action_id,))
        conn.execute(
            "UPDATE workflow_runs SET status='completed', completed_at=?, updated_at=? WHERE run_id=?",
            (NOW.isoformat(), NOW.isoformat(), run_id),
        )
        conn.commit()
    scan_operational_alerts(now=NOW + timedelta(minutes=1), config=_settings())
    assert _active_alerts() == []


def test_phase21_4_acceptance_unknown_alert_reconcile_complete_event():
    repo, event, _, _, run_id, _, action_id = _chain()
    initial = runtime_summary(now=NOW)
    assert initial["activeEvents"] == 1
    assert initial["pausedWorkflows"] == 1
    assert initial["unknownActions"] == 1

    first_scan = scan_operational_alerts(now=NOW, config=_settings())
    second_scan = scan_operational_alerts(now=NOW + timedelta(minutes=1), config=_settings())
    assert first_scan["created"] == 1 and second_scan["created"] == 0

    reconciled = repo.apply_action_reconciliation(
        action_id, status=ActionStatus.SUCCEEDED,
        result={"delivered": True}, external_reference="confirmed-receipt",
    )
    assert reconciled["updated"] is True
    run = repo.get_run(run_id)
    run.status = WorkflowRunStatus.COMPLETED
    run.state["status"] = "completed"
    run.completed_at = "2026-10-05T12:02:00Z"
    run.updated_at = run.completed_at
    repo.save_run(run)
    update_event_status(event["eventId"], "已处置")
    scan_operational_alerts(now=NOW + timedelta(minutes=2), config=_settings())

    final = runtime_summary(now=NOW + timedelta(minutes=2))
    assert final["unknownActions"] == 0
    assert final["unresolvedAlerts"] == 0
    assert final["workflows"]["completed"] == 1
    assert final["events"]["resolved"] == 1
    trace = build_event_trace(event["eventId"])
    event_types = {item["eventType"] for item in trace["timeline"]["items"]}
    assert {"action_unknown", "action_reconciled", "event_status_updated"}.issubset(event_types)


def test_event_lookup_query_plans_use_targeted_indexes():
    repo, event, _, _, _, _, _ = _chain()
    conn = sqlite3.connect(config.DB_PATH)
    try:
        plans = {
            "agent": conn.execute(
                """EXPLAIN QUERY PLAN SELECT * FROM collaboration_runs
                   WHERE json_valid(normalized_event)
                     AND json_extract(normalized_event, '$.eventId')=?""",
                (event["eventId"],),
            ).fetchall(),
            "workflow": conn.execute(
                """EXPLAIN QUERY PLAN SELECT * FROM workflow_runs
                   WHERE json_valid(state_json)
                     AND json_extract(state_json, '$.currentEvent.eventId')=?""",
                (event["eventId"],),
            ).fetchall(),
            "plan": conn.execute(
                """EXPLAIN QUERY PLAN SELECT * FROM workflow_definitions
                   WHERE json_valid(metadata_json)
                     AND json_extract(metadata_json, '$.plan.eventId')=?""",
                (event["eventId"],),
            ).fetchall(),
            "ingestion": conn.execute(
                """EXPLAIN QUERY PLAN SELECT * FROM event_ingestion_audit
                   WHERE event_id=? ORDER BY sequence LIMIT 200""",
                (event["eventId"],),
            ).fetchall(),
            "lifecycle": conn.execute(
                """EXPLAIN QUERY PLAN SELECT * FROM event_lifecycle_audit
                   WHERE event_id=? ORDER BY sequence LIMIT 200""",
                (event["eventId"],),
            ).fetchall(),
            "alerts": conn.execute(
                """EXPLAIN QUERY PLAN SELECT * FROM operational_alerts
                   WHERE status='active' ORDER BY last_seen_at DESC LIMIT 100"""
            ).fetchall(),
            "workflow_audit": conn.execute(
                """EXPLAIN QUERY PLAN SELECT * FROM workflow_events
                   WHERE run_id=? ORDER BY sequence LIMIT 200""",
                ("workflow-event-a",),
            ).fetchall(),
        }
    finally:
        conn.close()
    assert "idx_collab_runs_event_id" in " ".join(str(row) for row in plans["agent"])
    assert "idx_wf_runs_event_id" in " ".join(str(row) for row in plans["workflow"])
    assert "idx_wf_definitions_plan_event" in " ".join(str(row) for row in plans["plan"])
    assert "idx_event_ingestion_event_sequence" in " ".join(str(row) for row in plans["ingestion"])
    assert "idx_event_lifecycle_event_sequence" in " ".join(str(row) for row in plans["lifecycle"])
    assert "idx_operational_alert_status_seen" in " ".join(str(row) for row in plans["alerts"])
    assert "idx_wf_events_run_sequence" in " ".join(str(row) for row in plans["workflow_audit"])


def test_phase21_4_http_trace_and_audit_contracts():
    _, event, _, _, run_id, _, action_id = _chain()
    api = FastAPI()
    api.include_router(event_trace_router)
    api.include_router(operations_router)
    client = TestClient(api)

    event_trace = client.get(f"/events/{event['eventId']}/trace?limit=3&offset=0")
    assert event_trace.status_code == 200
    assert event_trace.json()["eventId"] == event["eventId"]
    assert event_trace.json()["timeline"]["limit"] == 3

    workflow_trace = client.get(f"/operations/workflows/{run_id}/trace")
    action_trace = client.get(f"/operations/actions/{action_id}/trace")
    assert workflow_trace.status_code == action_trace.status_code == 200
    assert workflow_trace.json()["requestedBy"] == "workflowRunId"
    assert action_trace.json()["requestedBy"] == "actionExecutionId"

    workflow_audit = client.get(
        f"/operations/audit/workflows/{run_id}",
        params={"limit": 2, "offset": 0, "eventType": "action_unknown"},
    )
    action_audit = client.get(
        f"/operations/audit/actions/{action_id}",
        params={"limit": 2, "offset": 0, "status": "unknown"},
    )
    assert workflow_audit.status_code == action_audit.status_code == 200
    assert all(
        item["eventType"] == "action_unknown"
        for item in workflow_audit.json()["items"]
    )
    assert all(item["status"] == "unknown" for item in action_audit.json()["items"])

    summary = client.get("/operations/summary")
    alerts = client.get("/operations/alerts", params={"status": "active"})
    assert summary.status_code == alerts.status_code == 200
    assert summary.json()["unknownActions"] == 1


def test_phase21_4_http_missing_resources_and_filter_validation():
    api = FastAPI()
    api.include_router(event_trace_router)
    api.include_router(operations_router)
    client = TestClient(api)

    assert client.get("/events/missing/trace").status_code == 404
    assert client.get("/operations/workflows/missing/trace").status_code == 404
    assert client.get("/operations/actions/missing/trace").status_code == 404
    assert client.get("/operations/alerts", params={"status": "bogus"}).status_code == 400
    assert client.get("/operations/alerts", params={"alertType": "BOGUS"}).status_code == 400
