"""Phase 21 production runtime acceptance tests.

All persistence uses an isolated SQLite database.  The tests exercise durable
event identity, truthful relationships, retry attempts, restart recovery,
approval recovery, cancellation, and action idempotency.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.responses import Response

import backend.agent.collaboration.db_repository as collab_db
import backend.chat.chat_db as chat_db
import backend.config as config
import backend.tools.db_tools as db_tools
import backend.workflow.api as workflow_api
from backend.agent.collaboration.db_repository import SQLiteCollaborationRepository
from backend.event_ingestion import ingest_event, project_event_relationships
from backend.planning.budget import new_lineage, set_lineage
from backend.workflow.executor import WorkflowExecutor
from backend.workflow.models import (
    ApprovalDecision,
    DefinitionStatus,
    NodeConfig,
    NodeStatus,
    NodeType,
    WorkflowApproval,
    WorkflowDefinition,
    WorkflowNodeRun,
    WorkflowRun,
    WorkflowRunStatus,
)
from backend.workflow.nodes.action import execute_action
from backend.workflow.nodes.base import get_node_registry
from backend.workflow.repository import SQLiteWorkflowRepository, init_workflow_tables
from backend.workflow.state import TrafficWorkflowState


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    path = str(tmp_path / "phase21_runtime.db")
    monkeypatch.setattr(config, "DB_PATH", path)
    monkeypatch.setattr(db_tools, "DB_PATH", path)
    monkeypatch.setattr(collab_db, "DB_PATH", path)
    monkeypatch.setattr(chat_db, "DB_PATH", path)
    chat_db.reset_initialized()
    db_tools.init_db()
    chat_db.init_chat_tables()
    collab_db.init_collaboration_tables()
    init_workflow_tables()
    yield path


def _event(speed: float = 9) -> dict:
    return {
        "eventType": "congestion",
        "roadName": "钱塘测试路",
        "direction": "东向西",
        "avgSpeed": speed,
        "queueLength": 680,
        "duration": 900,
        "confidence": 0.98,
        "regionId": "qt-test",
    }


async def _drain(generator):
    items = []
    async for item in generator:
        items.append(item)
    return items


def _run_id(events: list[str]) -> str:
    for item in events:
        if item.startswith("event: workflow_started"):
            return json.loads(item.split("data: ", 1)[1])["runId"]
    raise AssertionError("workflow_started event missing")


def _save_definition(repo: SQLiteWorkflowRepository, definition: WorkflowDefinition) -> None:
    repo.save_definition(definition)


def test_ingestion_create_duplicate_update_and_no_runtime_side_effects():
    first = ingest_event(
        source="camera-webhook",
        source_event_id="cam-001",
        event=_event(),
        occurred_at="2026-10-02T08:00:00+08:00",
        source_metadata={"cameraId": "camera-17"},
    )
    duplicate = ingest_event(
        source="CAMERA-WEBHOOK",
        source_event_id="cam-001",
        event=_event(),
        occurred_at="2026-10-02T08:00:00+08:00",
        source_metadata={"cameraId": "camera-17"},
    )
    assert first["outcome"] == "created"
    assert duplicate["outcome"] == "duplicate"
    assert duplicate["eventId"] == first["eventId"]
    assert duplicate["revision"] == 1

    assert db_tools.update_event_status(first["eventId"], "处置中")
    updated = ingest_event(
        source="camera-webhook",
        source_event_id="cam-001",
        event=_event(speed=6),
        occurred_at="2026-10-02T08:00:00+08:00",
        source_metadata={"cameraId": "camera-17"},
    )
    assert updated["outcome"] == "updated"
    assert updated["eventId"] == first["eventId"]
    assert updated["revision"] == 2
    assert updated["event"]["status"] == "处置中"  # update does not reset lifecycle
    assert updated["event"]["rawEvent"]["avgSpeed"] == 6

    # A late analysis save is allowed to refresh analysis data, but it cannot
    # regress the lifecycle owned by the canonical event row.
    assert db_tools.save_event_analysis({
        "eventId": first["eventId"],
        "standardEvent": {"eventId": first["eventId"], **_event(speed=5)},
        "riskScore": 88,
        "riskLevel": "高风险",
        "status": "待派单",
    })
    assert db_tools.get_event_by_id(first["eventId"])["status"] == "处置中"

    conn = db_tools.get_connection()
    try:
        assert conn.execute("SELECT COUNT(*) FROM event_records").fetchone()[0] == 1
        for table in ("collaboration_runs", "workflow_runs", "workflow_definitions"):
            assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
    finally:
        conn.close()


def test_event_relationships_are_projected_only_from_persisted_foreign_identity():
    event_id = ingest_event(
        source="api", source_event_id="relation-1", event=_event()
    )["eventId"]
    collab = SQLiteCollaborationRepository()
    collab.save_run({
        "run_id": "agent-run-1",
        "session_id": "session-1",
        "trace_id": "trace-1",
        "status": "completed",
        "normalized_event": {"eventId": event_id, "runKind": "live"},
        "selected_agents": ["CongestionAgent"],
    })
    assert db_tools.get_event_by_id(event_id)["status"] == "待派单"
    repo = SQLiteWorkflowRepository()
    definition = WorkflowDefinition(
        id="plan-1",
        name="真实关系计划",
        status=DefinitionStatus.ACTIVE,
        nodes=[NodeConfig("close", NodeType.CLOSE)],
        entry_node_id="close",
        metadata={
            "planFingerprint": "fp-1",
            "plan": {
                "eventId": event_id,
                "version": 1,
                "metadata": {"sourceAgent": {"collaborationRunId": "agent-run-1"}},
            },
        },
    )
    repo.save_definition(definition)
    repo.save_run(WorkflowRun(
        run_id="workflow-run-1",
        definition_id="plan-1",
        status=WorkflowRunStatus.RUNNING,
        state={"currentEvent": {"eventId": event_id}},
    ))

    related = project_event_relationships(event_id)
    assert related["agentRuns"]["items"][0]["agentRunId"] == "agent-run-1"
    assert related["plans"]["items"][0]["planId"] == "plan-1"
    assert related["plans"]["items"][0]["agentRunId"] == "agent-run-1"
    assert related["workflowRuns"]["items"][0]["workflowRunId"] == "workflow-run-1"
    assert related["workflowRuns"]["items"][0]["planId"] == "plan-1"


def test_replay_api_creates_isolated_agent_run_without_advancing_event():
    # Import after the isolated DB fixture has rebound every legacy module that
    # caches DB_PATH at import time.
    import backend.app as app_module

    client = TestClient(app_module.app)
    created = client.post("/events/ingest", json={
        "source": "camera-webhook",
        "sourceEventId": "replay-source-1",
        "occurredAt": "2026-10-02T08:00:00+08:00",
        "sourceMetadata": {"cameraId": "camera-17"},
        "event": _event(),
    })
    assert created.status_code == 200
    event_id = created.json()["eventId"]

    collab = SQLiteCollaborationRepository()
    collab.save_run({
        "run_id": "original-live-run",
        "session_id": "original-live-session",
        "trace_id": "original-live-trace",
        "status": "completed",
        "normalized_event": {"eventId": event_id, "runKind": "live"},
        "selected_agents": ["CongestionAgent"],
    })
    status_before_replay = db_tools.get_event_by_id(event_id)["status"]

    response = client.post(f"/events/{event_id}/replay", json={})
    assert response.status_code == 200
    replay_id = response.headers["x-trafficmind-replay-id"]
    assert replay_id.startswith("replay_")
    assert "event: replay_started" in response.text
    assert "event: done" in response.text
    assert '"status": "completed"' in response.text

    assert db_tools.get_event_by_id(event_id)["status"] == status_before_replay
    assert collab.get_run("original-live-run")["normalized_event"] == json.dumps(
        {"eventId": event_id, "runKind": "live"}, ensure_ascii=False
    )

    conn = db_tools.get_connection()
    try:
        rows = conn.execute(
            "SELECT run_id, normalized_event FROM collaboration_runs ORDER BY run_id"
        ).fetchall()
        assert len(rows) == 2
        replay_rows = [
            (row["run_id"], json.loads(row["normalized_event"]))
            for row in rows if row["run_id"] != "original-live-run"
        ]
        assert len(replay_rows) == 1
        replay_event = replay_rows[0][1]
        assert replay_event["eventId"] == event_id
        assert replay_event["runKind"] == "replay"
        assert replay_event["replayId"] == replay_id
        assert replay_event["actionExecutionAllowed"] is False
        assert conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0] == 0
    finally:
        conn.close()


def test_workflow_api_projects_truth_and_rejects_invalid_operations(monkeypatch):
    repo = SQLiteWorkflowRepository()
    monkeypatch.setattr(workflow_api, "_repo", repo)
    monkeypatch.setattr(workflow_api, "_def_manager", workflow_api.DefinitionManager(repo))
    app = FastAPI()
    app.include_router(workflow_api.router)
    client = TestClient(app)

    replay_definition = WorkflowDefinition(
        id="replay-api-plan",
        name="Replay API guard",
        status=DefinitionStatus.ACTIVE,
        nodes=[NodeConfig("close", NodeType.CLOSE)],
        entry_node_id="close",
        metadata={
            "plan": {
                "metadata": {"actionExecutionAllowed": False, "runKind": "replay"}
            }
        },
    )
    repo.save_definition(replay_definition)
    replay_start = client.post("/workflow/runs", json={
        "definitionId": replay_definition.id,
        "event": {"eventId": "forged-live", "actionExecutionAllowed": True},
    })
    assert replay_start.status_code == 409
    assert replay_start.json()["detail"]["code"] == "replay_execution_blocked"

    definition = WorkflowDefinition(
        id="api-plan",
        name="API truth plan",
        status=DefinitionStatus.ACTIVE,
        nodes=[NodeConfig("action", NodeType.ACTION)],
        entry_node_id="action",
        metadata={"planFingerprint": "api-fingerprint"},
    )
    repo.save_definition(definition)
    failed = WorkflowRun(
        run_id="api-failed",
        definition_id=definition.id,
        status=WorkflowRunStatus.FAILED,
        current_node_id="action",
        state={
            "currentEvent": {"eventId": "event-api"},
            "completedSteps": ["trigger"],
            "retryCount": 2,
            "errors": [{
                "nodeId": "action",
                "error": "人工注入故障",
                "attempt": 3,
                "timestamp": "2026-10-02T08:05:00Z",
            }],
        },
    )
    repo.save_run(failed)
    repo.save_node_run(WorkflowNodeRun(
        node_run_id="api-failed:action:3",
        run_id=failed.run_id,
        node_id="action",
        node_type=NodeType.ACTION,
        status=NodeStatus.FAILED,
        attempt=3,
        max_attempts=3,
        error="人工注入故障",
        started_at="2026-10-02T08:04:00Z",
        completed_at="2026-10-02T08:05:00Z",
    ))

    detail = client.get("/workflow/runs/api-failed")
    assert detail.status_code == 200
    payload = detail.json()
    assert payload["runtime"]["eventId"] == "event-api"
    assert payload["runtime"]["planId"] == "api-plan"
    assert payload["runtime"]["failure"] == {
        "nodeId": "action",
        "message": "人工注入故障",
        "attempt": 3,
        "timestamp": "2026-10-02T08:05:00Z",
    }
    assert payload["operations"] == {
        "canRetry": True,
        "canResume": False,
        "canCancel": False,
        "retryNodeId": "action",
    }

    invalid_resume = client.post("/workflow/runs/api-failed/resume", json={})
    assert invalid_resume.status_code == 409
    assert invalid_resume.json()["detail"]["code"] == "invalid_status"
    wrong_retry = client.post(
        "/workflow/runs/api-failed/retry", json={"nodeId": "not-the-failed-node"}
    )
    assert wrong_retry.status_code == 409

    waiting = WorkflowRun(
        run_id="api-waiting",
        definition_id=definition.id,
        status=WorkflowRunStatus.AWAITING_APPROVAL,
        current_node_id="approval",
        state={
            "currentEvent": {"eventId": "event-api"},
            "pendingApproval": {
                "approvalId": "approval-exact",
                "nodeId": "approval",
                "createdAt": "2026-10-02T08:06:00Z",
                "proposedActions": [{"type": "notify_wechat"}],
            },
        },
    )
    repo.save_run(waiting)
    repo.save_approval(WorkflowApproval(
        approval_id="approval-exact",
        run_id=waiting.run_id,
        node_id="approval",
        proposed_actions=[{"type": "notify_wechat"}],
    ))
    blocked_resume = client.post("/workflow/runs/api-waiting/resume", json={})
    assert blocked_resume.status_code == 409
    assert blocked_resume.json()["detail"]["code"] == "approval_pending"
    wrong_approval = client.post(
        "/workflow/runs/api-waiting/approvals/wrong-id",
        json={"action": "approve", "reviewer": "tester"},
    )
    assert wrong_approval.status_code == 409

    cancellable = WorkflowRun(
        run_id="api-cancellable",
        definition_id=definition.id,
        status=WorkflowRunStatus.PENDING,
        state={"currentEvent": {"eventId": "event-api"}},
    )
    repo.save_run(cancellable)
    cancelled = client.post(
        "/workflow/runs/api-cancellable/cancel",
        json={"reason": "值班员停止错误派单"},
    )
    assert cancelled.status_code == 200
    cancelled_detail = client.get("/workflow/runs/api-cancellable").json()
    assert cancelled_detail["run"]["status"] == "cancelled"
    assert cancelled_detail["runtime"]["cancelReason"] == "值班员停止错误派单"
    assert cancelled_detail["runtime"]["cancelledAt"]
    assert cancelled_detail["operations"] == {
        "canRetry": False,
        "canResume": False,
        "canCancel": False,
        "retryNodeId": None,
    }

    missing = client.post(
        "/workflow/runs/does-not-exist/cancel", json={"reason": "nothing"}
    )
    assert missing.status_code == 404
    assert missing.json()["detail"]["code"] == "not_found"


def test_actual_event_agent_plan_workflow_failure_retry_recovery(monkeypatch):
    """Run the Phase 21 acceptance chain against real repositories/APIs.

    Only the external WeChat boundary is replaced: it fails twice, then
    succeeds.  This proves truthful failure/retry semantics without sending a
    real message from the test suite.
    """
    import backend.app as app_module
    import backend.planning.api as planning_api
    import backend.workflow.nodes.action as action_module
    from backend.tools.event_identity import (
        compact_event_context,
        hydrate_authoritative_event,
    )

    client = TestClient(app_module.app)
    ingest_body = {
        "source": "acceptance-camera",
        "sourceEventId": "phase21-e2e-1",
        "occurredAt": "2026-10-02T08:00:00+08:00",
        "event": {
            **_event(speed=5),
            "roadName": "钱塘验收路",
            "queueLength": 900,
            "duration": 1200,
            "confidence": 0.99,
        },
    }
    ingested = client.post("/events/ingest", json=ingest_body)
    assert ingested.status_code == 200
    event_id = ingested.json()["eventId"]

    agent = client.post("/agent/routed_analyze/stream", json={
        "eventId": event_id,
        "content": "请研判该高风险拥堵事件并形成处置建议",
        "contextPolicy": "fresh_event",
    })
    assert agent.status_code == 200
    assert "event: done" in agent.text
    assert '"status": "completed"' in agent.text

    conn = db_tools.get_connection()
    try:
        agent_row = conn.execute(
            "SELECT run_id, session_id, status FROM collaboration_runs "
            "ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert agent_row is not None
    agent_run_id = agent_row["run_id"]
    session_id = agent_row["session_id"]
    assert agent_row["status"] == "completed"

    planned = client.post("/planning/plans/from-agent", json={
        "eventId": event_id,
        "sessionId": session_id,
        "collaborationRunId": agent_run_id,
        "plannerMode": "deterministic",
    })
    assert planned.status_code == 200
    plan_id = planned.json()["planId"]
    assert planned.json()["sourceAgent"]["collaborationRunId"] == agent_run_id
    assert planned.json()["plan"]["eventId"] == event_id

    run_request = planning_api.PlanRunRequest(
        event={"eventId": event_id},
        sessionId=session_id,
        triggeredBy="phase21-acceptance",
    )
    workflow_run_id = planning_api._create_planning_run_record(
        plan_id,
        run_request,
        initial_event=compact_event_context(hydrate_authoritative_event(event_id)),
    )
    assert workflow_run_id

    def drive_with_fresh_runtime(owner: str):
        repo = SQLiteWorkflowRepository()
        claim = repo.claim_driver_run(
            workflow_run_id, owner, "2099-01-01T00:00:00Z"
        )
        assert claim["claimed"] is True
        executor = WorkflowExecutor(repo)
        executor.set_driver_context(owner, claim["generation"])
        events = asyncio.run(_drain(executor.execute_created_run(workflow_run_id)))
        repo.release_driver_lease(workflow_run_id, owner, claim["generation"])
        return events, SQLiteWorkflowRepository().get_run(workflow_run_id)

    _, waiting = drive_with_fresh_runtime("phase21-driver-1")
    assert waiting.status == WorkflowRunStatus.AWAITING_APPROVAL
    approval_id = waiting.state["pendingApproval"]["approvalId"]
    assert SQLiteWorkflowRepository().get_approval(approval_id).decision.value == "pending"

    # Reconstruct both repository and executor to model a process restart.
    restarted_waiting = SQLiteWorkflowRepository().get_run(workflow_run_id)
    assert restarted_waiting.state["pendingApproval"]["approvalId"] == approval_id
    approved = client.post(
        f"/workflow/runs/{workflow_run_id}/approvals/{approval_id}",
        json={
            "action": "approve",
            "reviewer": "phase21-acceptance",
            "comment": "验收批准",
        },
    )
    assert approved.status_code == 200
    assert approved.json()["continuationScheduled"] is True
    after_approval = SQLiteWorkflowRepository().get_run(workflow_run_id)
    assert after_approval.status == WorkflowRunStatus.PENDING
    assert after_approval.state["executionLineage"]["rootRunId"] == workflow_run_id

    original_dispatch = action_module._dispatch_action
    notify_calls = {"count": 0}

    async def injected_dispatch(action_type, params, state):
        if action_type == "notify_wechat":
            notify_calls["count"] += 1
            if notify_calls["count"] <= 2:
                return {
                    "sent": False,
                    "channel": "acceptance",
                    "error": "人工注入：通知网关暂时不可用",
                }
            return {"sent": True, "channel": "acceptance", "simulated": True}
        return await original_dispatch(action_type, params, state)

    monkeypatch.setattr(action_module, "_dispatch_action", injected_dispatch)
    _, failed = drive_with_fresh_runtime("phase21-driver-2")
    assert failed.status == WorkflowRunStatus.FAILED
    assert notify_calls["count"] == 2  # initial + configured local retry

    detail = client.get(f"/workflow/runs/{workflow_run_id}")
    assert detail.status_code == 200
    assert detail.json()["runtime"]["failure"] == {
        "nodeId": "action_notify_wechat",
        "message": "人工注入：通知网关暂时不可用",
        "attempt": 2,
        "timestamp": failed.state["errors"][-1]["timestamp"],
    }
    assert detail.json()["operations"]["retryNodeId"] == "action_notify_wechat"

    retried = client.post(
        f"/workflow/runs/{workflow_run_id}/retry",
        json={"nodeId": "action_notify_wechat"},
    )
    assert retried.status_code == 200
    assert retried.json()["retryCount"] == 1

    _, completed = drive_with_fresh_runtime("phase21-driver-3")
    assert completed.status == WorkflowRunStatus.COMPLETED
    assert completed.state["retryCount"] == 1
    attempts = [
        (item.attempt, item.status.value, item.error)
        for item in SQLiteWorkflowRepository().get_node_runs(workflow_run_id)
        if item.node_id == "action_notify_wechat"
    ]
    assert attempts == [
        (1, "failed", "人工注入：通知网关暂时不可用"),
        (2, "failed", "人工注入：通知网关暂时不可用"),
        (3, "succeeded", ""),
    ]

    relationships = project_event_relationships(event_id)
    assert relationships["agentRuns"]["total"] == 1
    assert relationships["agentRuns"]["items"][0]["agentRunId"] == agent_run_id
    assert relationships["plans"]["total"] == 1
    assert relationships["plans"]["items"][0]["planId"] == plan_id
    assert relationships["workflowRuns"]["total"] == 1
    assert relationships["workflowRuns"]["items"][0]["workflowRunId"] == workflow_run_id

    duplicate = client.post("/events/ingest", json=ingest_body)
    assert duplicate.status_code == 200
    assert duplicate.json()["outcome"] == "duplicate"
    assert duplicate.json()["eventId"] == event_id
    after_duplicate = project_event_relationships(event_id)
    assert after_duplicate["agentRuns"]["total"] == 1
    assert after_duplicate["plans"]["total"] == 1
    assert after_duplicate["workflowRuns"]["total"] == 1

    final_event = db_tools.get_event_by_id(event_id)
    assert final_event["status"] == "已处置"
    assert final_event["roadName"] == "钱塘验收路"


def test_failure_retry_uses_new_attempt_and_skips_completed_steps():
    repo = SQLiteWorkflowRepository()
    executor = WorkflowExecutor(repo)
    registry = get_node_registry()
    original_validate = registry.get("validate_event")
    calls = {"triggered": 0, "validate": 0}
    original_trigger = registry.get("trigger")

    async def counted_trigger(state, node):
        calls["triggered"] += 1
        return await original_trigger(state, node)

    async def fail_validate(state, node):
        calls["validate"] += 1
        raise RuntimeError("人工注入的可重试故障")

    definition = WorkflowDefinition(
        id="retry-definition",
        name="retry",
        status=DefinitionStatus.ACTIVE,
        nodes=[
            NodeConfig("trigger", NodeType.TRIGGER, next_nodes=["validate"]),
            NodeConfig("validate", NodeType.VALIDATE_EVENT, next_nodes=["close"], max_attempts=1),
            NodeConfig("close", NodeType.CLOSE),
        ],
        entry_node_id="trigger",
    )
    _save_definition(repo, definition)
    registry.register("trigger", counted_trigger)
    registry.register("validate_event", fail_validate)
    try:
        events = asyncio.run(_drain(executor.start(
            definition.id,
            initial_event={"eventId": "event-retry", **_event()},
        )))
        run_id = _run_id(events)
        failed = repo.get_run(run_id)
        assert failed.status == WorkflowRunStatus.FAILED
        assert failed.state["completedSteps"] == ["trigger"]
        assert failed.state["errors"][-1]["error"] == "人工注入的可重试故障"

        scheduled = asyncio.run(executor.retry_node(run_id, "validate"))
        assert scheduled["status"] == "retrying"
        assert scheduled["retryCount"] == 1
        claim = repo.claim_driver_run(run_id, "retry-owner", "2099-01-01T00:00:00Z")
        assert claim["claimed"] is True

        async def successful_validate(state, node):
            calls["validate"] += 1
            return {"validated": True}

        restarted = WorkflowExecutor(SQLiteWorkflowRepository())
        get_node_registry().register("trigger", counted_trigger)
        get_node_registry().register("validate_event", successful_validate)
        restarted.set_driver_context("retry-owner", claim["generation"])
        asyncio.run(_drain(restarted.execute_created_run(run_id)))

        recovered = SQLiteWorkflowRepository().get_run(run_id)
        assert recovered.status == WorkflowRunStatus.COMPLETED
        assert recovered.state["retryCount"] == 1
        assert recovered.state["completedSteps"] == ["trigger", "validate", "close"]
        assert calls == {"triggered": 1, "validate": 2}
        attempts = [
            (item.attempt, item.status.value, item.error)
            for item in repo.get_node_runs(run_id)
            if item.node_id == "validate"
        ]
        assert attempts == [
            (1, "failed", "人工注入的可重试故障"),
            (2, "succeeded", ""),
        ]
        assert asyncio.run(restarted.retry_node(run_id, "validate"))["errorCode"] == "invalid_status"
    finally:
        registry.register("trigger", original_trigger)
        registry.register("validate_event", original_validate)


def test_approval_survives_restart_and_exact_decision_resumes_original_run():
    repo = SQLiteWorkflowRepository()
    definition = WorkflowDefinition(
        id="approval-definition",
        name="approval",
        status=DefinitionStatus.ACTIVE,
        nodes=[
            NodeConfig("trigger", NodeType.TRIGGER, next_nodes=["approval"]),
            NodeConfig(
                "approval", NodeType.HUMAN_APPROVAL, next_nodes=["close"],
                config={"action_types": ["notify_wechat"]},
            ),
            NodeConfig("close", NodeType.CLOSE),
        ],
        entry_node_id="trigger",
    )
    _save_definition(repo, definition)
    events = asyncio.run(_drain(WorkflowExecutor(repo).start(
        definition.id,
        initial_event={"eventId": "event-approval", **_event()},
    )))
    run_id = _run_id(events)
    waiting = SQLiteWorkflowRepository().get_run(run_id)
    approval_id = waiting.state["pendingApproval"]["approvalId"]
    assert waiting.status == WorkflowRunStatus.AWAITING_APPROVAL
    assert SQLiteWorkflowRepository().get_approval(approval_id).decision.value == "pending"

    restarted = WorkflowExecutor(SQLiteWorkflowRepository())
    mismatch = asyncio.run(restarted.approve(run_id, approval_id="another-approval"))
    assert mismatch["errorCode"] == "approval_mismatch"
    approved = asyncio.run(restarted.approve(run_id, approval_id=approval_id, reviewer="operator"))
    assert approved["continuationScheduled"] is False
    asyncio.run(_drain(restarted.resume(run_id)))

    completed = SQLiteWorkflowRepository().get_run(run_id)
    assert completed.status == WorkflowRunStatus.COMPLETED
    assert SQLiteWorkflowRepository().get_approval(approval_id).decision.value == "approved"


def test_cancel_reason_is_durable_and_terminal_run_cannot_resume_or_retry():
    repo = SQLiteWorkflowRepository()
    state = {
        "workflowRunId": "cancel-run",
        "status": "paused",
        "currentNode": "wait",
        "currentEvent": {"eventId": "cancel-event"},
    }
    repo.save_run(WorkflowRun(
        run_id="cancel-run",
        definition_id="cancel-definition",
        status=WorkflowRunStatus.PAUSED,
        current_node_id="wait",
        state=state,
    ))
    cancelled = asyncio.run(WorkflowExecutor(repo).cancel("cancel-run", "现场已人工接管"))
    assert cancelled["status"] == "cancelled"
    assert cancelled["cancelReason"] == "现场已人工接管"

    restarted = WorkflowExecutor(SQLiteWorkflowRepository())
    persisted = restarted.repo.get_run("cancel-run")
    assert persisted.status == WorkflowRunStatus.CANCELLED
    assert persisted.state["cancelReason"] == "现场已人工接管"
    assert persisted.state["cancelledAt"]
    assert asyncio.run(restarted.retry_node("cancel-run", "wait"))["errorCode"] == "invalid_status"
    resume_events = asyncio.run(_drain(restarted.resume("cancel-run")))
    assert any("无法恢复" in item for item in resume_events)


def test_action_is_idempotent_and_replay_is_fail_closed(monkeypatch):
    repo = SQLiteWorkflowRepository()
    state_dict = {
        "workflowRunId": "action-run",
        "status": "running",
        "currentEvent": {"eventId": "action-event", **_event()},
    }
    set_lineage(state_dict, new_lineage("action-run"))
    repo.save_run(WorkflowRun(
        run_id="action-run",
        status=WorkflowRunStatus.RUNNING,
        state=state_dict,
    ))
    from backend.workflow.state import TrafficWorkflowState
    state = TrafficWorkflowState.from_dict(state_dict)
    node = NodeConfig(
        "save", NodeType.ACTION,
        config={"action_type": "save_result", "action_params": {}},
    )
    dispatched = []

    async def dispatch(action_type, params, workflow_state):
        dispatched.append(action_type)
        return {"saved": True}

    monkeypatch.setattr("backend.workflow.nodes.action._dispatch_action", dispatch)
    first = asyncio.run(execute_action(state, node, repository=repo))
    second = asyncio.run(execute_action(state, node, repository=repo))
    assert first["status"] == "succeeded"
    assert second["status"] == "skipped"
    assert dispatched == ["save_result"]
    assert len(repo.list_action_records("action-run")) == 1

    replay_state = TrafficWorkflowState(
        workflow_run_id="replay-run",
        current_event={"eventId": "action-event", "actionExecutionAllowed": False},
    )
    replay = asyncio.run(execute_action(replay_state, node, repository=None))
    assert replay["status"] == "denied"
    assert replay["executed"] is False
    assert dispatched == ["save_result"]


def test_public_agent_stream_ignores_forged_replay_metadata(monkeypatch):
    import backend.app as app_module

    captured = {}

    async def fake_orchestrated(body, authoritative_event=None, runtime_metadata=None):
        captured["body"] = body.model_dump()
        captured["runtime"] = runtime_metadata
        return Response("ok", media_type="text/plain")

    monkeypatch.setattr(app_module, "COLLABORATION_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(app_module, "_orchestrated_analyze_stream", fake_orchestrated)
    response = TestClient(app_module.app).post("/agent/routed_analyze/stream", json={
        "content": "普通实时研判",
        "runKind": "replay",
        "replayId": "client-forged-replay",
        "actionExecutionAllowed": False,
    })
    assert response.status_code == 200
    assert captured["body"]["runKind"] == "replay"  # still accepted for compatibility
    assert captured["runtime"] == {
        "runKind": "live",
        "actionExecutionAllowed": True,
    }


def test_direct_executor_enforces_replay_definition_policy(monkeypatch):
    repo = SQLiteWorkflowRepository()
    definition = WorkflowDefinition(
        id="replay-direct-plan",
        name="Replay direct guard",
        status=DefinitionStatus.ACTIVE,
        nodes=[
            NodeConfig("trigger", NodeType.TRIGGER, next_nodes=["action"]),
            NodeConfig(
                "action",
                NodeType.ACTION,
                next_nodes=["close"],
                config={"action_type": "save_result", "action_params": {}},
            ),
            NodeConfig("close", NodeType.CLOSE),
        ],
        entry_node_id="trigger",
        metadata={
            "plan": {
                "metadata": {"actionExecutionAllowed": False, "runKind": "replay"}
            }
        },
    )
    repo.save_definition(definition)
    dispatched = []

    async def dispatch(*args, **kwargs):
        dispatched.append(args[0])
        return {"saved": True}

    monkeypatch.setattr("backend.workflow.nodes.action._dispatch_action", dispatch)
    stream = asyncio.run(_drain(WorkflowExecutor(repo).start(
        definition.id,
        initial_event={"eventId": "replay-event", "actionExecutionAllowed": True},
    )))
    run_id = _run_id(stream)
    run = repo.get_run(run_id)
    assert dispatched == []
    assert run.status == WorkflowRunStatus.FAILED
    assert run.state["currentEvent"]["actionExecutionAllowed"] is False
    assert run.state["currentEvent"]["runKind"] == "replay"


def test_approval_transaction_rolls_back_decision_when_run_checkpoint_fails():
    repo = SQLiteWorkflowRepository()
    pending = {
        "approvalId": "approval-atomic",
        "nodeId": "approval",
        "createdAt": "2026-10-03T00:00:00Z",
        "proposedActions": [{"actionType": "notify_wechat"}],
    }
    run = WorkflowRun(
        run_id="approval-atomic-run",
        definition_id="approval-definition",
        status=WorkflowRunStatus.AWAITING_APPROVAL,
        current_node_id="approval",
        state={"status": "awaiting_approval", "pendingApproval": pending},
    )
    repo.save_run(run)
    approval = WorkflowApproval(
        approval_id="approval-atomic",
        run_id=run.run_id,
        node_id="approval",
        proposed_actions=pending["proposedActions"],
    )
    repo.save_approval(approval)

    conn = db_tools.get_connection()
    try:
        conn.execute("""
            CREATE TRIGGER fail_approval_checkpoint
            BEFORE UPDATE ON workflow_runs
            WHEN NEW.run_id='approval-atomic-run'
            BEGIN SELECT RAISE(ABORT, 'injected checkpoint failure'); END
        """)
        conn.commit()
    finally:
        conn.close()

    target = WorkflowRun(
        **{
            **run.__dict__,
            "status": WorkflowRunStatus.PENDING,
            "state": {"status": "pending", "pendingApproval": None},
        }
    )
    decided = WorkflowApproval(
        approval_id=approval.approval_id,
        run_id=approval.run_id,
        node_id=approval.node_id,
        proposed_actions=approval.proposed_actions,
        decision=ApprovalDecision.APPROVED,
        reviewer="operator",
        decided_at="2026-10-03T00:01:00Z",
    )
    with pytest.raises(sqlite3.IntegrityError, match="injected checkpoint failure"):
        repo.decide_approval_and_transition(decided, target)

    assert repo.get_approval(approval.approval_id).decision.value == "pending"
    assert repo.get_run(run.run_id).status == WorkflowRunStatus.AWAITING_APPROVAL
    assert repo.list_events(run.run_id) == []


def test_retry_expected_status_cas_does_not_overwrite_completed(monkeypatch):
    repo = SQLiteWorkflowRepository()
    definition = WorkflowDefinition(
        id="retry-race-definition",
        name="retry race",
        status=DefinitionStatus.ACTIVE,
        nodes=[NodeConfig("action", NodeType.ACTION)],
        entry_node_id="action",
    )
    repo.save_definition(definition)
    version = workflow_api.DefinitionManager(repo).create_version(definition, "race")
    failed = WorkflowRun(
        run_id="retry-race-run",
        definition_id=definition.id,
        version=version.version,
        status=WorkflowRunStatus.FAILED,
        current_node_id="action",
        state={
            "workflowRunId": "retry-race-run",
            "workflowDefinitionId": definition.id,
            "workflowVersion": version.version,
            "status": "failed",
            "currentNode": "action",
            "errors": [{"nodeId": "action", "error": "failed", "attempt": 1}],
        },
    )
    repo.save_run(failed)
    repo.save_node_run(WorkflowNodeRun(
        node_run_id="retry-race-attempt",
        run_id=failed.run_id,
        node_id="action",
        node_type=NodeType.ACTION,
        status=NodeStatus.FAILED,
        attempt=1,
        error="failed",
    ))
    original_cas = repo.set_run_status_managed

    def complete_then_cas(*args, **kwargs):
        current = repo.get_run(failed.run_id)
        current.status = WorkflowRunStatus.COMPLETED
        current.state = {**current.state, "status": "completed"}
        repo.save_run(current)
        return original_cas(*args, **kwargs)

    monkeypatch.setattr(repo, "set_run_status_managed", complete_then_cas)
    result = asyncio.run(WorkflowExecutor(repo).retry_node(failed.run_id, "action"))
    assert result["errorCode"] == "invalid_status"
    assert repo.get_run(failed.run_id).status == WorkflowRunStatus.COMPLETED


def test_wait_scheduler_status_cas_cannot_resurrect_cancelled_run(monkeypatch):
    from backend.workflow.wait_scheduler import WaitScheduler

    repo = SQLiteWorkflowRepository()
    repo.save_run(WorkflowRun(
        run_id="wait-cancel-race",
        status=WorkflowRunStatus.PAUSED,
        current_node_id="wait",
        state={
            "workflowRunId": "wait-cancel-race",
            "status": "paused",
            "currentNode": "wait",
            "nodeOutputs": {},
        },
    ))
    repo.mark_driver_managed("wait-cancel-race")
    original_cas = SQLiteWorkflowRepository.set_run_status_managed
    injected = {"done": False}

    def cancel_then_cas(self, run_id, *args, **kwargs):
        if run_id == "wait-cancel-race" and not injected["done"]:
            injected["done"] = True
            conn = db_tools.get_connection()
            try:
                conn.execute(
                    "UPDATE workflow_runs SET status='cancelled' WHERE run_id=?",
                    (run_id,),
                )
                conn.commit()
            finally:
                conn.close()
        return original_cas(self, run_id, *args, **kwargs)

    monkeypatch.setattr(
        SQLiteWorkflowRepository,
        "set_run_status_managed",
        cancel_then_cas,
    )
    asyncio.run(WaitScheduler()._resume_waiting_run("wait-cancel-race"))
    assert SQLiteWorkflowRepository().get_run("wait-cancel-race").status == WorkflowRunStatus.CANCELLED


def test_successful_node_and_checkpoint_rollback_together_on_db_error():
    repo = SQLiteWorkflowRepository()
    run = WorkflowRun(
        run_id="node-atomic-run",
        status=WorkflowRunStatus.RUNNING,
        current_node_id="validate",
        state={"status": "running", "currentNode": "validate"},
    )
    repo.save_run(run)
    node_run = WorkflowNodeRun(
        node_run_id="node-atomic-attempt",
        run_id=run.run_id,
        node_id="validate",
        node_type=NodeType.VALIDATE_EVENT,
        status=NodeStatus.RUNNING,
        attempt=1,
    )
    repo.save_node_run(node_run)
    node_run.status = NodeStatus.SUCCEEDED
    checkpoint = repo.get_run(run.run_id)
    checkpoint.state = {
        "status": "running",
        "currentNode": "validate",
        "completedSteps": ["validate"],
        "nodeOutputs": {"validate": {"validated": True}},
    }

    conn = db_tools.get_connection()
    try:
        conn.execute("""
            CREATE TRIGGER fail_node_checkpoint
            BEFORE UPDATE ON workflow_runs
            WHEN NEW.run_id='node-atomic-run'
            BEGIN SELECT RAISE(ABORT, 'injected node checkpoint failure'); END
        """)
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(sqlite3.IntegrityError, match="injected node checkpoint failure"):
        repo.finalize_node_run(node_run, checkpoint_run=checkpoint)
    stored_attempt = repo.get_node_runs(run.run_id)[0]
    assert stored_attempt.status == NodeStatus.RUNNING
    assert repo.get_run(run.run_id).state.get("completedSteps") is None


def test_concurrent_workflow_events_receive_unique_monotonic_sequences():
    repo = SQLiteWorkflowRepository()
    repo.save_run(WorkflowRun(run_id="event-sequence-run"))

    def append(index: int):
        return SQLiteWorkflowRepository().append_event(
            "event-sequence-run",
            "concurrent_event",
            payload={"index": index},
        ).sequence

    with ThreadPoolExecutor(max_workers=8) as pool:
        allocated = list(pool.map(append, range(24)))
    assert sorted(allocated) == list(range(24))
    events = repo.list_events("event-sequence-run")
    assert [item.sequence for item in events] == list(range(24))
    assert len({item.event_id for item in events}) == 24
