"""Phase 21.3 reliable Action Execution acceptance and race tests.

These tests use a real isolated SQLite database and canonical Event rows.  No
external network is contacted: notification delivery is exercised through the
controlled provider abstraction, including ambiguous timeout outcomes.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import backend.config as config
import backend.agent.collaboration.db_repository as collab_db
import backend.chat.chat_db as chat_db
import backend.tools.db_tools as db_tools
import backend.workflow.api as workflow_api
from backend.event_ingestion import ingest_event, project_event_relationships
from backend.planning.budget import new_lineage, set_lineage
from backend.workflow.action_execution import (
    ActionExecutionContext,
    ActionExecutorResult,
    LocalNotificationProvider,
    NotificationRequest,
    WebhookNotificationProvider,
    reconcile_action_execution,
    request_action_execution_retry,
    reset_notification_provider,
    set_notification_provider,
)
from backend.workflow.executor import WorkflowExecutor
from backend.workflow.definition import DefinitionManager
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
    WorkflowNodeRun,
    WorkflowRun,
    WorkflowRunStatus,
    compute_action_idempotency_key,
    compute_legacy_action_idempotency_key,
    generate_action_id,
    generate_node_run_id,
)
from backend.workflow.nodes.action import execute_action
from backend.workflow.repository import SQLiteWorkflowRepository, init_workflow_tables
from backend.workflow.recovery import detect_unknown_outcome
from backend.workflow.state import TrafficWorkflowState


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    path = str(tmp_path / "phase21_action_execution.db")
    monkeypatch.setattr(config, "DB_PATH", path)
    monkeypatch.setattr(db_tools, "DB_PATH", path)
    monkeypatch.setattr(collab_db, "DB_PATH", path)
    monkeypatch.setattr(chat_db, "DB_PATH", path)
    chat_db.reset_initialized()
    db_tools.init_db()
    chat_db.init_chat_tables()
    collab_db.init_collaboration_tables()
    init_workflow_tables()
    reset_notification_provider()
    yield path
    reset_notification_provider()


def _event_payload() -> dict:
    return {
        "eventType": "congestion",
        "roadName": "钱塘可靠执行测试路",
        "direction": "东向西",
        "avgSpeed": 8,
        "queueLength": 720,
        "duration": 960,
        "confidence": 0.99,
        "regionId": "qt-action-test",
    }


def _canonical_event(source_id: str = "action-event") -> dict:
    event_id = ingest_event(
        source="phase21-action-test",
        source_event_id=source_id,
        event=_event_payload(),
        occurred_at="2026-10-03T08:00:00+08:00",
    )["eventId"]
    return db_tools.get_event_by_id(event_id)


async def _drain(generator):
    result = []
    async for item in generator:
        result.append(item)
    return result


def _run_id(events: list[str]) -> str:
    for item in events:
        if item.startswith("event: workflow_started"):
            return json.loads(item.split("data: ", 1)[1])["runId"]
    raise AssertionError("workflow_started event missing")


class CountingLocalProvider(LocalNotificationProvider):
    def __init__(self, outcome: str = ""):
        super().__init__(outcome)
        self.send_calls = 0
        self.reconcile_calls = 0

    async def send(self, request: NotificationRequest, repository):
        self.send_calls += 1
        return await super().send(request, repository)

    async def reconcile(self, request, repository, external_reference):
        self.reconcile_calls += 1
        return await super().reconcile(
            request, repository, external_reference
        )


class StillUnknownProvider:
    name = "still-unknown"
    reconciliation_supported = True

    def __init__(self):
        self.send_calls = 0
        self.reconcile_calls = 0

    async def send(self, request, repository):
        self.send_calls += 1
        return ActionExecutorResult(
            status=ActionStatus.UNKNOWN,
            message="provider acknowledgement lost",
            reconciliation_supported=True,
        )

    async def reconcile(self, request, repository, external_reference):
        self.reconcile_calls += 1
        return ActionExecutorResult(
            status=ActionStatus.UNKNOWN,
            message="provider still processing",
            reconciliation_supported=True,
        )


class UnsupportedProvider:
    name = "unsupported"
    reconciliation_supported = False

    def __init__(self):
        self.send_calls = 0
        self.reconcile_calls = 0

    async def send(self, request, repository):
        self.send_calls += 1
        return ActionExecutorResult(
            status=ActionStatus.UNKNOWN,
            message="request may have been delivered",
            reconciliation_supported=False,
            reconciliation_message="reconciliation unsupported",
        )

    async def reconcile(self, request, repository, external_reference):
        self.reconcile_calls += 1
        raise AssertionError("unsupported provider must never be queried")


class RaiseAfterDeliveryProvider(CountingLocalProvider):
    def __init__(self, error_message: str = "connection broke after provider accepted request"):
        super().__init__("")
        self.error_message = error_message

    async def send(self, request, repository):
        self.send_calls += 1
        await LocalNotificationProvider.send(self, request, repository)
        raise RuntimeError(self.error_message)


class SlowProvider(CountingLocalProvider):
    def __init__(self):
        super().__init__("")

    async def send(self, request, repository):
        self.send_calls += 1
        await asyncio.sleep(0.1)
        return await LocalNotificationProvider.send(self, request, repository)


def _full_definition(definition_id: str = "phase21-action-e2e") -> WorkflowDefinition:
    return WorkflowDefinition(
        id=definition_id,
        name="Phase 21.3 reliable action acceptance",
        status=DefinitionStatus.ACTIVE,
        nodes=[
            NodeConfig("trigger", NodeType.TRIGGER, next_nodes=["approval"]),
            NodeConfig(
                "approval",
                NodeType.HUMAN_APPROVAL,
                next_nodes=["dispatch"],
                config={
                    "action_types": ["create_dispatch_task", "send_notification"],
                },
            ),
            NodeConfig(
                "dispatch",
                NodeType.ACTION,
                next_nodes=["notify"],
                config={
                    "action_type": "create_dispatch_task",
                    "semantic_action_version": "dispatch-v1",
                    "action_params": {
                        "assignee": "钱塘交警",
                        "target": "钱塘可靠执行测试路",
                        "instruction": "到场疏导并建立处置任务",
                    },
                },
            ),
            NodeConfig(
                "notify",
                NodeType.ACTION,
                next_nodes=["status"],
                config={
                    "action_type": "send_notification",
                    "semantic_action_version": "notify-v1",
                    "action_params": {
                        "channel": "local",
                        "target": "值班群",
                        "message": "钱塘可靠执行测试路已创建处置任务",
                    },
                },
            ),
            NodeConfig(
                "status",
                NodeType.ACTION,
                next_nodes=["close"],
                config={
                    "action_type": "update_event_status",
                    "semantic_action_version": "status-v1",
                    "action_params": {"status": "已处置"},
                },
            ),
            NodeConfig("close", NodeType.CLOSE),
        ],
        entry_node_id="trigger",
    )


def _seed_direct_action(
    *,
    action_type: str,
    params: dict,
    source_id: str,
    run_id: str,
    semantic_version: str = "v1",
    durable_approval: bool = True,
    definition_metadata: dict | None = None,
) -> tuple[SQLiteWorkflowRepository, TrafficWorkflowState, NodeConfig, dict]:
    repo = SQLiteWorkflowRepository()
    event = _canonical_event(source_id)
    node = NodeConfig(
        "action",
        NodeType.ACTION,
        next_nodes=["close"],
        config={
            "action_type": action_type,
            "semantic_action_version": semantic_version,
            "action_params": params,
        },
    )
    definition = WorkflowDefinition(
        id=f"definition-{run_id}",
        name="direct reliable action",
        status=DefinitionStatus.ACTIVE,
        nodes=[
            NodeConfig("trigger", NodeType.TRIGGER, next_nodes=["action"]),
            node,
            NodeConfig("close", NodeType.CLOSE),
        ],
        entry_node_id="trigger",
        metadata=dict(definition_metadata or {}),
    )
    repo.save_definition(definition)
    state = TrafficWorkflowState(
        workflow_run_id=run_id,
        workflow_definition_id=definition.id,
        current_event=event,
        original_input=dict(event),
        approved_actions=[{"actionType": action_type}],
        current_node="action",
        status=WorkflowRunStatus.RUNNING,
    )
    state_dict = state.to_dict()
    set_lineage(state_dict, new_lineage(run_id))
    repo.save_run(WorkflowRun(
        run_id=run_id,
        definition_id=definition.id,
        status=WorkflowRunStatus.RUNNING,
        current_node_id="action",
        state=state_dict,
    ))
    if durable_approval:
        repo.save_approval(WorkflowApproval(
            approval_id=f"approval-{run_id}",
            run_id=run_id,
            node_id="approval",
            proposed_actions=[{"actionType": action_type}],
            decision=ApprovalDecision.APPROVED,
            reviewer="phase21-test",
            decided_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        ))
    return repo, TrafficWorkflowState.from_dict(state_dict), node, event


def _pause_run(repo: SQLiteWorkflowRepository, run_id: str) -> None:
    run = repo.get_run(run_id)
    run.status = WorkflowRunStatus.PAUSED
    run.state["status"] = WorkflowRunStatus.PAUSED.value
    repo.save_run(run)


def _fail_run(repo: SQLiteWorkflowRepository, run_id: str) -> None:
    run = repo.get_run(run_id)
    run.status = WorkflowRunStatus.FAILED
    run.state["status"] = WorkflowRunStatus.FAILED.value
    repo.save_run(run)


def test_final_acceptance_timeout_restart_reconcile_then_complete():
    """Event → approval → dispatch → ambiguous delivery → restart → close."""
    repo = SQLiteWorkflowRepository()
    definition = _full_definition()
    repo.save_definition(definition)
    event = _canonical_event("final-acceptance")
    provider = CountingLocalProvider("timeout_after_delivery")
    set_notification_provider(provider)

    executor = WorkflowExecutor(repo)
    started = asyncio.run(_drain(executor.start(
        definition.id,
        initial_event=event,
        triggered_by="phase21-acceptance",
    )))
    run_id = _run_id(started)
    waiting = repo.get_run(run_id)
    assert waiting.status == WorkflowRunStatus.AWAITING_APPROVAL
    approval_id = waiting.state["pendingApproval"]["approvalId"]

    approved = asyncio.run(executor.approve(
        run_id, approval_id=approval_id, reviewer="值班长"
    ))
    assert approved["decision"] == "approved"
    resumed = asyncio.run(_drain(executor.resume(run_id)))
    assert any("action_unknown" in item for item in resumed)

    paused = repo.get_run(run_id)
    records = repo.list_action_records(run_id)
    assert paused.status == WorkflowRunStatus.PAUSED
    assert [(r.action_type, r.status.value) for r in records] == [
        ("create_dispatch_task", "succeeded"),
        ("send_notification", "unknown"),
    ]
    notification = records[-1]
    assert provider.send_calls == 1
    assert notification.external_reference.startswith("local-notify-")
    assert "status" not in paused.state["completedSteps"]
    assert asyncio.run(executor.retry_node(run_id, "notify"))["errorCode"] == "reconcile_required"

    # Simulate a fresh process.  The default controlled provider reconciles
    # from the durable receipt and never sends the notification again.
    reset_notification_provider()
    restarted_repo = SQLiteWorkflowRepository()
    reconciled = asyncio.run(reconcile_action_execution(
        restarted_repo, run_id, notification.action_id
    ))
    assert reconciled["status"] == "succeeded"
    reconciled_run = restarted_repo.get_run(run_id)
    assert reconciled_run.status == WorkflowRunStatus.PENDING
    reconciled_output = reconciled_run.state["nodeOutputs"]["notify"]
    assert reconciled_output["actionExecutionId"] == notification.action_id
    assert reconciled_output["status"] == "succeeded"
    notify_node_run = [
        item
        for item in restarted_repo.get_node_runs(run_id)
        if item.node_id == "notify"
    ][-1]
    assert notify_node_run.status == NodeStatus.SUCCEEDED
    assert notify_node_run.output_snapshot == reconciled_output

    claim = restarted_repo.claim_driver_run(
        run_id, "phase21-restarted-worker", "2099-01-01T00:00:00Z"
    )
    assert claim["claimed"] is True
    restarted = WorkflowExecutor(restarted_repo)
    restarted.set_driver_context("phase21-restarted-worker", claim["generation"])
    asyncio.run(_drain(restarted.execute_created_run(run_id)))

    final_run = restarted_repo.get_run(run_id)
    final_records = restarted_repo.list_action_records(run_id)
    assert final_run.status == WorkflowRunStatus.COMPLETED
    assert [r.status for r in final_records] == [
        ActionStatus.SUCCEEDED,
        ActionStatus.SUCCEEDED,
        ActionStatus.SUCCEEDED,
    ]
    assert db_tools.get_event_by_id(event["eventId"])["status"] == "已处置"
    assert provider.send_calls == 1
    with sqlite3.connect(config.DB_PATH) as conn:
        assert conn.execute("SELECT COUNT(*) FROM workflow_dispatch_tasks").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM workflow_notification_receipts").fetchone()[0] == 1

    duplicate_event = ingest_event(
        source="phase21-action-test",
        source_event_id="final-acceptance",
        event=_event_payload(),
        occurred_at="2026-10-03T08:00:00+08:00",
    )
    assert duplicate_event["outcome"] == "duplicate"
    assert duplicate_event["eventId"] == event["eventId"]
    duplicate_action = asyncio.run(execute_action(
        TrafficWorkflowState.from_dict(final_run.state),
        definition.get_node("notify"),
        repository=restarted_repo,
    ))
    assert duplicate_action["status"] == "skipped"
    assert duplicate_action["actionExecutionId"] == notification.action_id
    assert provider.send_calls == 1


def test_real_event_agent_plan_reliable_action_acceptance(monkeypatch):
    """One eventId traces Agent → Plan → approvals → UNKNOWN → reconcile."""
    import backend.app as app_module
    import backend.agent.collaboration.orchestrator as orchestrator_module
    import backend.planning.api as planning_api
    import backend.workflow.nodes.action as action_module

    monkeypatch.setattr(app_module, "LLM_ENABLED", False)
    monkeypatch.setattr(orchestrator_module, "LLM_ENABLED", False)

    async def fake_agent_call(agent_name: str, context: dict) -> dict:
        return {
            "agentName": agent_name,
            "findings": ["已完成基于真实 Event 的结构化研判"],
            "confidence": 0.9,
            "suggestion": "创建派单、通知值班群并闭环事件",
            "urgency": "low",
            "proposed_actions": [
                {
                    "actionType": "create_dispatch_task",
                    "params": {
                        "assignee": "钱塘交警",
                        "target": "钱塘可靠执行低风险路",
                        "instruction": "到场核验并建立处置任务",
                    },
                },
                {
                    "actionType": "send_notification",
                    "params": {
                        "channel": "local",
                        "target": "值班群",
                        "message": "钱塘可靠执行事件已派单",
                    },
                },
                {
                    "actionType": "update_event_status",
                    "params": {"status": "已处置"},
                },
            ],
        }

    monkeypatch.setattr(
        "backend.agent.collaboration.executor._call_agent_function",
        fake_agent_call,
    )

    # Guard against an unexpected legacy notification candidate in this
    # low-risk plan: acceptance must never contact a real external channel.
    original_dispatch = action_module._dispatch_action

    async def controlled_legacy_dispatch(action_type, params, state):
        if action_type in {"notify_wechat", "notify_dingtalk"}:
            return {"sent": True, "channel": "acceptance-local"}
        return await original_dispatch(action_type, params, state)

    monkeypatch.setattr(action_module, "_dispatch_action", controlled_legacy_dispatch)

    repo = SQLiteWorkflowRepository()
    monkeypatch.setattr(planning_api, "_repo", repo)
    monkeypatch.setattr(workflow_api, "_repo", repo)
    monkeypatch.setattr(workflow_api, "_def_manager", DefinitionManager(repo))
    client = TestClient(app_module.app)

    ingest_body = {
        "source": "phase21-action-acceptance",
        "sourceEventId": "agent-plan-reliable-action",
        "occurredAt": "2026-10-03T08:30:00+08:00",
        "event": {
            "eventType": "congestion",
            "roadName": "钱塘可靠执行低风险路",
            "direction": "东向西",
            "avgSpeed": 35,
            "queueLength": 40,
            "duration": 120,
            "confidence": 0.95,
            "regionId": "qt-action-test",
        },
    }
    ingested = client.post("/events/ingest", json=ingest_body)
    assert ingested.status_code == 200
    event_id = ingested.json()["eventId"]

    agent_response = client.post("/agent/routed_analyze/stream", json={
        "eventId": event_id,
        "content": "请研判并形成可靠执行计划",
        "contextPolicy": "fresh_event",
    })
    assert agent_response.status_code == 200
    assert '"status": "completed"' in agent_response.text
    agent_runs = collab_db.SQLiteCollaborationRepository().list_runs_by_event_id(
        event_id, limit=10, offset=0
    )
    assert len(agent_runs) == 1
    agent_run_id = agent_runs[0]["run_id"]
    session_id = agent_runs[0]["session_id"]

    planned = client.post("/planning/plans/from-agent", json={
        "eventId": event_id,
        "sessionId": session_id,
        "collaborationRunId": agent_run_id,
        "plannerMode": "deterministic",
    })
    assert planned.status_code == 200, planned.text
    plan_body = planned.json()
    plan_id = plan_body["planId"]
    accepted_types = {
        item["actionType"]
        for item in plan_body["agentRecommendationAudit"]["accepted"]
    }
    assert {
        "create_dispatch_task",
        "send_notification",
        "update_event_status",
    }.issubset(accepted_types)
    plan_action_types = {
        step.get("actionType")
        for step in plan_body["plan"]["steps"]
        if step.get("stepType") == "action"
    }
    assert accepted_types.issubset(plan_action_types)

    definition = repo.get_definition(plan_id)
    assert definition is not None
    event = db_tools.get_event_by_id(event_id)
    provider = CountingLocalProvider("timeout_after_delivery")
    set_notification_provider(provider)
    executor = WorkflowExecutor(repo)
    started = asyncio.run(_drain(executor.start(
        definition.id,
        session_id=session_id,
        initial_event=event,
        triggered_by="phase21-agent-plan-acceptance",
    )))
    run_id = _run_id(started)

    approved_types = []
    stream_events = list(started)
    for _ in range(6):
        durable = repo.get_run(run_id)
        if durable.status != WorkflowRunStatus.AWAITING_APPROVAL:
            break
        pending = durable.state["pendingApproval"]
        approved_types.extend(
            item.get("actionType")
            for item in pending.get("proposedActions", [])
            if isinstance(item, dict) and item.get("actionType")
        )
        decision = asyncio.run(executor.approve(
            run_id,
            approval_id=pending["approvalId"],
            reviewer="值班长",
        ))
        assert decision["decision"] == "approved"
        stream_events.extend(asyncio.run(_drain(executor.resume(run_id))))
        if repo.get_run(run_id).status == WorkflowRunStatus.PAUSED:
            break

    assert "create_dispatch_task" in approved_types
    assert "send_notification" in approved_types
    paused = repo.get_run(run_id)
    assert paused.status == WorkflowRunStatus.PAUSED
    assert any("action_unknown" in item for item in stream_events)
    reliable_before = {
        item.action_type: item for item in repo.list_action_records(run_id)
        if item.action_type in {
            "create_dispatch_task",
            "send_notification",
            "update_event_status",
        }
    }
    assert reliable_before["create_dispatch_task"].status == ActionStatus.SUCCEEDED
    notification = reliable_before["send_notification"]
    assert notification.status == ActionStatus.UNKNOWN
    assert "update_event_status" not in reliable_before
    assert provider.send_calls == 1

    # Fresh provider/runtime reconciles from the durable receipt; execute is
    # never called a second time for the ambiguous notification.
    reset_notification_provider()
    restarted_repo = SQLiteWorkflowRepository()
    reconciled = asyncio.run(reconcile_action_execution(
        restarted_repo, run_id, notification.action_id
    ))
    assert reconciled["status"] == "succeeded"
    claim = restarted_repo.claim_driver_run(
        run_id, "phase21-agent-plan-worker", "2099-01-01T00:00:00Z"
    )
    assert claim["claimed"] is True
    restarted = WorkflowExecutor(restarted_repo)
    restarted.set_driver_context(
        "phase21-agent-plan-worker", claim["generation"]
    )
    asyncio.run(_drain(restarted.execute_created_run(run_id)))

    final_run = restarted_repo.get_run(run_id)
    assert final_run.status == WorkflowRunStatus.COMPLETED
    assert db_tools.get_event_by_id(event_id)["status"] == "已处置"
    reliable_after = {
        item.action_type: item
        for item in restarted_repo.list_action_records(run_id)
        if item.action_type in {
            "create_dispatch_task",
            "send_notification",
            "update_event_status",
        }
    }
    assert set(reliable_after) == {
        "create_dispatch_task",
        "send_notification",
        "update_event_status",
    }
    assert all(
        item.status == ActionStatus.SUCCEEDED
        for item in reliable_after.values()
    )
    assert len(restarted_repo.list_action_attempts(notification.action_id)) == 1

    relationships = project_event_relationships(event_id)
    assert relationships["agentRuns"]["total"] == 1
    assert relationships["agentRuns"]["items"][0]["agentRunId"] == agent_run_id
    assert relationships["plans"]["total"] == 1
    assert relationships["plans"]["items"][0]["planId"] == plan_id
    assert relationships["workflowRuns"]["total"] == 1
    assert relationships["workflowRuns"]["items"][0]["workflowRunId"] == run_id

    duplicate_event = client.post("/events/ingest", json=ingest_body)
    assert duplicate_event.status_code == 200
    assert duplicate_event.json()["outcome"] == "duplicate"
    duplicate_notification = asyncio.run(execute_action(
        TrafficWorkflowState.from_dict(final_run.state),
        definition.get_node("action_send_notification"),
        repository=restarted_repo,
    ))
    assert duplicate_notification["status"] == "skipped"
    assert duplicate_notification["actionExecutionId"] == notification.action_id
    assert provider.send_calls == 1
    with sqlite3.connect(config.DB_PATH) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM workflow_dispatch_tasks"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM workflow_notification_receipts"
        ).fetchone()[0] == 1


def test_real_internal_actions_and_semantic_version_idempotency():
    repo, state, node, event = _seed_direct_action(
        action_type="create_dispatch_task",
        params={"instruction": "派员到场", "target": "测试路"},
        source_id="dispatch-idempotency",
        run_id="run-dispatch-idempotency",
        semantic_version="dispatch-v3",
    )
    first = asyncio.run(execute_action(state, node, repository=repo))
    exhausted = repo.get_run(state.workflow_run_id)
    exhausted.state["executionLineage"]["budgetLimits"]["maxToolCalls"] = 1
    exhausted.state["executionLineage"]["budgetUsage"]["toolCallsUsed"] = 1
    repo.save_run(exhausted)
    second = asyncio.run(execute_action(state, node, repository=repo))
    assert first["status"] == "succeeded"
    assert second["status"] == "skipped"
    assert first["actionExecutionId"] == second["actionExecutionId"]
    record = repo.get_action_record(first["actionExecutionId"])
    assert record.semantic_action_version == "dispatch-v3"
    assert record.event_id == event["eventId"]
    assert record.idempotency_key == compute_action_idempotency_key(
        state.workflow_run_id, node.node_id, "create_dispatch_task", "dispatch-v3"
    )
    with sqlite3.connect(config.DB_PATH) as conn:
        assert conn.execute("SELECT COUNT(*) FROM workflow_dispatch_tasks").fetchone()[0] == 1

    update_repo, update_state, update_node, update_event = _seed_direct_action(
        action_type="update_event_status",
        params={"status": "待派单"},
        source_id="status-action",
        run_id="run-status-action",
        durable_approval=False,
    )
    updated = asyncio.run(execute_action(
        update_state, update_node, repository=update_repo
    ))
    assert updated["status"] == "succeeded"
    assert db_tools.get_event_by_id(update_event["eventId"])["status"] == "待派单"


def test_upgrade_recognizes_legacy_v1_idempotency_key_without_redispatch():
    provider = CountingLocalProvider()
    set_notification_provider(provider)
    repo, state, node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "升级幂等保护"},
        source_id="legacy-idempotency-upgrade",
        run_id="run-legacy-idempotency-upgrade",
    )
    legacy_key = compute_legacy_action_idempotency_key(
        state.workflow_run_id,
        node.node_id,
        "send_notification",
    )
    legacy = WorkflowActionRecord(
        action_id="legacy-v1-action",
        run_id=state.workflow_run_id,
        node_id=node.node_id,
        event_id="",  # Pre-21.3 rows did not have this column/value.
        action_type="send_notification",
        idempotency_key=legacy_key,
        semantic_action_version="v1",
        params=node.config["action_params"],
        result={"delivered": True},
        status=ActionStatus.SUCCEEDED,
        attempt=1,
        completed_at="2026-10-03T00:00:01Z",
    )
    repo.save_action_record(legacy)

    duplicate = asyncio.run(execute_action(state, node, repository=repo))
    assert duplicate["status"] == "skipped"
    assert duplicate["actionExecutionId"] == legacy.action_id
    assert provider.send_calls == 0
    records = repo.list_action_records(state.workflow_run_id)
    assert len(records) == 1
    assert records[0].idempotency_key == legacy_key
    assert records[0].event_id == state.current_event["eventId"]


def test_webhook_provider_forwards_idempotency_key_without_persisting_credentials(
    monkeypatch,
):
    token = "phase21-webhook-token-secret"
    webhook_secret = "phase21-webhook-url-secret"
    monkeypatch.setenv(
        "TRAFFICMIND_NOTIFICATION_WEBHOOK_URL",
        f"https://notify.invalid/hook?key={webhook_secret}",
    )
    monkeypatch.setenv("TRAFFICMIND_NOTIFICATION_TOKEN", token)
    monkeypatch.delenv("TRAFFICMIND_NOTIFICATION_RECONCILE_URL", raising=False)
    captured = {}

    class FakeResponse:
        status = 202
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return b'{"externalReference":"provider-receipt-1"}'

    def fake_urlopen(request, timeout):
        captured["request"] = request
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    set_notification_provider(WebhookNotificationProvider())
    repo, state, node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "Webhook 幂等测试"},
        source_id="webhook-idempotency",
        run_id="run-webhook-idempotency",
    )
    result = asyncio.run(execute_action(state, node, repository=repo))
    assert result["status"] == "succeeded"
    record = repo.get_action_record(result["actionExecutionId"])
    request = captured["request"]
    assert request.get_header("Idempotency-key") == record.idempotency_key
    assert request.get_header("Authorization") == f"Bearer {token}"
    with open(config.DB_PATH, "rb") as database_file:
        durable_bytes = database_file.read()
    assert token.encode() not in durable_bytes
    assert webhook_secret.encode() not in durable_bytes


def test_webhook_uncertain_conflict_and_eventual_not_found_never_enable_retry(
    monkeypatch,
):
    provider = WebhookNotificationProvider()
    request = NotificationRequest(
        idempotency_key="webhook-conservative-key",
        target="值班群",
        channel="webhook",
        message="保守对账",
    )
    monkeypatch.setenv(
        "TRAFFICMIND_NOTIFICATION_WEBHOOK_URL", "https://notify.invalid/send"
    )
    monkeypatch.setenv(
        "TRAFFICMIND_NOTIFICATION_RECONCILE_URL", "https://notify.invalid/query"
    )

    def conflict_urlopen(req, timeout):
        raise urllib.error.HTTPError(req.full_url, 409, "conflict", None, None)

    monkeypatch.setattr("urllib.request.urlopen", conflict_urlopen)
    conflict = asyncio.run(provider.send(request, SQLiteWorkflowRepository()))
    assert conflict.status == ActionStatus.UNKNOWN
    assert conflict.retryable is False

    class FakeResponse:
        status = 200
        headers = {}

        def __init__(self, payload: bytes):
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return self.payload

    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda req, timeout: FakeResponse(b'{"status":"not_found"}'),
    )
    not_found = asyncio.run(provider.reconcile(
        request, SQLiteWorkflowRepository(), "provider-ref"
    ))
    assert not_found.status == ActionStatus.UNKNOWN
    assert not_found.retryable is False

    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda req, timeout: FakeResponse(b'{"status":"not_executed"}'),
    )
    confirmed_absent = asyncio.run(provider.reconcile(
        request, SQLiteWorkflowRepository(), "provider-ref"
    ))
    assert confirmed_absent.status == ActionStatus.FAILED
    assert confirmed_absent.retryable is True


def test_failed_action_requires_explicit_retry_and_success_is_terminal():
    provider = CountingLocalProvider("failed")
    set_notification_provider(provider)
    repo, state, node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "重试测试"},
        source_id="failed-retry",
        run_id="run-failed-retry",
    )
    failed = asyncio.run(execute_action(state, node, repository=repo))
    assert failed["status"] == "failed"
    assert failed["retryable"] is True
    assert provider.send_calls == 1
    _fail_run(repo, state.workflow_run_id)

    scheduled = request_action_execution_retry(
        repo, state.workflow_run_id, failed["actionExecutionId"]
    )
    assert scheduled["scheduled"] is True
    assert scheduled["nextAttempt"] == 2
    provider._outcome = ""
    retry_state = TrafficWorkflowState.from_dict(repo.get_run(state.workflow_run_id).state)
    succeeded = asyncio.run(execute_action(retry_state, node, repository=repo))
    assert succeeded["status"] == "succeeded"
    assert provider.send_calls == 2
    attempts = repo.list_action_attempts(failed["actionExecutionId"])
    assert [(a.attempt, a.status.value) for a in attempts] == [
        (1, "failed"),
        (2, "succeeded"),
    ]
    denied = request_action_execution_retry(
        repo, state.workflow_run_id, failed["actionExecutionId"]
    )
    assert denied["errorCode"] == "invalid_status"


def test_cancel_replay_and_durable_approval_are_final_execution_fences():
    provider = CountingLocalProvider()
    set_notification_provider(provider)

    repo, state, node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "审批伪造测试"},
        source_id="forged-approval",
        run_id="run-forged-approval",
        durable_approval=False,
    )
    blocked = asyncio.run(execute_action(state, node, repository=repo))
    assert blocked["status"] == "blocked"
    assert "durable approval" in blocked["reason"]
    assert provider.send_calls == 0
    blocked_audit = [
        event for event in repo.list_events(state.workflow_run_id)
        if event.event_type == "action_blocked"
    ][-1]
    assert blocked_audit.payload["workflowRunId"] == state.workflow_run_id
    assert blocked_audit.payload["eventId"] == state.current_event["eventId"]
    assert blocked_audit.payload["nodeId"] == node.node_id
    assert blocked_audit.payload["actionExecutionId"]
    assert blocked_audit.payload["attempt"] == 0

    valid_repo, forged_state, valid_node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "原始批准内容"},
        source_id="forged-approved-params",
        run_id="run-forged-approved-params",
    )
    forged_state.approved_actions = [{
        "actionType": "send_notification",
        "params": {"target": "未批准目标", "message": "伪造参数"},
    }]
    forged_params = asyncio.run(execute_action(
        forged_state, valid_node, repository=valid_repo
    ))
    assert forged_params["status"] == "blocked"
    assert "durable Workflow" in forged_params["reason"]
    assert provider.send_calls == 0

    replay_repo, replay_state, replay_node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "replay"},
        source_id="replay-block",
        run_id="run-replay-block",
        definition_metadata={"runKind": "replay", "actionExecutionAllowed": False},
    )
    replay_state.current_event["actionExecutionAllowed"] = False
    replay = asyncio.run(execute_action(
        replay_state, replay_node, repository=replay_repo
    ))
    assert replay["status"] in {"denied", "blocked"}
    assert provider.send_calls == 0

    nested_repo, nested_state, nested_node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "nested replay"},
        source_id="nested-replay-block",
        run_id="run-nested-replay-block",
        definition_metadata={
            "plan": {
                "metadata": {
                    "runKind": "replay",
                    "actionExecutionAllowed": False,
                }
            }
        },
    )
    nested_replay = asyncio.run(execute_action(
        nested_state, nested_node, repository=nested_repo
    ))
    assert nested_replay["status"] == "blocked"
    assert "replay-derived" in nested_replay["reason"]
    assert provider.send_calls == 0

    cancel_repo, cancel_state, cancel_node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "cancel"},
        source_id="cancel-block",
        run_id="run-cancel-block",
    )
    asyncio.run(WorkflowExecutor(cancel_repo).cancel(
        cancel_state.workflow_run_id, "人工接管"
    ))
    cancelled = asyncio.run(execute_action(
        cancel_state, cancel_node, repository=cancel_repo
    ))
    assert cancelled["status"] == "cancelled"
    assert provider.send_calls == 0

    terminal_repo, terminal_state, terminal_node, terminal_event = _seed_direct_action(
        action_type="update_event_status",
        params={"status": "待派单"},
        source_id="terminal-run-block",
        run_id="run-terminal-block",
        durable_approval=False,
    )
    terminal_run = terminal_repo.get_run(terminal_state.workflow_run_id)
    terminal_run.status = WorkflowRunStatus.COMPLETED
    terminal_run.state["status"] = WorkflowRunStatus.COMPLETED.value
    terminal_repo.save_run(terminal_run)
    terminal_blocked = asyncio.run(execute_action(
        terminal_state, terminal_node, repository=terminal_repo
    ))
    assert terminal_blocked["status"] == "blocked"
    assert db_tools.get_event_by_id(terminal_event["eventId"])["status"] == "待研判"
    assert terminal_repo.list_action_records(terminal_state.workflow_run_id) == []


def test_action_authority_is_the_run_pinned_definition_version():
    repo = SQLiteWorkflowRepository()
    event = _canonical_event("pinned-definition")
    pinned_node = NodeConfig(
        "status",
        NodeType.ACTION,
        next_nodes=["close"],
        config={
            "action_type": "update_event_status",
            "semantic_action_version": "status-v1",
            "action_params": {"status": "待派单"},
        },
    )
    definition = WorkflowDefinition(
        id="pinned-definition",
        name="pinned definition",
        status=DefinitionStatus.ACTIVE,
        nodes=[
            NodeConfig("trigger", NodeType.TRIGGER, next_nodes=["status"]),
            pinned_node,
            NodeConfig("close", NodeType.CLOSE),
        ],
        entry_node_id="trigger",
    )
    repo.save_definition(definition)
    version = DefinitionManager(repo).create_version(definition, "pinned")
    state = TrafficWorkflowState(
        workflow_run_id="run-pinned-definition",
        workflow_definition_id=definition.id,
        workflow_version=version.version,
        current_event=event,
        original_input=dict(event),
        current_node="status",
        status=WorkflowRunStatus.RUNNING,
    )
    state_dict = state.to_dict()
    set_lineage(state_dict, new_lineage(state.workflow_run_id))
    repo.save_run(WorkflowRun(
        run_id=state.workflow_run_id,
        definition_id=definition.id,
        version=version.version,
        status=WorkflowRunStatus.RUNNING,
        current_node_id="status",
        state=state_dict,
    ))

    forged_latest_node = NodeConfig(
        "status",
        NodeType.ACTION,
        next_nodes=["close"],
        config={
            "action_type": "update_event_status",
            "semantic_action_version": "status-v1",
            "action_params": {"status": "已归档"},
        },
    )
    repo.save_definition(WorkflowDefinition(
        **{
            **definition.__dict__,
            "nodes": [
                definition.nodes[0],
                forged_latest_node,
                definition.nodes[2],
            ],
        }
    ))

    forged = asyncio.run(execute_action(
        TrafficWorkflowState.from_dict(state_dict),
        forged_latest_node,
        repository=repo,
    ))
    assert forged["status"] == "blocked"
    assert "不可变 Workflow Definition" in forged["reason"]
    assert repo.list_action_records(state.workflow_run_id) == []

    legitimate = asyncio.run(execute_action(
        TrafficWorkflowState.from_dict(state_dict),
        pinned_node,
        repository=repo,
    ))
    assert legitimate["status"] == "succeeded"
    assert db_tools.get_event_by_id(event["eventId"])["status"] == "待派单"


def test_unknown_blocks_retry_and_resume_until_reconciliation():
    provider = CountingLocalProvider("timeout_after_delivery")
    set_notification_provider(provider)
    repo, state, node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "UNKNOWN 不重发"},
        source_id="unknown-fence",
        run_id="run-unknown-fence",
    )
    unknown = asyncio.run(execute_action(state, node, repository=repo))
    _pause_run(repo, state.workflow_run_id)
    assert unknown["status"] == "unknown"
    assert provider.send_calls == 1

    retry = request_action_execution_retry(
        repo, state.workflow_run_id, unknown["actionExecutionId"]
    )
    assert retry["errorCode"] == "reconcile_required"
    resume_events = asyncio.run(_drain(
        WorkflowExecutor(repo).resume(state.workflow_run_id)
    ))
    assert any("必须先 reconciliation" in item for item in resume_events)
    assert provider.send_calls == 1


def test_action_api_uses_404_and_409_for_missing_or_illegal_transitions(
    monkeypatch,
):
    provider = UnsupportedProvider()
    set_notification_provider(provider)
    repo, state, node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "API 状态码约束"},
        source_id="action-api-status",
        run_id="run-action-api-status",
    )
    unknown = asyncio.run(execute_action(state, node, repository=repo))
    _pause_run(repo, state.workflow_run_id)
    assert unknown["status"] == "unknown"

    monkeypatch.setattr(workflow_api, "_repo", repo)
    app = FastAPI()
    app.include_router(workflow_api.router)
    client = TestClient(app)

    missing_run = client.get(
        "/workflow/runs/missing-run/actions/missing-action"
    )
    assert missing_run.status_code == 404
    missing_action = client.get(
        f"/workflow/runs/{state.workflow_run_id}/actions/missing-action"
    )
    assert missing_action.status_code == 404

    retry = client.post(
        f"/workflow/runs/{state.workflow_run_id}/actions/"
        f"{unknown['actionExecutionId']}/retry"
    )
    assert retry.status_code == 409
    assert retry.json()["detail"]["code"] == "reconcile_required"

    reconcile = client.post(
        f"/workflow/runs/{state.workflow_run_id}/actions/"
        f"{unknown['actionExecutionId']}/reconcile"
    )
    assert reconcile.status_code == 409
    assert reconcile.json()["detail"]["code"] == "reconciliation_unsupported"
    assert repo.get_action_record(
        unknown["actionExecutionId"]
    ).status == ActionStatus.UNKNOWN
    assert provider.send_calls == 1
    assert provider.reconcile_calls == 0


def test_provider_exception_after_delivery_is_unknown_and_reconciles_without_resend(
    monkeypatch,
):
    secret = "phase21-provider-token-secret"
    monkeypatch.setenv("TRAFFICMIND_NOTIFICATION_TOKEN", secret)
    provider = RaiseAfterDeliveryProvider(
        f"Authorization: Bearer {secret}; acknowledgement lost"
    )
    set_notification_provider(provider)
    repo, state, node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "异常后对账"},
        source_id="raise-after-delivery",
        run_id="run-raise-after-delivery",
    )
    result = asyncio.run(execute_action(state, node, repository=repo))
    _pause_run(repo, state.workflow_run_id)
    assert result["status"] == "unknown"
    assert result["retryable"] is False
    assert secret not in json.dumps(result, ensure_ascii=False)
    assert secret not in json.dumps(
        repo.get_action_record(result["actionExecutionId"]).to_dict(),
        ensure_ascii=False,
    )
    assert secret not in json.dumps(
        [event.to_dict() for event in repo.list_events(state.workflow_run_id)],
        ensure_ascii=False,
    )
    assert provider.send_calls == 1
    assert request_action_execution_retry(
        repo, state.workflow_run_id, result["actionExecutionId"]
    )["errorCode"] == "reconcile_required"

    reconciled = asyncio.run(reconcile_action_execution(
        repo, state.workflow_run_id, result["actionExecutionId"]
    ))
    assert reconciled["status"] == "succeeded"
    assert provider.send_calls == 1
    assert provider.reconcile_calls == 1


def test_runtime_timeout_projects_durable_unknown_to_paused_workflow():
    provider = SlowProvider()
    set_notification_provider(provider)
    repo = SQLiteWorkflowRepository()
    definition = WorkflowDefinition(
        id="slow-provider-timeout",
        name="slow provider timeout",
        status=DefinitionStatus.ACTIVE,
        nodes=[
            NodeConfig("trigger", NodeType.TRIGGER, next_nodes=["approval"]),
            NodeConfig(
                "approval",
                NodeType.HUMAN_APPROVAL,
                next_nodes=["notify"],
                config={"action_types": ["send_notification"]},
            ),
            NodeConfig(
                "notify",
                NodeType.ACTION,
                next_nodes=["close"],
                timeout_seconds=0.01,
                config={
                    "action_type": "send_notification",
                    "action_params": {
                        "target": "值班群",
                        "message": "节点超时仍需 UNKNOWN",
                    },
                },
            ),
            NodeConfig("close", NodeType.CLOSE),
        ],
        entry_node_id="trigger",
    )
    repo.save_definition(definition)
    event = _canonical_event("slow-provider-timeout")
    executor = WorkflowExecutor(repo)
    started = asyncio.run(_drain(executor.start(
        definition.id, initial_event=event
    )))
    run_id = _run_id(started)
    waiting = repo.get_run(run_id)
    asyncio.run(executor.approve(
        run_id,
        approval_id=waiting.state["pendingApproval"]["approvalId"],
        reviewer="timeout-test",
    ))
    asyncio.run(_drain(executor.resume(run_id)))

    paused = repo.get_run(run_id)
    action = repo.list_action_records(run_id)[0]
    node_run = [n for n in repo.get_node_runs(run_id) if n.node_id == "notify"][0]
    assert paused.status == WorkflowRunStatus.PAUSED
    assert action.status == ActionStatus.UNKNOWN
    assert node_run.status == NodeStatus.PAUSED
    assert "close" not in paused.state["completedSteps"]
    assert provider.send_calls == 1


def test_reconciliation_can_confirm_not_executed_then_enable_retry():
    provider = CountingLocalProvider("timeout_before_delivery")
    set_notification_provider(provider)
    repo, state, node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "未送达确认"},
        source_id="reconcile-not-executed",
        run_id="run-reconcile-not-executed",
    )
    unknown = asyncio.run(execute_action(state, node, repository=repo))
    _pause_run(repo, state.workflow_run_id)
    reconciled = asyncio.run(reconcile_action_execution(
        repo, state.workflow_run_id, unknown["actionExecutionId"]
    ))
    assert reconciled["status"] == "failed"
    action = repo.get_action_record(unknown["actionExecutionId"])
    assert action.status == ActionStatus.FAILED
    assert action.retryable is True
    assert repo.get_run(state.workflow_run_id).status == WorkflowRunStatus.FAILED

    scheduled = request_action_execution_retry(
        repo, state.workflow_run_id, action.action_id
    )
    assert scheduled["scheduled"] is True
    assert provider.send_calls == 1
    provider._outcome = ""
    retried_state = TrafficWorkflowState.from_dict(
        repo.get_run(state.workflow_run_id).state
    )
    retried = asyncio.run(execute_action(
        retried_state, node, repository=repo
    ))
    assert retried["status"] == "succeeded"
    assert provider.send_calls == 2


@pytest.mark.parametrize(
    "provider_factory, expected_error",
    [
        (StillUnknownProvider, None),
        (UnsupportedProvider, "reconciliation_unsupported"),
    ],
)
def test_reconciliation_still_unknown_or_unsupported_never_redelivers(
    provider_factory, expected_error
):
    provider = provider_factory()
    set_notification_provider(provider)
    repo, state, node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "不确定结果"},
        source_id=f"unknown-{provider.name}",
        run_id=f"run-unknown-{provider.name}",
    )
    unknown = asyncio.run(execute_action(state, node, repository=repo))
    _pause_run(repo, state.workflow_run_id)
    result = asyncio.run(reconcile_action_execution(
        repo, state.workflow_run_id, unknown["actionExecutionId"]
    ))
    assert result["status"] == "unknown"
    assert provider.send_calls == 1
    if expected_error:
        assert result["errorCode"] == expected_error
        assert provider.reconcile_calls == 0
    else:
        assert "errorCode" not in result
        assert provider.reconcile_calls == 1
    action = repo.get_action_record(unknown["actionExecutionId"])
    assert action.status == ActionStatus.UNKNOWN
    assert action.last_reconciled_at
    assert repo.get_run(state.workflow_run_id).status == WorkflowRunStatus.PAUSED


def test_restart_after_dispatch_marker_uses_receipt_and_never_executes_again():
    repo, state, node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "崩溃恢复"},
        source_id="crash-marker",
        run_id="run-crash-marker",
    )
    key = compute_action_idempotency_key(
        state.workflow_run_id, node.node_id, "send_notification", "v1"
    )
    record = WorkflowActionRecord(
        action_id=generate_action_id(),
        run_id=state.workflow_run_id,
        node_id=node.node_id,
        event_id=state.current_event["eventId"],
        action_type="send_notification",
        idempotency_key=key,
        params=node.config["action_params"],
        reconciliation_supported=True,
    )
    claimed = repo.claim_action_execution(record)
    assert claimed["claimed"] is True
    running = claimed["record"]
    assert repo.batch_get_action_counts([state.workflow_run_id])[state.workflow_run_id][
        "unknown"
    ] == 0
    receipt_id = f"local-notify-{key}"
    repo.record_local_notification_receipt(
        receipt_id=receipt_id,
        idempotency_key=key,
        channel="local",
        target="值班群",
        message_digest="digest",
    )
    assert repo.mark_running_action_unknown_and_pause(running.action_id)
    assert repo.batch_get_action_counts([state.workflow_run_id])[state.workflow_run_id][
        "unknown"
    ] == 1

    provider = CountingLocalProvider()
    set_notification_provider(provider)
    reconciled = asyncio.run(reconcile_action_execution(
        SQLiteWorkflowRepository(), state.workflow_run_id, running.action_id
    ))
    assert reconciled["status"] == "succeeded"
    assert provider.send_calls == 0
    assert provider.reconcile_calls == 1
    assert repo.get_action_record(running.action_id).external_reference == receipt_id


def test_restart_repairs_unknown_projection_for_non_driver_run():
    repo, state, node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "断连恢复"},
        source_id="unknown-projection-restart",
        run_id="run-unknown-projection-restart",
    )
    record = WorkflowActionRecord(
        action_id=generate_action_id(),
        run_id=state.workflow_run_id,
        node_id=node.node_id,
        event_id=state.current_event["eventId"],
        action_type="send_notification",
        semantic_action_version="v1",
        params=node.config["action_params"],
        reconciliation_supported=True,
    )
    claimed = repo.claim_action_execution(record)["record"]
    # Simulate an older process that committed Action=UNKNOWN, then exited
    # before it could project the node/Run checkpoint.
    with sqlite3.connect(config.DB_PATH) as conn:
        conn.execute(
            "UPDATE workflow_action_records SET status='unknown', error=? WHERE action_id=?",
            ("client disconnected after dispatch", claimed.action_id),
        )
        conn.execute(
            "UPDATE workflow_action_attempts SET status='unknown', error=? WHERE action_id=?",
            ("client disconnected after dispatch", claimed.action_id),
        )
        conn.commit()
    assert repo.get_run(state.workflow_run_id).status == WorkflowRunStatus.RUNNING

    repaired = repo.recover_action_runtime_invariants()
    assert repaired["activeProjections"] == 1
    durable_run = repo.get_run(state.workflow_run_id)
    assert durable_run.status == WorkflowRunStatus.PAUSED
    assert durable_run.state["actionResults"]["send_notification"]["status"] == "unknown"
    assert repo.recover_action_runtime_invariants()["activeProjections"] == 0


def test_cancelled_run_recovers_inflight_action_and_allows_query_only_reconcile():
    from backend.workflow.run_driver import RunDriver

    repo, state, node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "取消后仅对账"},
        source_id="cancelled-inflight-recovery",
        run_id="run-cancelled-inflight-recovery",
    )
    key = compute_action_idempotency_key(
        state.workflow_run_id, node.node_id, "send_notification", "v1"
    )
    record = WorkflowActionRecord(
        action_id=generate_action_id(),
        run_id=state.workflow_run_id,
        node_id=node.node_id,
        event_id=state.current_event["eventId"],
        action_type="send_notification",
        idempotency_key=key,
        params=node.config["action_params"],
        reconciliation_supported=True,
    )
    running = repo.claim_action_execution(record)["record"]
    receipt_id = f"local-notify-{key}"
    repo.record_local_notification_receipt(
        receipt_id=receipt_id,
        idempotency_key=key,
        channel="local",
        target="值班群",
        message_digest="digest",
    )
    # Simulate a pre-fix crash: cancellation committed while the Action marker
    # was still RUNNING, so only startup invariant recovery can repair it.
    cancelled = repo.get_run(state.workflow_run_id)
    cancelled.status = WorkflowRunStatus.CANCELLED
    cancelled.state["status"] = WorkflowRunStatus.CANCELLED.value
    cancelled.state["cancelledAt"] = "2026-10-03T00:00:02Z"
    repo.save_run(cancelled)

    async def restart_driver_once():
        driver = RunDriver(repo, owner_id="phase21-cancel-recovery")
        await driver.start()
        await driver.stop()

    asyncio.run(restart_driver_once())
    assert repo.get_action_record(running.action_id).status == ActionStatus.UNKNOWN
    assert repo.get_run(state.workflow_run_id).status == WorkflowRunStatus.CANCELLED

    provider = CountingLocalProvider()
    set_notification_provider(provider)
    reconciled = asyncio.run(reconcile_action_execution(
        SQLiteWorkflowRepository(), state.workflow_run_id, running.action_id
    ))
    assert reconciled["status"] == "succeeded"
    assert reconciled["runStatus"] == "cancelled"
    assert repo.get_action_record(running.action_id).status == ActionStatus.SUCCEEDED
    assert repo.get_run(state.workflow_run_id).status == WorkflowRunStatus.CANCELLED
    assert provider.send_calls == 0
    assert provider.reconcile_calls == 1


def test_restart_detects_reconcilable_internal_action_as_unknown_outcome():
    repo, state, node, _ = _seed_direct_action(
        action_type="update_event_status",
        params={"status": "待派单"},
        source_id="internal-crash-marker",
        run_id="run-internal-crash-marker",
        durable_approval=False,
    )
    record = WorkflowActionRecord(
        action_id=generate_action_id(),
        run_id=state.workflow_run_id,
        node_id=node.node_id,
        event_id=state.current_event["eventId"],
        action_type="update_event_status",
        semantic_action_version="v1",
        params=node.config["action_params"],
        reconciliation_supported=True,
    )
    claimed = repo.claim_action_execution(record)["record"]
    candidates = detect_unknown_outcome(repo, state.workflow_run_id)
    assert candidates == [{
        "actionId": claimed.action_id,
        "actionType": "update_event_status",
        "nodeId": node.node_id,
        "idempotencyKey": claimed.idempotency_key,
    }]
    assert repo.mark_running_action_unknown_and_pause(claimed.action_id)
    assert repo.get_action_record(claimed.action_id).status == ActionStatus.UNKNOWN


def test_confirmed_action_terminal_survives_node_checkpoint_crash_without_repeat():
    repo, state, node, _ = _seed_direct_action(
        action_type="create_dispatch_task",
        params={"instruction": "崩溃后不可重复", "target": "测试路"},
        source_id="terminal-before-checkpoint",
        run_id="run-terminal-before-checkpoint",
    )
    first = asyncio.run(execute_action(state, node, repository=repo))
    assert first["status"] == "succeeded"
    # The run/node checkpoint is intentionally absent, modelling a process
    # crash after the action terminal commit.  A fresh invocation must skip.
    restarted_state = TrafficWorkflowState.from_dict(
        repo.get_run(state.workflow_run_id).state
    )
    second = asyncio.run(execute_action(
        restarted_state, node, repository=SQLiteWorkflowRepository()
    ))
    assert second["status"] == "skipped"
    with sqlite3.connect(config.DB_PATH) as conn:
        assert conn.execute("SELECT COUNT(*) FROM workflow_dispatch_tasks").fetchone()[0] == 1


def test_concurrent_duplicate_claim_dispatches_exactly_once():
    provider = CountingLocalProvider()
    set_notification_provider(provider)
    repo, _, node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "并发幂等"},
        source_id="concurrent-claim",
        run_id="run-concurrent-claim",
    )

    def invoke():
        durable = SQLiteWorkflowRepository().get_run("run-concurrent-claim")
        local_state = TrafficWorkflowState.from_dict(durable.state)
        return asyncio.run(execute_action(
            local_state, node, repository=SQLiteWorkflowRepository()
        ))

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: invoke(), range(2)))
    assert provider.send_calls == 1
    assert sorted(result["status"] for result in results) in [
        ["in_flight", "succeeded"],
        ["skipped", "succeeded"],
    ]
    assert len(repo.list_action_records("run-concurrent-claim")) == 1


def test_retry_cancel_and_reconcile_retry_races_are_serialized():
    provider = CountingLocalProvider("failed")
    set_notification_provider(provider)
    repo, state, node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "retry/cancel race"},
        source_id="retry-cancel-race",
        run_id="run-retry-cancel-race",
    )
    failed = asyncio.run(execute_action(state, node, repository=repo))
    _fail_run(repo, state.workflow_run_id)

    def retry_failed():
        return request_action_execution_retry(
            SQLiteWorkflowRepository(), state.workflow_run_id,
            failed["actionExecutionId"],
        )

    def cancel_failed():
        return asyncio.run(WorkflowExecutor(SQLiteWorkflowRepository()).cancel(
            state.workflow_run_id, "竞态取消"
        ))

    with ThreadPoolExecutor(max_workers=2) as pool:
        retry_future = pool.submit(retry_failed)
        cancel_future = pool.submit(cancel_failed)
        retry_result = retry_future.result()
        cancel_result = cancel_future.result()
    final_run = repo.get_run(state.workflow_run_id)
    # FAILED is terminal to cancellation.  Depending on which command reads
    # first, retry either atomically schedules PENDING or observes a state that
    # no longer qualifies; neither command can dispatch the action inline.
    assert final_run.status in {
        WorkflowRunStatus.PENDING,
        WorkflowRunStatus.CANCELLED,
    }
    assert (
        cancel_result.get("status") == "cancelled"
        or cancel_result.get("errorCode") == "invalid_status"
    )
    assert (
        retry_result.get("scheduled") is True
        or retry_result.get("errorCode") == "invalid_status"
    )
    assert provider.send_calls == 1

    unknown_provider = CountingLocalProvider("timeout_after_delivery")
    set_notification_provider(unknown_provider)
    unknown_repo, unknown_state, unknown_node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "reconcile/retry race"},
        source_id="reconcile-retry-race",
        run_id="run-reconcile-retry-race",
    )
    unknown = asyncio.run(execute_action(
        unknown_state, unknown_node, repository=unknown_repo
    ))
    _pause_run(unknown_repo, unknown_state.workflow_run_id)

    def reconcile_unknown():
        return asyncio.run(reconcile_action_execution(
            SQLiteWorkflowRepository(), unknown_state.workflow_run_id,
            unknown["actionExecutionId"],
        ))

    def retry_unknown():
        return request_action_execution_retry(
            SQLiteWorkflowRepository(), unknown_state.workflow_run_id,
            unknown["actionExecutionId"],
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        reconcile_future = pool.submit(reconcile_unknown)
        retry_future = pool.submit(retry_unknown)
        reconcile_result = reconcile_future.result()
        retry_result = retry_future.result()
    assert reconcile_result["status"] == "succeeded"
    assert retry_result["errorCode"] in {"reconcile_required", "invalid_status"}
    assert unknown_provider.send_calls == 1
    assert unknown_repo.get_action_record(
        unknown["actionExecutionId"]
    ).status == ActionStatus.SUCCEEDED


def test_action_node_run_checkpoint_finalization_is_atomic():
    repo, state, node, _ = _seed_direct_action(
        action_type="create_dispatch_task",
        params={"instruction": "原子提交", "target": "测试路"},
        source_id="atomic-finalization",
        run_id="run-atomic-finalization",
    )
    record = WorkflowActionRecord(
        action_id=generate_action_id(),
        run_id=state.workflow_run_id,
        node_id=node.node_id,
        event_id=state.current_event["eventId"],
        action_type="create_dispatch_task",
        semantic_action_version="v1",
        params=node.config["action_params"],
        reconciliation_supported=True,
    )
    claimed = repo.claim_action_execution(record)["record"]
    node_run = WorkflowNodeRun(
        node_run_id=generate_node_run_id(state.workflow_run_id, node.node_id, 1),
        run_id=state.workflow_run_id,
        node_id=node.node_id,
        node_type=NodeType.ACTION,
        status=NodeStatus.RUNNING,
        attempt=1,
        started_at="2026-10-03T00:00:00Z",
    )
    repo.save_node_run(node_run)
    terminal_node = WorkflowNodeRun.from_dict({
        **node_run.to_dict(),
        "status": "succeeded",
        "completedAt": "2026-10-03T00:00:01Z",
    })
    nonexistent_checkpoint = WorkflowRun(
        run_id="nonexistent-checkpoint",
        status=WorkflowRunStatus.RUNNING,
        state={},
    )
    ok = repo.finalize_node_run(
        terminal_node,
        checkpoint_run=nonexistent_checkpoint,
        action_finalization={
            "actionExecutionId": claimed.action_id,
            "attempt": 1,
            "status": "succeeded",
            "result": {"ok": True},
            "finishedAt": "2026-10-03T00:00:01Z",
            "reconciliationSupported": True,
        },
    )
    assert ok is False
    assert repo.get_action_record(claimed.action_id).status == ActionStatus.RUNNING
    assert repo.get_node_runs(state.workflow_run_id)[0].status == NodeStatus.RUNNING


def test_credentials_are_rejected_and_public_action_dto_is_allow_listed(monkeypatch):
    secret = "phase21-super-secret-value"
    repo, state, safe_node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "安全参数"},
        source_id="credential-isolation",
        run_id="run-credential-isolation",
    )
    unsafe_node = NodeConfig(
        safe_node.node_id,
        NodeType.ACTION,
        config={
            "action_type": "send_notification",
            "action_params": {
                "target": "值班群",
                "message": "禁止携带密钥",
                "authorizationToken": secret,
            },
        },
    )
    blocked = asyncio.run(execute_action(
        state, unsafe_node, repository=repo
    ))
    assert blocked["status"] == "blocked"
    assert repo.list_action_records(state.workflow_run_id) == []
    events_dump = json.dumps(
        [event.to_dict() for event in repo.list_events(state.workflow_run_id)],
        ensure_ascii=False,
    )
    assert secret not in events_dump
    with open(config.DB_PATH, "rb") as database_file:
        assert secret.encode() not in database_file.read()

    # Even a legacy/corrupt row cannot make raw request params or
    # secret-shaped provider fields cross the public API boundary.
    public_record = WorkflowActionRecord(
        action_id="public-safe-action",
        run_id=state.workflow_run_id,
        node_id=safe_node.node_id,
        event_id=state.current_event["eventId"],
        action_type="send_notification",
        params={"password": "do-not-return", "message": "private"},
        result={
            "delivered": True,
            "token": "do-not-return",
            "nested": {"authorization": "do-not-return", "safe": "visible"},
        },
        status=ActionStatus.SUCCEEDED,
        attempt=1,
        external_reference="Authorization: Bearer do-not-return",
        error="token=do-not-return",
        reconciliation_message="password=do-not-return",
        completed_at="2026-10-03T00:00:01Z",
    )
    repo.save_action_record(public_record)
    monkeypatch.setattr(workflow_api, "_repo", repo)
    monkeypatch.setattr(
        workflow_api, "_def_manager", workflow_api.DefinitionManager(repo)
    )
    app = FastAPI()
    app.include_router(workflow_api.router)
    response = TestClient(app).get(
        f"/workflow/runs/{state.workflow_run_id}/actions/{public_record.action_id}"
    )
    assert response.status_code == 200
    body_text = response.text
    assert "password" not in body_text
    assert "token" not in body_text
    assert "authorization" not in body_text
    assert "do-not-return" not in body_text
    assert response.json()["result"]["nested"]["safe"] == "visible"
    assert "params" not in response.json()


def test_approval_edit_credentials_are_422_and_legacy_state_is_not_exposed(
    monkeypatch,
):
    repo = SQLiteWorkflowRepository()
    definition = _full_definition("approval-credential-boundary")
    repo.save_definition(definition)
    event = _canonical_event("approval-credential-boundary")
    executor = WorkflowExecutor(repo)
    started = asyncio.run(_drain(executor.start(
        definition.id,
        initial_event=event,
        triggered_by="phase21-security-test",
    )))
    run_id = _run_id(started)
    waiting = repo.get_run(run_id)
    approval_id = waiting.state["pendingApproval"]["approvalId"]

    monkeypatch.setattr(workflow_api, "_repo", repo)
    monkeypatch.setattr(workflow_api, "_def_manager", DefinitionManager(repo))
    monkeypatch.setattr(workflow_api, "get_executor", lambda: executor)
    app = FastAPI()
    app.include_router(workflow_api.router)
    client = TestClient(app)
    secret = "approval-injected-secret"
    edited_actions = [{
        "actionType": "send_notification",
        "params": {
            "target": "值班群",
            "message": "禁止密钥",
            "authorizationToken": secret,
        },
    }]
    response = client.post(
        f"/workflow/runs/{run_id}/approvals/{approval_id}",
        json={"action": "edit_and_approve", "editedActions": edited_actions},
    )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "invalid_parameters"
    direct = asyncio.run(executor.edit_and_approve(
        run_id,
        edited_actions=edited_actions,
        approval_id=approval_id,
    ))
    assert direct["errorCode"] == "invalid_parameters"
    assert repo.get_run(run_id).status == WorkflowRunStatus.AWAITING_APPROVAL
    assert repo.get_approval(approval_id).decision == ApprovalDecision.PENDING
    with open(config.DB_PATH, "rb") as database_file:
        assert secret.encode() not in database_file.read()

    # Old/corrupt rows are still filtered at the public DTO boundary.
    legacy_secret = "legacy-approval-secret"
    approval = repo.get_approval(approval_id)
    approval.proposed_actions = [{
        "actionType": "send_notification",
        "authorizationToken": legacy_secret,
    }]
    repo.save_approval(approval)
    corrupt_run = repo.get_run(run_id)
    corrupt_run.state["pendingApproval"]["proposedActions"] = list(
        approval.proposed_actions
    )
    repo.save_run(corrupt_run)
    public = client.get(f"/workflow/runs/{run_id}")
    assert public.status_code == 200
    assert legacy_secret not in public.text
    assert "authorizationToken" not in public.text


def test_action_audit_events_and_attempt_history_are_durable():
    provider = CountingLocalProvider("failed")
    set_notification_provider(provider)
    repo, state, node, _ = _seed_direct_action(
        action_type="send_notification",
        params={"target": "值班群", "message": "审计测试"},
        source_id="audit-attempts",
        run_id="run-audit-attempts",
    )
    failed = asyncio.run(execute_action(state, node, repository=repo))
    _fail_run(repo, state.workflow_run_id)
    request_action_execution_retry(
        repo, state.workflow_run_id, failed["actionExecutionId"]
    )
    provider._outcome = ""
    retry_state = TrafficWorkflowState.from_dict(repo.get_run(state.workflow_run_id).state)
    asyncio.run(execute_action(retry_state, node, repository=repo))

    event_types = [event.event_type for event in repo.list_events(state.workflow_run_id)]
    assert event_types.count("action_created") == 1
    assert event_types.count("action_started") == 2
    assert event_types.count("action_failed") == 1
    assert event_types.count("action_retry_requested") == 1
    assert event_types.count("action_succeeded") == 1
    for event in repo.list_events(state.workflow_run_id):
        if event.event_type not in {
            "action_created",
            "action_started",
            "action_failed",
            "action_retry_requested",
            "action_succeeded",
        }:
            continue
        assert event.payload["workflowRunId"] == state.workflow_run_id
        assert event.payload["eventId"] == state.current_event["eventId"]
        assert event.payload["actionExecutionId"] == failed["actionExecutionId"]
        assert int(event.payload["attempt"]) >= 1
    attempts = repo.list_action_attempts(failed["actionExecutionId"])
    assert [attempt.status for attempt in attempts] == [
        ActionStatus.FAILED,
        ActionStatus.SUCCEEDED,
    ]
