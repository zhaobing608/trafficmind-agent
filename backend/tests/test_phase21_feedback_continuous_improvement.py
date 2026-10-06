"""Phase 21.5 structured feedback, outcome-aware memory, and replay coverage."""

from __future__ import annotations

import os
import multiprocessing as mp
import sqlite3
import sys
from copy import deepcopy
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import backend.agent.collaboration.db_repository as collab_db
import backend.case_memory.api as case_api
import backend.case_memory.repository as case_repository
import backend.config as cfg
import backend.tools.db_tools as db_tools
from backend.agent.collaboration.db_repository import (
    SQLiteCollaborationRepository,
    init_collaboration_tables,
)
from backend.case_memory.builder import (
    TrafficCaseBuilder,
    _structured_action_diff,
)
from backend.case_memory.models import (
    CaseMemoryQuality,
    EventOutcome,
    FeedbackEffectiveness,
    FeedbackReasonCode,
    TrafficCaseMemory,
    TrafficEventFeedback,
)
from backend.case_memory.repository import (
    CaseProjectionConflict,
    SQLiteCaseMemoryRepository,
    init_case_memory_tables,
)
from backend.case_memory.service import (
    TrafficCaseMemoryService,
    TrafficFeedbackService,
)
from backend.evaluation.feedback_replay import (
    acceptance_score,
    rank_cases,
    run_feedback_pilot,
    strict_past_cases,
)
from backend.regional.repository import SQLiteRegionalRepository
from backend.tests import test_phase21_case_memory as legacy
from backend.workflow.executor import WorkflowExecutor
from backend.workflow.models import (
    ActionStatus,
    ApprovalDecision,
    ApprovalReasonCode,
    WorkflowActionRecord,
    WorkflowApproval,
    WorkflowRun,
    WorkflowRunStatus,
)
from backend.workflow.repository import SQLiteWorkflowRepository, init_workflow_tables


def _case_migration_worker(db_path: str, queue) -> None:
    try:
        cfg.DB_PATH = db_path
        init_case_memory_tables()
        queue.put("ok")
    except Exception as exc:  # pragma: no cover - asserted in parent process
        queue.put(f"{type(exc).__name__}: {exc}")


def _workflow_migration_worker(db_path: str, queue) -> None:
    try:
        cfg.DB_PATH = db_path
        init_workflow_tables()
        queue.put("ok")
    except Exception as exc:  # pragma: no cover - asserted in parent process
        queue.put(f"{type(exc).__name__}: {exc}")


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    test_db = str(tmp_path / "phase21_feedback.db")
    monkeypatch.setattr(cfg, "DB_PATH", test_db)
    monkeypatch.setattr(db_tools, "DB_PATH", test_db)
    monkeypatch.setattr(collab_db, "DB_PATH", test_db)
    db_tools.init_db()
    init_workflow_tables()
    init_collaboration_tables()
    regional = SQLiteRegionalRepository(db_path=test_db)
    regional.import_context_pack(legacy._context_pack())
    regional.import_context_pack(legacy._context_pack_b())
    init_case_memory_tables()
    workflow = SQLiteWorkflowRepository()
    case_repo = SQLiteCaseMemoryRepository()
    collaboration = SQLiteCollaborationRepository()
    builder = TrafficCaseBuilder(
        workflow_repo=workflow,
        regional_repo=regional,
        collaboration_repo=collaboration,
        feedback_repository=case_repo,
    )
    case_service = TrafficCaseMemoryService(
        repository=case_repo,
        builder=builder,
        regional_repo=regional,
    )
    feedback_service = TrafficFeedbackService(
        repository=case_repo,
        workflow_repo=workflow,
        regional_repo=regional,
        case_service=case_service,
    )
    return {
        "db": test_db,
        "workflowRepo": workflow,
        "collaborationRepo": collaboration,
        "regionalRepo": regional,
        "caseRepo": case_repo,
        "caseService": case_service,
        "feedbackService": feedback_service,
    }


@pytest.fixture()
def api_client(isolated, monkeypatch):
    monkeypatch.setattr(case_api, "_feedback_service", lambda: isolated["feedbackService"])
    monkeypatch.setattr(case_api, "_service", lambda: isolated["caseService"])
    app = FastAPI()
    app.include_router(case_api.router)
    app.include_router(case_api.feedback_router)
    return TestClient(app)


def _chain(
    isolated,
    suffix: str,
    *,
    status: WorkflowRunStatus = WorkflowRunStatus.COMPLETED,
    event_time: str = "2026-06-30T08:00:00Z",
    completed_at: str = "2026-06-30T09:00:00Z",
) -> tuple[str, str]:
    event_id = f"EVT_FB_{suffix}"
    run_id = f"wfrun_fb_{suffix}"
    legacy._save_workflow_chain(
        isolated,
        event_id=event_id,
        run_id=run_id,
        status=status,
        completed_at=completed_at,
        plan_id=f"plan_fb_{suffix}",
        collaboration_run_id=f"collab_fb_{suffix}",
    )
    # The legacy helper accepts analyzed_at only at its lower layer.  Patch the
    # canonical created_at for strict-past test fixtures explicitly.
    conn = sqlite3.connect(isolated["db"])
    try:
        conn.execute(
            "UPDATE event_records SET createdAt=?, updatedAt=? WHERE eventId=?",
            (event_time, event_time, event_id),
        )
        conn.commit()
    finally:
        conn.close()
    return event_id, run_id


def _submit(
    isolated,
    event_id: str,
    run_id: str,
    *,
    outcome: EventOutcome = EventOutcome.RESOLVED,
    effectiveness: FeedbackEffectiveness = FeedbackEffectiveness.EFFECTIVE,
    reason: FeedbackReasonCode = FeedbackReasonCode.NONE,
    comment: str = "",
):
    return isolated["feedbackService"].submit_feedback(
        event_id=event_id,
        workflow_run_id=run_id,
        event_outcome=outcome,
        effectiveness=effectiveness,
        reason_code=reason,
        comment=comment,
        reviewer="operator_phase21",
    )


@pytest.mark.parametrize(
    "decision,expected",
    [
        (ApprovalDecision.APPROVED, "accepted"),
        (ApprovalDecision.EDITED, "modified"),
        (ApprovalDecision.REJECTED, "rejected"),
    ],
)
def test_recommendation_status_projection(isolated, decision, expected):
    event_id, run_id = _chain(isolated, expected)
    original = isolated["workflowRepo"].list_approvals(run_id)[0]
    original.decision = decision
    original.edited_actions = (
        [{"actionType": "notify_wechat", "params": {"channel": "secondary"}}]
        if decision == ApprovalDecision.EDITED else []
    )
    original.reason_code = (
        ApprovalReasonCode.TOO_HIGH_RISK
        if decision == ApprovalDecision.REJECTED else ApprovalReasonCode.NONE
    )
    isolated["workflowRepo"].save_approval(original)
    case = isolated["caseService"].build_from_workflow_run(run_id).case
    assert case.recommendation_feedback["status"] == expected


def test_structured_plan_diff_records_lane_change():
    diff = _structured_action_diff(
        [{"actionStepId": "close", "actionType": "close_lanes", "params": {"close_lanes": 2}}],
        [{"actionStepId": "close", "actionType": "close_lanes", "params": {"close_lanes": 1}}],
    )
    assert diff == [{
        "actionKey": "step:close",
        "field": "params.close_lanes",
        "proposedValue": 2,
        "finalValue": 1,
        "modificationType": "changed",
    }]


def test_rejection_reason_is_durable_and_enters_case(isolated):
    _, run_id = _chain(isolated, "reason", status=WorkflowRunStatus.REJECTED)
    approval = isolated["workflowRepo"].list_approvals(run_id)[0]
    approval.reason_code = ApprovalReasonCode.INCORRECT_CONTEXT
    isolated["workflowRepo"].save_approval(approval)
    reloaded = isolated["workflowRepo"].get_approval(approval.approval_id)
    case = isolated["caseService"].build_from_workflow_run(run_id).case
    assert reloaded.reason_code == ApprovalReasonCode.INCORRECT_CONTEXT
    assert case.human_decisions[0]["reasonCode"] == "INCORRECT_CONTEXT"


def test_event_outcome_is_stored_separately_from_workflow(isolated):
    event_id, run_id = _chain(isolated, "outcome")
    response = _submit(isolated, event_id, run_id)
    case = isolated["caseRepo"].get_case(response["caseMemoryProjection"]["caseId"])
    assert case.final_status == "completed"
    assert case.event_outcome["operatorAssessment"]["outcome"] == "RESOLVED"
    assert case.event_outcome["operatorAssessment"]["effectiveness"] == "EFFECTIVE"


def test_duplicate_feedback_is_upserted_not_duplicated(isolated):
    event_id, run_id = _chain(isolated, "duplicate")
    first = _submit(isolated, event_id, run_id)
    second = _submit(
        isolated, event_id, run_id,
        outcome=EventOutcome.PARTIALLY_RESOLVED,
        effectiveness=FeedbackEffectiveness.PARTIALLY_EFFECTIVE,
    )
    assert first["created"] is True
    assert second["created"] is False
    assert first["feedback"]["feedbackId"] == second["feedback"]["feedbackId"]
    assert first["feedback"]["revision"] == 1
    assert second["feedback"]["revision"] == 2
    assert len(isolated["caseRepo"].list_feedback_for_event(event_id)) == 1


def test_terminal_projection_hook_generates_case(isolated):
    _, run_id = _chain(isolated, "hook")
    assert isolated["caseRepo"].get_case_by_source_workflow_run_id(run_id) is None
    WorkflowExecutor(isolated["workflowRepo"])._project_terminal_case_memory(run_id)
    assert isolated["caseRepo"].get_case_by_source_workflow_run_id(run_id) is not None


def test_case_contains_final_plan_not_only_proposal(isolated):
    _, run_id = _chain(isolated, "final_plan")
    approval = isolated["workflowRepo"].list_approvals(run_id)[0]
    approval.proposed_actions = [
        {"actionStepId": "close", "actionType": "close_lanes", "params": {"close_lanes": 2}}
    ]
    approval.edited_actions = [
        {"actionStepId": "close", "actionType": "close_lanes", "params": {"close_lanes": 1}}
    ]
    approval.decision = ApprovalDecision.EDITED
    isolated["workflowRepo"].save_approval(approval)
    case = isolated["caseService"].build_from_workflow_run(run_id).case
    final = case.recommendation_feedback["finalPlan"]["actions"][0]
    assert final["params"]["close_lanes"] == 1
    assert case.recommendation_feedback["modifications"][0]["proposedValue"] == 2


def test_mixed_automatic_and_approval_actions_remain_complete(isolated):
    event_id, run_id = _chain(isolated, "mixed_actions")
    plan_id = "plan_fb_mixed_actions"

    frozen = isolated["workflowRepo"].get_definition_version(plan_id, 1)
    frozen.definition_json["metadata"]["plan"]["steps"].append({
        "stepId": "auto_dispatch",
        "stepType": "action",
        "objective": "自动派警",
        "dependsOn": ["validate_event"],
        "agentType": None,
        "toolName": None,
        "actionType": "dispatch_police",
        "preconditions": [],
        "expectedOutcome": "警力已派出",
        "riskLevel": "low",
        "approvalRequired": False,
        "evidenceRefs": [],
        "retryPolicy": {},
        "timeoutSeconds": 60,
        "resultRef": "",
        "failureReason": "",
        "metadata": {"paramsTemplate": {"priority": "high"}},
    })
    isolated["workflowRepo"].save_definition_version(frozen)

    approval = isolated["workflowRepo"].list_approvals(run_id)[0]
    approval.proposed_actions = [{
        "actionStepId": "notify_ops",
        "actionType": "notify_wechat",
        "params": {"channel": "duty"},
    }]
    approval.edited_actions = []
    approval.decision = ApprovalDecision.APPROVED
    isolated["workflowRepo"].save_approval(approval)
    isolated["workflowRepo"].save_approval(WorkflowApproval(
        approval_id=f"appr_rejected_{run_id}",
        run_id=run_id,
        node_id="approval_broadcast",
        proposed_actions=[{
            "actionStepId": "broadcast",
            "actionType": "broadcast_all_channels",
            "params": {"scope": "citywide"},
        }],
        decision=ApprovalDecision.REJECTED,
        reason_code=ApprovalReasonCode.TOO_HIGH_RISK,
        reviewer="operator_b",
        created_at="2026-06-30T08:20:30Z",
        decided_at="2026-06-30T08:21:30Z",
    ))
    isolated["workflowRepo"].save_action_record(WorkflowActionRecord(
        action_id=f"act_auto_{run_id}",
        run_id=run_id,
        node_id="auto_dispatch",
        action_type="dispatch_police",
        params={"priority": "high"},
        result={"taskCreated": True},
        status=ActionStatus.SUCCEEDED,
        event_id=event_id,
        attempt=1,
        created_at="2026-06-30T08:22:30Z",
        completed_at="2026-06-30T08:23:00Z",
    ))

    case = isolated["caseService"].build_from_workflow_run(run_id).case
    original = {
        item["actionType"]: item
        for item in case.recommendation_feedback["originalRecommendation"]["actionRefs"]
    }
    assert set(original) == {
        "notify_wechat", "dispatch_police", "broadcast_all_channels",
    }
    assert original["notify_wechat"]["params"] == {"channel": "duty"}

    final_types = {
        item["actionType"]
        for item in case.recommendation_feedback["finalPlan"]["actions"]
    }
    assert final_types == {"notify_wechat", "dispatch_police"}

    dispatch = next(
        item for item in case.action_feedback
        if item["actionType"] == "dispatch_police"
    )
    assert dispatch["approved"] is True
    assert dispatch["executed"] is True
    assert dispatch["succeeded"] is True

    rejected = next(
        item for item in case.action_feedback
        if item["actionType"] == "broadcast_all_channels"
    )
    assert rejected["proposed"] is True
    assert rejected["approved"] is False
    assert rejected["executed"] is False
    assert rejected["status"] == "rejected"


def test_action_execution_outcome_enters_case(isolated):
    _, run_id = _chain(isolated, "action")
    case = isolated["caseService"].build_from_workflow_run(run_id).case
    action = next(item for item in case.action_feedback if item["actionExecutionId"])
    assert action["executed"] is True
    assert action["succeeded"] is True
    assert action["businessEffectiveness"] == "UNKNOWN"


def test_feedback_rebuild_updates_same_case_id(isolated):
    event_id, run_id = _chain(isolated, "rebuild")
    before = isolated["caseService"].build_from_workflow_run(run_id).case
    result = _submit(isolated, event_id, run_id)
    after = isolated["caseRepo"].get_case_by_source_workflow_run_id(run_id)
    assert before.case_id == after.case_id == result["caseMemoryProjection"]["caseId"]
    assert after.quality_status == CaseMemoryQuality.VERIFIED_SUCCESS


def test_feedback_rebuild_does_not_create_duplicate_case(isolated):
    event_id, run_id = _chain(isolated, "case_dedupe")
    _submit(isolated, event_id, run_id)
    _submit(isolated, event_id, run_id)
    assert len(isolated["caseRepo"].list_cases_for_source_event(event_id)) == 1


def test_one_canonical_case_per_event_supersedes_older_workflow_attempt(isolated):
    event_id, first_run = _chain(
        isolated,
        "canonical_first",
        completed_at="2026-06-30T09:00:00Z",
    )
    second_run = "wfrun_fb_canonical_second"
    legacy._save_workflow_chain(
        isolated,
        event_id=event_id,
        run_id=second_run,
        completed_at="2026-06-30T10:00:00Z",
        plan_id="plan_fb_canonical_second",
        collaboration_run_id="collab_fb_canonical_second",
    )

    first = isolated["caseService"].build_from_workflow_run(first_run).case
    stale_existing = isolated["caseRepo"].get_case(first.case_id)
    stale_rebuild = isolated["caseService"].builder.build_from_workflow_run(first_run)
    second = isolated["caseService"].build_from_workflow_run(second_run).case
    # A stale rebuild of the old attempt must not violate the partial unique
    # index or reclaim canonical status from the later Workflow.
    isolated["caseRepo"].update_case_preserving_identity(
        stale_existing,
        stale_rebuild,
        expected_feedback_revision=0,
    )
    rows = isolated["caseRepo"].list_cases_for_source_event(event_id)
    restored_first = next(item for item in rows if item.case_id == first.case_id)
    restored_second = next(item for item in rows if item.case_id == second.case_id)

    assert len(rows) == 2  # workflow-attempt audit is retained
    assert restored_first.is_canonical is False
    assert restored_first.superseded_by_case_id == restored_second.case_id
    assert restored_second.is_canonical is True
    assert restored_second.superseded_by_case_id is None
    agent_query = isolated["caseService"].query_cases(
        region_id="REGION_A",
        event_type="congestion",
        for_agent=True,
        limit=10,
    )
    assert [item.case_id for item in agent_query["cases"]] == [restored_second.case_id]
    assert isolated["caseRepo"].feedback_metrics()["cases"]["unverified"] == 1


def test_feedback_projection_cas_rejects_stale_or_mismatched_revision(isolated):
    event_id, run_id = _chain(isolated, "feedback_cas")
    _submit(isolated, event_id, run_id)
    stale_case = isolated["caseService"].builder.build_from_workflow_run(run_id)
    existing = isolated["caseRepo"].get_case_by_source_workflow_run_id(run_id)
    feedback = isolated["caseRepo"].get_feedback(event_id, run_id)
    assert stale_case.feedback_revision == feedback.revision == 1

    isolated["caseRepo"].upsert_feedback(feedback)
    with pytest.raises(CaseProjectionConflict):
        isolated["caseRepo"].update_case_preserving_identity(
            existing,
            stale_case,
            expected_feedback_revision=1,
        )

    stale_case.feedback_revision = 999
    with pytest.raises(CaseProjectionConflict):
        isolated["caseRepo"].update_case_preserving_identity(
            existing,
            stale_case,
            expected_feedback_revision=2,
        )

    repaired = isolated["caseService"].build_from_workflow_run(run_id).case
    assert repaired.feedback_revision == 2


def test_case_projection_revision_rejects_same_feedback_stale_writer(isolated):
    _, run_id = _chain(isolated, "projection_revision")
    original = isolated["caseService"].build_from_workflow_run(run_id).case
    first_writer = deepcopy(original)
    stale_writer = deepcopy(original)

    first_writer.generated_summary = "newer system projection"
    updated = isolated["caseRepo"].update_case_preserving_identity(
        original,
        first_writer,
        expected_feedback_revision=0,
    )
    assert updated.projection_revision == original.projection_revision + 1

    stale_writer.generated_summary = "stale system projection"
    with pytest.raises(CaseProjectionConflict):
        isolated["caseRepo"].update_case_preserving_identity(
            original,
            stale_writer,
            expected_feedback_revision=0,
        )
    durable = isolated["caseRepo"].get_case(original.case_id)
    assert durable.generated_summary == "newer system projection"


def test_verified_success_requires_operator_confirmation(isolated):
    event_id, run_id = _chain(isolated, "verified")
    _submit(isolated, event_id, run_id)
    case = isolated["caseRepo"].get_case_by_source_workflow_run_id(run_id)
    assert case.quality_status == CaseMemoryQuality.VERIFIED_SUCCESS


@pytest.mark.parametrize(
    "outcome,effectiveness",
    [
        (EventOutcome.UNRESOLVED, FeedbackEffectiveness.INEFFECTIVE),
        (EventOutcome.CANCELLED, FeedbackEffectiveness.INEFFECTIVE),
    ],
)
def test_failed_outcome_classification(isolated, outcome, effectiveness):
    event_id, run_id = _chain(isolated, f"failed_{outcome.value}")
    _submit(isolated, event_id, run_id, outcome=outcome, effectiveness=effectiveness)
    case = isolated["caseRepo"].get_case_by_source_workflow_run_id(run_id)
    assert case.quality_status == CaseMemoryQuality.FAILED_OUTCOME


def test_no_human_outcome_is_unverified(isolated):
    _, run_id = _chain(isolated, "unverified")
    case = isolated["caseService"].build_from_workflow_run(run_id).case
    assert case.quality_status == CaseMemoryQuality.UNVERIFIED


def test_partial_feedback_is_incomplete(isolated):
    event_id, run_id = _chain(isolated, "incomplete")
    _submit(
        isolated, event_id, run_id,
        outcome=EventOutcome.RESOLVED,
        effectiveness=FeedbackEffectiveness.UNKNOWN,
    )
    case = isolated["caseRepo"].get_case_by_source_workflow_run_id(run_id)
    assert case.quality_status == CaseMemoryQuality.INCOMPLETE
    assert case.feedback_lifecycle.value == "PARTIAL"


def test_case_from_dict_parses_string_canonical_flag():
    payload = TrafficCaseMemory(
        case_id="case_bool",
        region_id="REGION_A",
        event_id="EVT_BOOL",
        event_type="congestion",
        source_workflow_run_id="run_bool",
        final_status="completed",
        quality_status=CaseMemoryQuality.UNVERIFIED,
    ).to_dict()

    payload["isCanonical"] = "false"
    assert TrafficCaseMemory.from_dict(payload).is_canonical is False
    payload["isCanonical"] = "true"
    assert TrafficCaseMemory.from_dict(payload).is_canonical is True


def _insert_case(
    isolated,
    *,
    case_id: str,
    event_id: str,
    quality: CaseMemoryQuality,
    completed_at: str,
    region_id: str = "REGION_A",
    event_type: str = "congestion",
    feedback_updated_at: str | None = None,
    system_updated_at: str | None = None,
):
    isolated["caseRepo"].insert_case(TrafficCaseMemory(
        case_id=case_id,
        region_id=region_id,
        event_id=event_id,
        event_type=event_type,
        road_id="ROAD_A_MAIN" if region_id == "REGION_A" else "ROAD_B_MAIN",
        intersection_id="INT_A_MAIN" if region_id == "REGION_A" else "INT_B_MAIN",
        source_workflow_run_id=f"run_{case_id}",
        final_status="completed",
        quality_status=quality,
        recommendation_feedback={
            "status": "accepted",
            "finalPlan": {"actions": [{"actionType": "close_lanes", "params": {"close_lanes": 1}}]},
        },
        action_feedback=[{
            "actionType": "close_lanes",
            "status": "failed",
            "failed": True,
            "businessEffectiveness": "EFFECTIVE",
            "businessEffectivenessSource": "operator_action_feedback",
            "businessReasonCode": "OPERATOR_JUDGMENT",
        }],
        event_outcome={
            "operatorAssessment": {
                "outcome": "RESOLVED",
                "effectiveness": "EFFECTIVE",
                "comment": "private comment",
                "assessedAt": feedback_updated_at,
            },
            "businessOutcomeConfirmed": True,
        },
        completed_at=completed_at,
        feedback_updated_at=feedback_updated_at,
        system_updated_at=system_updated_at,
    ))


def _retrieval_event(isolated, suffix: str, created_at: str = "2026-08-01T08:00:00Z") -> str:
    event_id = f"EVT_RETRIEVE_{suffix}"
    legacy._seed_event(event_id, analyzed_at=created_at)
    legacy._bind_event(isolated["regionalRepo"], event_id)
    conn = sqlite3.connect(isolated["db"])
    try:
        conn.execute(
            "UPDATE event_records SET createdAt=?, updatedAt=? WHERE eventId=?",
            (created_at, created_at, event_id),
        )
        conn.commit()
    finally:
        conn.close()
    return event_id


def test_migration_preserves_legacy_update_horizon(isolated):
    _insert_case(
        isolated,
        case_id="legacy_migrated_late",
        event_id="OLD_LEGACY_MIGRATED_LATE",
        quality=CaseMemoryQuality.VERIFIED_SUCCESS,
        completed_at="2026-01-01T08:00:00Z",
        system_updated_at="2026-01-01T08:00:00Z",
    )
    conn = sqlite3.connect(isolated["db"])
    try:
        conn.execute(
            """UPDATE traffic_case_memories
               SET system_updated_at=NULL, created_at=?, updated_at=?
               WHERE case_id=?""",
            (
                "2026-01-01T08:00:00Z",
                "2026-06-01T08:00:00Z",
                "legacy_migrated_late",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    init_case_memory_tables()

    migrated = isolated["caseRepo"].get_case("legacy_migrated_late")
    assert migrated.system_updated_at == "2026-06-01T08:00:00Z"
    strict_past = isolated["caseRepo"].query_cases(
        region_id="REGION_A",
        event_type="congestion",
        as_of="2026-03-01T08:00:00Z",
        for_agent=True,
        limit=10,
    )
    assert all(case.case_id != "legacy_migrated_late" for case in strict_past["cases"])


def test_repository_reuses_completed_schema_initialization(isolated, monkeypatch):
    calls = 0
    original = case_repository.init_case_memory_tables

    def counted_init():
        nonlocal calls
        calls += 1
        return original()

    monkeypatch.setattr(case_repository, "init_case_memory_tables", counted_init)
    isolated["caseRepo"].get_case("missing_case_a")
    isolated["caseRepo"].get_case("missing_case_b")

    assert calls == 0


def test_case_schema_migration_is_safe_across_worker_processes(tmp_path, monkeypatch):
    migration_db = str(tmp_path / "case_migration_race.db")
    monkeypatch.setattr(cfg, "DB_PATH", migration_db)
    init_case_memory_tables()
    conn = sqlite3.connect(migration_db)
    try:
        conn.execute(
            "ALTER TABLE traffic_case_memories DROP COLUMN projection_revision"
        )
        conn.commit()
    finally:
        conn.close()

    context = mp.get_context("spawn")
    queue = context.Queue()
    workers = [
        context.Process(
            target=_case_migration_worker,
            args=(migration_db, queue),
        )
        for _ in range(8)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(15)
    results = [queue.get(timeout=2) for _ in workers]

    assert [worker.exitcode for worker in workers] == [0] * len(workers)
    assert results == ["ok"] * len(workers)


def test_workflow_schema_migration_is_safe_across_worker_processes(tmp_path, monkeypatch):
    migration_db = str(tmp_path / "workflow_migration_race.db")
    monkeypatch.setattr(cfg, "DB_PATH", migration_db)
    init_workflow_tables()
    conn = sqlite3.connect(migration_db)
    try:
        conn.execute("ALTER TABLE workflow_approvals DROP COLUMN reason_code")
        conn.commit()
    finally:
        conn.close()

    context = mp.get_context("spawn")
    queue = context.Queue()
    workers = [
        context.Process(
            target=_workflow_migration_worker,
            args=(migration_db, queue),
        )
        for _ in range(8)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(15)
    results = [queue.get(timeout=2) for _ in workers]

    assert [worker.exitcode for worker in workers] == [0] * len(workers)
    assert results == ["ok"] * len(workers)


def test_successful_case_ranks_before_similar_failed_case(isolated):
    current = _retrieval_event(isolated, "rank")
    _insert_case(isolated, case_id="failed_rank", event_id="OLD_FAILED", quality=CaseMemoryQuality.FAILED_OUTCOME, completed_at="2026-07-20T08:00:00Z", feedback_updated_at="2026-07-20T09:00:00Z")
    _insert_case(isolated, case_id="success_rank", event_id="OLD_SUCCESS", quality=CaseMemoryQuality.VERIFIED_SUCCESS, completed_at="2026-07-10T08:00:00Z", feedback_updated_at="2026-07-10T09:00:00Z")
    result = isolated["caseService"].get_case_context_for_event(current)
    assert result["cases"][0]["caseId"] == "success_rank"


def test_failed_case_remains_negative_evidence(isolated):
    current = _retrieval_event(isolated, "negative")
    _insert_case(isolated, case_id="failed_visible", event_id="OLD_NEG", quality=CaseMemoryQuality.FAILED_OUTCOME, completed_at="2026-07-01T08:00:00Z", feedback_updated_at="2026-07-01T09:00:00Z")
    result = isolated["caseService"].get_case_context_for_event(current)
    assert result["negativeCases"][0]["caseId"] == "failed_visible"
    assert result["negativeCases"][0]["caution"]["instruction"] == "treat_as_caution_not_positive_template"


def test_strict_past_excludes_future_case(isolated):
    current = _retrieval_event(isolated, "future", "2026-08-01T08:00:00Z")
    _insert_case(isolated, case_id="future_case", event_id="FUTURE", quality=CaseMemoryQuality.VERIFIED_SUCCESS, completed_at="2026-09-01T08:00:00Z", feedback_updated_at="2026-09-01T09:00:00Z")
    assert isolated["caseService"].get_case_context_for_event(current)["cases"] == []


def test_replay_excludes_current_event_case(isolated):
    current = _retrieval_event(isolated, "self")
    _insert_case(isolated, case_id="self_case", event_id=current, quality=CaseMemoryQuality.VERIFIED_SUCCESS, completed_at="2026-07-01T08:00:00Z", feedback_updated_at="2026-07-01T09:00:00Z")
    assert isolated["caseService"].get_case_context_for_event(current)["cases"] == []


@pytest.mark.parametrize(
    "region_id,event_type",
    [("REGION_B", "congestion"), ("REGION_A", "accident")],
)
def test_different_region_or_type_is_not_promoted(isolated, region_id, event_type):
    current = _retrieval_event(isolated, f"scope_{region_id}_{event_type}")
    _insert_case(isolated, case_id=f"wrong_{region_id}_{event_type}", event_id="WRONG", quality=CaseMemoryQuality.VERIFIED_SUCCESS, completed_at="2026-07-01T08:00:00Z", region_id=region_id, event_type=event_type, feedback_updated_at="2026-07-01T09:00:00Z")
    assert isolated["caseService"].get_case_context_for_event(current)["cases"] == []


def test_future_feedback_is_masked_and_reranked_as_unverified(isolated):
    current = _retrieval_event(isolated, "late_feedback", "2026-08-01T08:00:00Z")
    _insert_case(isolated, case_id="late_feedback", event_id="OLD_LATE", quality=CaseMemoryQuality.VERIFIED_SUCCESS, completed_at="2026-07-01T08:00:00Z", feedback_updated_at="2026-09-01T09:00:00Z")
    result = isolated["caseService"].get_case_context_for_event(current)
    item = result["unverifiedCases"][0]
    assert item["qualityStatus"] == "UNVERIFIED"
    assert item["eventOutcome"]["operatorAssessment"]["comment"] is None
    assert item["actionFeedback"][0]["businessEffectiveness"] == "UNKNOWN"
    assert item["actionFeedback"][0]["businessEffectivenessSource"] == "not_available_as_of"
    assert item["actionFeedback"][0]["businessReasonCode"] == "NONE"


def test_generic_agent_query_masks_feedback_that_did_not_exist_as_of(isolated):
    _insert_case(
        isolated,
        case_id="generic_late_feedback",
        event_id="OLD_GENERIC_LATE",
        quality=CaseMemoryQuality.VERIFIED_SUCCESS,
        completed_at="2026-07-01T08:00:00Z",
        feedback_updated_at="2026-09-01T09:00:00Z",
    )
    result = isolated["caseService"].query_cases(
        region_id="REGION_A",
        event_type="congestion",
        as_of="2026-08-01T08:00:00Z",
        for_agent=True,
    )
    case = result["cases"][0]
    assert case.quality_status == CaseMemoryQuality.UNVERIFIED
    assert case.feedback_revision is None
    assert case.event_outcome["operatorAssessment"]["source"] == "not_available_as_of"
    assert case.action_feedback[0]["businessReasonCode"] == "NONE"

    masked_filter = isolated["caseService"].query_cases(
        region_id="REGION_A",
        event_type="congestion",
        quality_status="UNVERIFIED",
        as_of="2026-08-01T08:00:00Z",
        for_agent=True,
    )
    assert [item.case_id for item in masked_filter["cases"]] == [
        "generic_late_feedback"
    ]
    future_positive_filter = isolated["caseService"].query_cases(
        region_id="REGION_A",
        event_type="congestion",
        quality_status="VERIFIED_SUCCESS",
        as_of="2026-08-01T08:00:00Z",
        for_agent=True,
    )
    assert future_positive_filter["cases"] == []


def test_strict_past_excludes_case_rebuilt_after_replay_cutoff(isolated):
    current = _retrieval_event(isolated, "future_system_projection")
    _insert_case(
        isolated,
        case_id="future_system_projection",
        event_id="OLD_REBUILT_LATE",
        quality=CaseMemoryQuality.VERIFIED_SUCCESS,
        completed_at="2026-07-01T08:00:00Z",
        feedback_updated_at="2026-07-01T09:00:00Z",
        system_updated_at="2026-09-01T09:00:00Z",
    )
    assert isolated["caseService"].get_case_context_for_event(current)["cases"] == []


def test_strict_past_uses_canonical_case_as_of_cutoff(isolated):
    current = _retrieval_event(
        isolated,
        "canonical_as_of",
        created_at="2026-02-01T08:00:00Z",
    )
    _insert_case(
        isolated,
        case_id="canonical_as_of_old",
        event_id="OLD_CANONICAL_AS_OF",
        quality=CaseMemoryQuality.VERIFIED_SUCCESS,
        completed_at="2026-01-01T08:00:00Z",
        system_updated_at="2026-01-01T09:00:00Z",
    )
    before = isolated["caseService"].get_case_context_for_event(current, limit=5)
    assert [item["caseId"] for item in before["cases"]] == ["canonical_as_of_old"]

    _insert_case(
        isolated,
        case_id="canonical_as_of_future",
        event_id="OLD_CANONICAL_AS_OF",
        quality=CaseMemoryQuality.VERIFIED_SUCCESS,
        completed_at="2026-03-01T08:00:00Z",
        system_updated_at="2026-03-01T09:00:00Z",
    )
    assert isolated["caseRepo"].get_case("canonical_as_of_old").is_canonical is False
    after = isolated["caseService"].get_case_context_for_event(current, limit=5)
    assert [item["caseId"] for item in after["cases"]] == ["canonical_as_of_old"]


def test_temporal_canonical_preserves_fractional_second_order(isolated):
    _insert_case(
        isolated,
        case_id="z_fractional_older",
        event_id="OLD_FRACTIONAL_CANONICAL",
        quality=CaseMemoryQuality.VERIFIED_SUCCESS,
        completed_at="2026-01-01T08:00:00.100Z",
        system_updated_at="2026-01-01T08:00:00.100Z",
    )
    _insert_case(
        isolated,
        case_id="a_fractional_newer",
        event_id="OLD_FRACTIONAL_CANONICAL",
        quality=CaseMemoryQuality.VERIFIED_SUCCESS,
        completed_at="2026-01-01T08:00:00.900Z",
        system_updated_at="2026-01-01T08:00:00.900Z",
    )

    result = isolated["caseRepo"].query_cases(
        region_id="REGION_A",
        event_type="congestion",
        as_of="2026-01-01T08:00:00.950Z",
        for_agent=True,
        limit=5,
    )

    assert [item.case_id for item in result["cases"]] == ["a_fractional_newer"]


def test_generic_strict_past_order_normalizes_timezone_offsets(isolated):
    _insert_case(
        isolated,
        case_id="offset_older",
        event_id="OLD_OFFSET_OLDER",
        quality=CaseMemoryQuality.VERIFIED_SUCCESS,
        completed_at="2026-01-01T10:00:00+08:00",
        system_updated_at="2026-01-01T10:00:00+08:00",
    )
    _insert_case(
        isolated,
        case_id="utc_newer",
        event_id="OLD_UTC_NEWER",
        quality=CaseMemoryQuality.VERIFIED_SUCCESS,
        completed_at="2026-01-01T03:00:00Z",
        system_updated_at="2026-01-01T03:00:00Z",
    )

    result = isolated["caseRepo"].query_cases(
        region_id="REGION_A",
        event_type="congestion",
        as_of="2026-01-01T04:00:00Z",
        for_agent=True,
        limit=5,
    )

    assert [item.case_id for item in result["cases"]] == [
        "utc_newer",
        "offset_older",
    ]


def test_system_horizon_includes_late_event_and_binding_facts(isolated):
    event_id, run_id = _chain(isolated, "late_source_horizon")
    conn = sqlite3.connect(isolated["db"])
    try:
        conn.execute(
            "UPDATE event_records SET updatedAt=? WHERE eventId=?",
            ("2026-07-15T08:00:00Z", event_id),
        )
        conn.execute(
            """UPDATE event_location_bindings
               SET resolved_at=?, created_at=?, updated_at=?
               WHERE event_id=? AND status='resolved'""",
            (
                "2026-07-20T08:00:00Z",
                "2026-07-20T08:00:00Z",
                "2026-07-20T08:00:00Z",
                event_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    case = isolated["caseService"].build_from_workflow_run(run_id).case
    assert case.system_updated_at == "2026-07-20T08:00:00Z"
    current = _retrieval_event(
        isolated,
        "before_late_binding",
        created_at="2026-07-18T08:00:00Z",
    )
    context = isolated["caseService"].get_case_context_for_event(current, limit=5)
    assert all(item["eventId"] != event_id for item in context["cases"])


def test_explicit_rebuild_never_moves_source_horizon_backwards(isolated, monkeypatch):
    event_id, run_id = _chain(isolated, "monotonic_source_horizon")
    future_source_time = "2099-07-20T08:00:00Z"
    conn = sqlite3.connect(isolated["db"])
    try:
        conn.execute(
            "UPDATE event_records SET updatedAt=? WHERE eventId=?",
            (future_source_time, event_id),
        )
        conn.commit()
    finally:
        conn.close()

    created = isolated["caseService"].build_from_workflow_run(run_id).case
    assert created.system_updated_at == future_source_time

    monkeypatch.setattr(
        "backend.case_memory.service.utc_now_iso",
        lambda: "2026-07-20T08:00:00Z",
    )
    rebuilt = isolated["caseService"].build_from_workflow_run(
        run_id,
        rebuild=True,
    ).case
    assert rebuilt.system_updated_at == future_source_time


def test_strict_past_order_ignores_mutable_projection_update_time(isolated):
    current = _retrieval_event(
        isolated,
        "stable_projection_order",
        created_at="2026-02-01T08:00:00Z",
    )
    for case_id in ("stable_a", "stable_z"):
        _insert_case(
            isolated,
            case_id=case_id,
            event_id=f"OLD_{case_id.upper()}",
            quality=CaseMemoryQuality.UNVERIFIED,
            completed_at="2026-01-01T08:00:00Z",
            system_updated_at="2026-01-01T09:00:00Z",
        )
    conn = sqlite3.connect(isolated["db"])
    try:
        conn.execute(
            "UPDATE traffic_case_memories SET updated_at=? WHERE case_id=?",
            ("2026-01-02T08:00:00Z", "stable_a"),
        )
        conn.execute(
            "UPDATE traffic_case_memories SET updated_at=? WHERE case_id=?",
            ("2026-01-01T08:00:00Z", "stable_z"),
        )
        conn.commit()
    finally:
        conn.close()
    before = isolated["caseService"].get_case_context_for_event(current, limit=1)
    assert [item["caseId"] for item in before["cases"]] == ["stable_a"]

    conn = sqlite3.connect(isolated["db"])
    try:
        conn.execute(
            "UPDATE traffic_case_memories SET updated_at=? WHERE case_id=?",
            ("2026-03-01T08:00:00Z", "stable_z"),
        )
        conn.commit()
    finally:
        conn.close()
    after = isolated["caseService"].get_case_context_for_event(current, limit=1)
    assert [item["caseId"] for item in after["cases"]] == ["stable_a"]


def test_negative_evidence_is_reserved_even_beyond_primary_candidate_window(isolated):
    current = _retrieval_event(isolated, "negative_reservation")
    for index in range(9):
        _insert_case(
            isolated,
            case_id=f"reserved_success_{index}",
            event_id=f"OLD_RESERVED_SUCCESS_{index}",
            quality=CaseMemoryQuality.VERIFIED_SUCCESS,
            completed_at=f"2026-07-{10 + index:02d}T08:00:00Z",
            feedback_updated_at=f"2026-07-{10 + index:02d}T09:00:00Z",
        )
    _insert_case(
        isolated,
        case_id="reserved_failed",
        event_id="OLD_RESERVED_FAILED",
        quality=CaseMemoryQuality.FAILED_OUTCOME,
        completed_at="2026-07-01T08:00:00Z",
        feedback_updated_at="2026-07-01T09:00:00Z",
    )
    context = isolated["caseService"].get_case_context_for_event(current, limit=2)
    assert len(context["cases"]) == 2
    assert [item["caseId"] for item in context["negativeCases"]] == ["reserved_failed"]


def test_invalid_event_timestamp_fails_closed(isolated):
    current = _retrieval_event(isolated, "bad_time")
    conn = sqlite3.connect(isolated["db"])
    conn.execute("UPDATE event_records SET createdAt='not-a-date' WHERE eventId=?", (current,))
    conn.commit()
    conn.close()
    with pytest.raises(Exception) as exc:
        isolated["caseService"].get_case_context_for_event(current)
    assert getattr(exc.value, "code", "") == "INVALID_EVENT_TIMESTAMP"


def test_replay_uses_real_final_plan_reference():
    report = run_feedback_pilot()
    modified = next(item for item in report["results"] if item["historyClass"] == "modified")
    assert modified["referenceFinalPlan"][0]["params"]["close_lanes"] == 1


def test_rejected_action_detection_metric():
    report = run_feedback_pilot()
    failed = next(item for item in report["results"] if item["historyClass"] == "failed")
    assert failed["baseline"]["rejectedActionsRecommended"] == ["broadcast_all_channels"]
    assert failed["outcomeAware"]["rejectedActionsRecommended"] == []


def test_acceptance_metric_compares_exact_final_params():
    assert acceptance_score(
        [{"actionType": "close_lanes", "params": {"close_lanes": 2}}],
        [{"actionType": "close_lanes", "params": {"close_lanes": 1}}],
    ) == 0.0


def test_outcome_aware_retrieval_metric():
    report = run_feedback_pilot()
    assert report["metrics"]["outcomeAwareRetrieval"] == 1.0
    assert report["metrics"]["futureFeedbackMasks"] == 1
    assert report["metrics"]["futureSystemProjectionExclusions"] == 1
    assert report["metrics"]["supersededCaseExclusions"] == 1
    assert report["metadata"]["agentReplayExecuted"] is False
    assert report["metadata"]["evaluationScope"] == "memory_policy_replay"


def test_negative_memory_use_metric():
    report = run_feedback_pilot()
    assert report["metrics"]["negativeMemoryUse"] == 1.0


def test_strict_past_replay_helper_excludes_self_and_future():
    cases, stats = strict_past_cases(
        {"eventId": "E", "createdAt": "2026-02-01T00:00:00Z"},
        [
            {"eventId": "E", "completedAt": "2026-01-01T00:00:00Z"},
            {"eventId": "F", "completedAt": "2026-03-01T00:00:00Z"},
        ],
    )
    assert cases == []
    assert stats["selfExcluded"] == stats["futureExcluded"] == 1


def test_strict_past_replay_normalizes_legacy_naive_timestamps():
    cases, stats = strict_past_cases(
        {"eventId": "CURRENT", "createdAt": "2026-02-01T00:00:00"},
        [{
            "caseId": "legacy_mixed_time",
            "eventId": "HISTORY",
            "completedAt": "2026-01-01T00:00:00Z",
            "systemUpdatedAt": "2026-01-01T01:00:00Z",
            "isCanonical": True,
        }],
    )

    assert [item["caseId"] for item in cases] == ["legacy_mixed_time"]
    assert stats["failedClosed"] == 0


def test_replay_temporal_canonical_keeps_old_case_before_future_successor():
    cases, stats = strict_past_cases(
        {"eventId": "CURRENT", "createdAt": "2026-02-01T00:00:00Z"},
        [
            {
                "caseId": "old_attempt",
                "eventId": "HISTORY",
                "completedAt": "2026-01-01T00:00:00Z",
                "systemUpdatedAt": "2026-01-01T01:00:00Z",
                "isCanonical": False,
                "supersededByCaseId": "future_attempt",
            },
            {
                "caseId": "future_attempt",
                "eventId": "HISTORY",
                "completedAt": "2026-03-01T00:00:00Z",
                "systemUpdatedAt": "2026-03-01T01:00:00Z",
                "isCanonical": True,
            },
        ],
    )
    assert [item["caseId"] for item in cases] == ["old_attempt"]
    assert stats["futureExcluded"] == 1
    assert stats["supersededExcluded"] == 0


def test_comment_secret_is_redacted(isolated, monkeypatch):
    monkeypatch.setenv("TRAFFICMIND_TEST_TOKEN", "s3cr3t-value")
    event_id, run_id = _chain(isolated, "secret")
    result = _submit(
        isolated, event_id, run_id,
        comment="token=s3cr3t-value operator note",
    )
    assert "s3cr3t-value" not in result["feedback"]["comment"]
    assert "REDACTED" in result["feedback"]["comment"]


def test_oversized_feedback_comment_is_rejected(api_client, isolated):
    event_id, run_id = _chain(isolated, "oversize")
    response = api_client.post(f"/events/{event_id}/feedback", json={
        "workflowRunId": run_id,
        "eventOutcome": "RESOLVED",
        "effectiveness": "EFFECTIVE",
        "comment": "x" * 1001,
    })
    assert response.status_code == 422


@pytest.mark.parametrize("field,value", [("qualityStatus", "VERIFIED_SUCCESS"), ("agentRunId", "forged")])
def test_malicious_system_metadata_is_rejected(api_client, isolated, field, value):
    event_id, run_id = _chain(isolated, f"malicious_{field}")
    payload = {
        "workflowRunId": run_id,
        "eventOutcome": "RESOLVED",
        "effectiveness": "EFFECTIVE",
        field: value,
    }
    assert api_client.post(f"/events/{event_id}/feedback", json=payload).status_code == 422


def test_client_cannot_forge_verified_success(api_client, isolated):
    event_id, run_id = _chain(isolated, "forge_quality", status=WorkflowRunStatus.FAILED)
    response = api_client.post(f"/events/{event_id}/feedback", json={
        "workflowRunId": run_id,
        "eventOutcome": "RESOLVED",
        "effectiveness": "EFFECTIVE"
    })
    assert response.status_code == 200
    assert response.json()["caseMemoryProjection"]["qualityStatus"] == "PARTIAL_SUCCESS"


def test_feedback_survives_service_restart(isolated):
    event_id, run_id = _chain(isolated, "restart")
    _submit(isolated, event_id, run_id)
    restarted = TrafficFeedbackService(
        repository=SQLiteCaseMemoryRepository(),
        workflow_repo=SQLiteWorkflowRepository(),
        regional_repo=isolated["regionalRepo"],
    )
    result = restarted.get_feedback(event_id, workflow_run_id=run_id)
    assert result["feedback"][0]["effectiveness"] == "EFFECTIVE"


def test_case_quality_survives_repository_restart(isolated):
    event_id, run_id = _chain(isolated, "quality_restart")
    _submit(isolated, event_id, run_id)
    restored = SQLiteCaseMemoryRepository().get_case_by_source_workflow_run_id(run_id)
    assert restored.quality_status == CaseMemoryQuality.VERIFIED_SUCCESS


def test_rebuild_has_no_duplicate_memory_or_embedding_side_effect(isolated):
    event_id, run_id = _chain(isolated, "embedding_dedupe")
    _submit(isolated, event_id, run_id)
    isolated["caseService"].build_from_workflow_run(run_id, rebuild=True)
    isolated["caseService"].build_from_workflow_run(run_id, rebuild=True)
    assert len(isolated["caseRepo"].list_cases_for_source_event(event_id)) == 1
    conn = sqlite3.connect(isolated["db"])
    names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    conn.close()
    assert "traffic_case_embeddings" not in names


def test_feedback_requires_exact_event_workflow_identity(api_client, isolated):
    event_a, _ = _chain(isolated, "identity_a")
    _, run_b = _chain(isolated, "identity_b")
    response = api_client.post(f"/events/{event_a}/feedback", json={
        "workflowRunId": run_b,
        "eventOutcome": "RESOLVED",
        "effectiveness": "EFFECTIVE",
    })
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "FEEDBACK_WORKFLOW_EVENT_MISMATCH"


def test_feedback_rejects_foreign_action_identity(isolated):
    event_a, run_a = _chain(isolated, "action_owner_a")
    _, run_b = _chain(isolated, "action_owner_b")
    foreign_action = isolated["workflowRepo"].list_action_records(run_b)[0]
    with pytest.raises(Exception) as exc:
        isolated["feedbackService"].submit_feedback(
            event_id=event_a,
            workflow_run_id=run_a,
            event_outcome=EventOutcome.RESOLVED,
            effectiveness=FeedbackEffectiveness.EFFECTIVE,
            action_execution_id=foreign_action.action_id,
        )
    assert getattr(exc.value, "code", "") == "FEEDBACK_ACTION_MISMATCH"


def test_per_action_business_effectiveness_is_durable_and_projected(isolated):
    event_id, run_id = _chain(isolated, "action_assessments")
    first_action = isolated["workflowRepo"].list_action_records(run_id)[0]
    second_action = WorkflowActionRecord(
        action_id="act_action_assessments_second",
        run_id=run_id,
        node_id="dispatch",
        action_type="dispatch_police",
        params={"priority": "high"},
        result={"taskCreated": True},
        status=ActionStatus.SUCCEEDED,
        event_id=event_id,
        attempt=1,
        created_at="2026-06-30T08:30:00Z",
        completed_at="2026-06-30T08:31:00Z",
    )
    isolated["workflowRepo"].save_action_record(second_action)

    response = isolated["feedbackService"].submit_feedback(
        event_id=event_id,
        workflow_run_id=run_id,
        event_outcome=EventOutcome.PARTIALLY_RESOLVED,
        effectiveness=FeedbackEffectiveness.PARTIALLY_EFFECTIVE,
        action_assessments=[
            {
                "actionExecutionId": first_action.action_id,
                "effectiveness": "INEFFECTIVE",
                "reasonCode": "INCORRECT_CONTEXT",
            },
            {
                "actionExecutionId": second_action.action_id,
                "effectiveness": "EFFECTIVE",
                "reasonCode": "NONE",
            },
        ],
    )
    assert response["feedback"]["revision"] == 1
    restored = SQLiteCaseMemoryRepository().get_feedback(event_id, run_id)
    assert [item["actionExecutionId"] for item in restored.action_assessments] == [
        first_action.action_id,
        second_action.action_id,
    ]
    case = isolated["caseRepo"].get_case_by_source_workflow_run_id(run_id)
    projected = {
        item["actionExecutionId"]: item
        for item in case.action_feedback
        if item.get("actionExecutionId")
    }
    assert projected[first_action.action_id]["businessEffectiveness"] == "INEFFECTIVE"
    assert projected[first_action.action_id]["businessReasonCode"] == "INCORRECT_CONTEXT"
    assert projected[second_action.action_id]["businessEffectiveness"] == "EFFECTIVE"
    metrics = isolated["caseRepo"].feedback_metrics()["actions"]
    assert metrics["businessAssessed"] == 2
    assert metrics["businessEffectivenessRate"] == 0.5


def test_omitted_action_assessments_preserve_and_explicit_empty_clears(isolated):
    event_id, run_id = _chain(isolated, "assessment_patch_semantics")
    action = isolated["workflowRepo"].list_action_records(run_id)[0]
    service = isolated["feedbackService"]

    service.submit_feedback(
        event_id=event_id,
        workflow_run_id=run_id,
        event_outcome=EventOutcome.RESOLVED,
        effectiveness=FeedbackEffectiveness.EFFECTIVE,
        action_assessments=[{
            "actionExecutionId": action.action_id,
            "effectiveness": "EFFECTIVE",
            "reasonCode": "NONE",
        }],
    )
    service.submit_feedback(
        event_id=event_id,
        workflow_run_id=run_id,
        event_outcome=EventOutcome.PARTIALLY_RESOLVED,
        effectiveness=FeedbackEffectiveness.PARTIALLY_EFFECTIVE,
    )
    preserved = isolated["caseRepo"].get_feedback(event_id, run_id)
    assert [item["actionExecutionId"] for item in preserved.action_assessments] == [
        action.action_id
    ]

    service.submit_feedback(
        event_id=event_id,
        workflow_run_id=run_id,
        event_outcome=EventOutcome.PARTIALLY_RESOLVED,
        effectiveness=FeedbackEffectiveness.PARTIALLY_EFFECTIVE,
        action_assessments=[],
    )
    cleared = isolated["caseRepo"].get_feedback(event_id, run_id)
    assert cleared.action_assessments == []


def test_action_assessments_reject_foreign_and_duplicate_action_ids(isolated):
    event_a, run_a = _chain(isolated, "assessment_owner_a")
    _, run_b = _chain(isolated, "assessment_owner_b")
    own = isolated["workflowRepo"].list_action_records(run_a)[0]
    foreign = isolated["workflowRepo"].list_action_records(run_b)[0]

    with pytest.raises(Exception) as foreign_exc:
        isolated["feedbackService"].submit_feedback(
            event_id=event_a,
            workflow_run_id=run_a,
            event_outcome=EventOutcome.RESOLVED,
            effectiveness=FeedbackEffectiveness.EFFECTIVE,
            action_assessments=[{
                "actionExecutionId": foreign.action_id,
                "effectiveness": "EFFECTIVE",
                "reasonCode": "NONE",
            }],
        )
    assert getattr(foreign_exc.value, "code", "") == "FEEDBACK_ACTION_MISMATCH"

    with pytest.raises(Exception) as duplicate_exc:
        isolated["feedbackService"].submit_feedback(
            event_id=event_a,
            workflow_run_id=run_a,
            event_outcome=EventOutcome.RESOLVED,
            effectiveness=FeedbackEffectiveness.EFFECTIVE,
            action_assessments=[
                {
                    "actionExecutionId": own.action_id,
                    "effectiveness": "EFFECTIVE",
                    "reasonCode": "NONE",
                },
                {
                    "actionExecutionId": own.action_id,
                    "effectiveness": "INEFFECTIVE",
                    "reasonCode": "OTHER",
                },
            ],
        )
    assert getattr(duplicate_exc.value, "code", "") == "FEEDBACK_ACTION_ASSESSMENT_INVALID"


def test_feedback_metrics_are_bounded_and_meaningful(isolated):
    event_id, run_id = _chain(isolated, "metrics")
    _submit(isolated, event_id, run_id)
    metrics = isolated["caseRepo"].feedback_metrics()
    assert metrics["total"] == 1
    assert metrics["effectiveResolutionRate"] == 1.0
    assert metrics["cases"]["verified"] == 1


def test_rank_cases_keeps_failed_case_as_cautionary_candidate():
    ranked = rank_cases([
        {"caseId": "failed", "effectiveQuality": "FAILED_OUTCOME", "locationTier": 3, "baseSimilarity": 0.99},
        {"caseId": "success", "effectiveQuality": "VERIFIED_SUCCESS", "locationTier": 3, "baseSimilarity": 0.98},
    ], outcome_aware=True)
    assert [item["caseId"] for item in ranked] == ["success", "failed"]


def test_replay_ranking_normalizes_timezone_offsets():
    ranked = rank_cases([
        {
            "caseId": "offset_older",
            "effectiveQuality": "VERIFIED_SUCCESS",
            "locationTier": 3,
            "baseSimilarity": 0.9,
            "completedAt": "2026-01-01T10:00:00+08:00",
        },
        {
            "caseId": "utc_newer",
            "effectiveQuality": "VERIFIED_SUCCESS",
            "locationTier": 3,
            "baseSimilarity": 0.9,
            "completedAt": "2026-01-01T03:00:00Z",
        },
    ], outcome_aware=True)

    assert [item["caseId"] for item in ranked] == ["utc_newer", "offset_older"]


def test_replay_ranking_has_stable_case_id_tie_break():
    ranked = rank_cases([
        {
            "caseId": "case_z",
            "effectiveQuality": "VERIFIED_SUCCESS",
            "locationTier": 3,
            "baseSimilarity": 0.9,
            "completedAt": "2026-01-01T03:00:00Z",
        },
        {
            "caseId": "case_a",
            "effectiveQuality": "VERIFIED_SUCCESS",
            "locationTier": 3,
            "baseSimilarity": 0.9,
            "completedAt": "2026-01-01T03:00:00Z",
        },
    ], outcome_aware=True)

    assert [item["caseId"] for item in ranked] == ["case_a", "case_z"]


def test_acceptance_scenario_modified_success_and_failed_caution(isolated):
    event_a, run_a = _chain(
        isolated,
        "acceptance_a",
        event_time="2026-07-01T08:00:00Z",
        completed_at="2026-07-01T09:00:00Z",
    )
    approval_a = isolated["workflowRepo"].list_approvals(run_a)[0]
    approval_a.proposed_actions = [
        {"actionStepId": "dispatch", "actionType": "dispatch_police", "params": {"priority": "high"}},
        {"actionStepId": "close", "actionType": "close_lanes", "params": {"close_lanes": 2}},
    ]
    approval_a.edited_actions = [
        {"actionStepId": "dispatch", "actionType": "dispatch_police", "params": {"priority": "high"}},
        {"actionStepId": "close", "actionType": "close_lanes", "params": {"close_lanes": 1}},
    ]
    approval_a.decision = ApprovalDecision.EDITED
    isolated["workflowRepo"].save_approval(approval_a)
    isolated["workflowRepo"].save_action_record(WorkflowActionRecord(
        action_id="act_acceptance_close",
        run_id=run_a,
        node_id="close",
        action_type="close_lanes",
        params={"close_lanes": 1},
        result={"confirmed": True},
        status=ActionStatus.SUCCEEDED,
        event_id=event_a,
        attempt=1,
        created_at="2026-07-01T08:30:00Z",
        completed_at="2026-07-01T08:31:00Z",
    ))
    _submit(isolated, event_a, run_a)
    conn = sqlite3.connect(isolated["db"])
    conn.execute(
        "UPDATE traffic_event_feedback SET created_at=?, updated_at=? WHERE workflow_run_id=?",
        ("2026-07-01T09:10:00Z", "2026-07-01T09:10:00Z", run_a),
    )
    conn.commit()
    conn.close()
    isolated["caseService"].refresh_feedback_projection(run_a)

    event_c, run_c = _chain(
        isolated,
        "acceptance_c",
        status=WorkflowRunStatus.REJECTED,
        event_time="2026-07-02T08:00:00Z",
        completed_at="2026-07-02T09:00:00Z",
    )
    approval_c = isolated["workflowRepo"].list_approvals(run_c)[0]
    approval_c.proposed_actions = [
        {"actionStepId": "broadcast", "actionType": "broadcast_all_channels", "params": {"scope": "citywide"}}
    ]
    approval_c.edited_actions = []
    approval_c.decision = ApprovalDecision.REJECTED
    approval_c.reason_code = ApprovalReasonCode.TOO_HIGH_RISK
    isolated["workflowRepo"].save_approval(approval_c)
    _submit(
        isolated,
        event_c,
        run_c,
        outcome=EventOutcome.UNRESOLVED,
        effectiveness=FeedbackEffectiveness.INEFFECTIVE,
        reason=FeedbackReasonCode.TOO_HIGH_RISK,
    )
    conn = sqlite3.connect(isolated["db"])
    conn.execute(
        "UPDATE traffic_event_feedback SET created_at=?, updated_at=? WHERE workflow_run_id=?",
        ("2026-07-02T09:10:00Z", "2026-07-02T09:10:00Z", run_c),
    )
    conn.commit()
    conn.close()
    isolated["caseService"].refresh_feedback_projection(run_c)

    event_b = _retrieval_event(
        isolated,
        "acceptance_b",
        created_at="2026-07-10T08:00:00Z",
    )
    context = isolated["caseService"].get_case_context_for_event(event_b, limit=10)
    case_a = next(item for item in context["positiveCases"] if item["eventId"] == event_a)
    case_c = next(item for item in context["negativeCases"] if item["eventId"] == event_c)

    proposed_close = next(
        item for item in case_a["recommendationFeedback"]["originalRecommendation"]["actionRefs"]
        if item["actionType"] == "close_lanes"
    )
    final_close = next(
        item for item in case_a["recommendationFeedback"]["finalPlan"]["actions"]
        if item["actionType"] == "close_lanes"
    )
    assert proposed_close["params"]["close_lanes"] == 2
    assert final_close["params"]["close_lanes"] == 1
    assert case_a["qualityStatus"] == "VERIFIED_SUCCESS"
    assert case_c["experienceType"] == "negative"
    assert case_c["caution"]["rejectedRecommendations"][0]["reasonCode"] == "TOO_HIGH_RISK"
