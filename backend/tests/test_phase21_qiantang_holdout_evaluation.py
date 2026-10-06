"""Phase21 G3-C Qiantang hold-out ablation evaluation.

The test materializes G1/G2/G3-A/G3-B/G3-C in isolated stores, then runs the
existing deterministic Agent orchestration and planning adapter under four
evaluation-only grounding projections.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import backend.agent.collaboration.db_repository as collab_db
import backend.chat.chat_db as chat_db
import backend.config as cfg
import backend.tools.db_tools as db_tools
from backend.agent.collaboration.budget import ExecutionBudget
from backend.agent.collaboration.db_repository import SQLiteCollaborationRepository, init_collaboration_tables
from backend.agent.collaboration.orchestrator import CollaborationOrchestrator
from backend.agent.multi_agent import _get_event_info
from backend.agent.router import route_agents
from backend.case_memory.repository import SQLiteCaseMemoryRepository, init_case_memory_tables
from backend.case_memory.service import TrafficCaseMemoryService
from backend.evaluation.phase21_g3c_holdout import (
    ABLATION_GROUPS,
    build_report,
    load_json,
    mask_grounding_context,
    run_summary,
    stable_hash,
    write_reports,
)
from backend.grounding.assembler import GroundedEventContextAssembler
from backend.memory.store import init_memory_tables
from backend.planning.adapter import plan_to_definition
from backend.planning.agent_planning_adapter import build_planning_input_from_agent
from backend.planning.context import build_planning_context
from backend.planning.models import PlanDefinitionStatus
from backend.planning.planner import build_plan_with_mode
from backend.planning.validator import has_errors, validate_plan
from backend.regional.importer import load_context_pack_from_directory
from backend.regional.repository import SQLiteRegionalRepository, init_regional_tables
from backend.regional.resolver import EventLocationBindingService
from backend.tools.event_identity import compact_event_context, hydrate_authoritative_event
from backend.workflow.executor import WorkflowExecutor
from backend.workflow.models import WorkflowRunStatus
from backend.workflow.repository import SQLiteWorkflowRepository, init_workflow_tables


REGION_ID = "QT_BY_XIASHA_PILOT_001"
HOLDOUT_PACK_ID = "QT_BY_XIASHA_HOLDOUT_G3C"
G3C_FROZEN_T0 = "2026-09-04T13:10:55Z"
ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_DB = ROOT / "data" / "trafficmind.db"
HISTORY_PACK_DIR = ROOT / "data" / "pilot_history" / "qt_by_xiasha_pilot_001"
REGION_PACK_DIR = ROOT / "data" / "pilot_regions" / "qt_by_xiasha_pilot_001"
HOLDOUT_DIR = ROOT / "data" / "pilot_holdout" / "qt_by_xiasha_pilot_001"


def _load_g3b_module():
    path = Path(__file__).with_name("test_phase21_qiantang_case_seed.py")
    spec = importlib.util.spec_from_file_location("phase21_g3b_case_seed_helpers", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


G3B = _load_g3b_module()

FORBIDDEN_HOLDOUT_KEYS = {
    "expectedRecommendation",
    "expectedAgentAnswer",
    "expectedPlan",
    "expectedAction",
    "expectedActions",
    "expectedCaseLesson",
    "expectedOutcome",
    "groundTruthAction",
    "correctAnswer",
    "bestStrategy",
    "preferredWorkflow",
    "shouldUseKnowledgeDoc",
    "shouldRetrieveCase",
    "goldenCase",
    "approvalDecision",
    "modelPrompt",
    "hiddenLabel",
}


def _parse_utc(value: Any) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _production_db_state() -> Dict[str, Any]:
    sidecars = {}
    for suffix in ("-wal", "-shm"):
        path = Path(f"{PRODUCTION_DB}{suffix}")
        if path.exists():
            sidecars[path.name] = {
                "sha256": _sha256(path),
                "size": path.stat().st_size,
            }
    return {
        "sha256": _sha256(PRODUCTION_DB),
        "sidecars": sidecars,
    }


def _walk_keys(value: Any) -> Iterable[str]:
    if isinstance(value, dict):
        for key, nested in value.items():
            yield key
            yield from _walk_keys(nested)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_keys(item)


def _json_field(value: Any, default: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value) if value else default
        except json.JSONDecodeError:
            return default
    return value if value is not None else default


def _table_count(db_path: str, table: str) -> int:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if row is None:
            return 0
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _location_key(event: Mapping[str, Any]) -> tuple[str, str]:
    validation = event["validation"]
    expected = validation["expectedCanonicalLocation"]
    if validation["locationGranularity"] == "intersection":
        return ("intersection", expected["intersectionId"])
    return ("road", expected["roadId"])


def _save_event(event: Dict[str, Any]) -> None:
    G3B._save_event(event)


def _resolve_event(repo: SQLiteRegionalRepository, event_id: str) -> Dict[str, Any]:
    result = EventLocationBindingService(repo).resolve_and_bind(event_id, region_id=REGION_ID)
    assert result["binding"] is not None
    assert result["resolution"]["status"] == "resolved"
    assert result["resolution"]["regionId"] == REGION_ID
    return result["binding"]


async def _drain(generator) -> List[str]:
    events: List[str] = []
    async for item in generator:
        events.append(item)
    return events


def _workflow_run_id_from_events(events: List[str]) -> str:
    return G3B._workflow_run_id_from_events(events)


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    event_db = str(tmp_path / "phase21_g3c_holdout.db")
    rag_db = str(tmp_path / "phase21_g3c_holdout_rag.db")
    fts_path = str(tmp_path / "phase21_g3c_holdout_fts.db")
    chroma_path = str(tmp_path / "phase21_g3c_holdout_chroma")
    assert event_db != str(PRODUCTION_DB)
    assert PRODUCTION_DB.exists()
    production_db_before = _production_db_state()

    monkeypatch.setattr(cfg, "DB_PATH", event_db)
    monkeypatch.setattr(db_tools, "DB_PATH", event_db)
    monkeypatch.setattr(db_tools, "datetime", G3B.FixedDateTime)
    monkeypatch.setattr(chat_db, "DB_PATH", event_db)
    monkeypatch.setattr(collab_db, "DB_PATH", event_db)
    monkeypatch.setattr(collab_db, "datetime", G3B.FixedDateTime)

    import backend.agent.collaboration.state as collab_state
    import backend.case_memory.models as case_models
    import backend.planning.models as planning_models
    import backend.regional.repository as regional_repository_module
    import backend.workflow.nodes.action as workflow_action_node
    import backend.workflow.nodes.human_approval as workflow_approval_node
    import backend.workflow.executor as workflow_executor
    import backend.workflow.models as workflow_models
    import backend.workflow.repository as workflow_repository_module
    import backend.workflow.state as workflow_state

    monkeypatch.setattr(collab_state, "datetime", G3B.FixedDateTime)
    monkeypatch.setattr(case_models, "datetime", G3B.FixedDateTime)
    monkeypatch.setattr(regional_repository_module, "datetime", G3B.FixedDateTime)
    monkeypatch.setattr(planning_models, "datetime", G3B.FixedDateTime)
    monkeypatch.setattr(workflow_action_node, "datetime", G3B.FixedDateTime)
    monkeypatch.setattr(workflow_approval_node, "datetime", G3B.FixedDateTime)
    monkeypatch.setattr(workflow_executor, "datetime", G3B.FixedDateTime)
    monkeypatch.setattr(workflow_models, "datetime", G3B.FixedDateTime)
    monkeypatch.setattr(workflow_repository_module, "datetime", G3B.FixedDateTime)
    monkeypatch.setattr(workflow_state, "datetime", G3B.FixedDateTime)

    chat_db.reset_initialized()
    db_tools.init_db()
    chat_db.init_chat_tables()
    init_memory_tables()
    init_workflow_tables()
    init_collaboration_tables()
    init_regional_tables(db_path=event_db)
    init_case_memory_tables()

    import backend.rag.v2.config as v2cfg
    import backend.rag.v2.dense_index as dense_idx
    import backend.rag.v2.document_repository as doc_repo
    import backend.rag.v2.pipeline as pipeline
    import backend.rag.v2.sparse_index as sparse_idx
    from backend.rag.v2.providers import FakeEmbeddingProvider, FakeRerankerProvider

    monkeypatch.setattr(v2cfg, "RAG_V2_DB_PATH", rag_db)
    monkeypatch.setattr(doc_repo, "RAG_V2_DB_PATH", rag_db)
    monkeypatch.setattr(v2cfg, "RAG_V2_FTS_PATH", fts_path)
    monkeypatch.setattr(sparse_idx, "RAG_V2_FTS_PATH", fts_path)
    dense_idx._VECTOR_DB_PATH = chroma_path
    monkeypatch.setattr(dense_idx, "_get_vector_db_path", lambda: chroma_path)

    fake_provider = FakeEmbeddingProvider(dimension=384)
    fake_reranker = FakeRerankerProvider()
    monkeypatch.setattr("backend.rag.v2.providers.get_embedding_provider", lambda: fake_provider)
    monkeypatch.setattr("backend.rag.v2.providers.get_reranker_provider", lambda: fake_reranker)
    monkeypatch.setattr(pipeline, "get_embedding_provider", lambda: fake_provider)
    monkeypatch.setattr(pipeline, "get_reranker_provider", lambda: fake_reranker)
    monkeypatch.setattr("backend.knowledge.service.get_embedding_provider", lambda: fake_provider)

    import backend.knowledge.regional_context as knowledge_context

    monkeypatch.setattr(knowledge_context, "get_embedding_provider", lambda: fake_provider)
    pipeline.reset_pipeline()
    sparse_idx.init_fts()
    doc_repo.init_db()

    regional_repo = SQLiteRegionalRepository(db_path=event_db)
    regional_repo.import_context_pack(load_context_pack_from_directory(REGION_PACK_DIR))
    workflow_repo = SQLiteWorkflowRepository()
    monkeypatch.setattr("backend.planning.api._repo", workflow_repo)
    monkeypatch.setattr("backend.workflow.api._repo", workflow_repo)

    yield {
        "tmpRoot": str(tmp_path),
        "eventDb": event_db,
        "ragDb": rag_db,
        "ftsPath": fts_path,
        "chromaPath": chroma_path,
        "regionalRepo": regional_repo,
        "collabRepo": SQLiteCollaborationRepository(),
        "workflowRepo": workflow_repo,
        "caseRepo": SQLiteCaseMemoryRepository(),
        "productionDbBefore": production_db_before,
    }

    assert _production_db_state() == production_db_before


@pytest.fixture()
def app_client(isolated, monkeypatch):
    import backend.agent.collaboration.orchestrator as orchestrator_mod
    import backend.app as app_mod

    monkeypatch.setattr(app_mod, "LLM_ENABLED", False)
    monkeypatch.setattr(orchestrator_mod, "LLM_ENABLED", False)

    async def memory_stub(*args, **kwargs):
        return {
            "runId": args[1],
            "sessionId": args[0],
            "candidateCount": 0,
            "createdCount": 0,
            "deduplicatedCount": 0,
            "supersededCount": 0,
            "rejectedCount": 0,
            "confirmedCount": 0,
            "latencyMs": 0,
            "traceId": "g3c_memory_stub",
            "writeResults": [],
        }

    class RecallStub:
        def recall_and_inject(self, **kwargs):
            return {
                "eventThreadId": "thread_g3c_holdout",
                "intent": "none",
                "candidateCount": 0,
                "selectedCount": 0,
                "rejectedCount": 0,
                "latencyMs": 0,
                "tokenEstimate": 0,
                "routingContext": {},
                "agentInjectionMap": {},
                "injectionContext": {
                    "stableFacts": [],
                    "confirmedDecisions": [],
                    "recentRunSummaries": [],
                },
            }

    monkeypatch.setattr(app_mod, "_run_memory_extraction", memory_stub)
    monkeypatch.setattr("backend.memory.coordinator.MemoryCoordinator", RecallStub)
    return TestClient(app_mod.app)


def test_g3c_holdout_pack_contract_is_frozen_and_unseen():
    package = load_json(HOLDOUT_DIR / "package.json")
    holdout = load_json(HOLDOUT_DIR / "holdout_events.json")
    spec = load_json(HOLDOUT_DIR / "evaluation_spec.json")
    events = holdout["events"]
    history_events = G3B._load_history_events()

    assert package["packId"] == HOLDOUT_PACK_ID
    assert package["frozenT0"] == G3C_FROZEN_T0
    assert spec["frozenT0"] == G3C_FROZEN_T0
    assert spec["selectionFrozenBeforeAgentRun"] is True
    assert spec["liveModelEvaluationClaimed"] is False
    assert spec["productionTrafficEvaluation"] is False
    assert [item["name"] for item in spec["ablationGroups"]] == ABLATION_GROUPS

    assert len(events) == 8
    assert all(_parse_utc(event["createdAt"]) > _parse_utc(G3C_FROZEN_T0) for event in events)
    assert len({event["eventId"] for event in events}) == 8
    assert set(event["eventId"] for event in events).isdisjoint({event["eventId"] for event in history_events})
    assert set(event["eventId"] for event in events).isdisjoint(set(G3B.FROZEN_SEED_IDS))
    assert len({event["eventType"] for event in events}) == 6
    assert len({_location_key(event) for event in events}) >= 5
    assert len({event["riskLevel"] for event in events}) >= 3
    assert sum(event["validation"]["locationGranularity"] == "intersection" for event in events) >= 2
    assert sum(event["validation"]["locationGranularity"] == "road" for event in events) >= 2

    formal_no_answer_payload = {
        "holdoutEvents": events,
        "evaluationSpec": {
            key: value
            for key, value in spec.items()
            if key not in {"outputClaimsAllowed", "outputClaimsNotMade"}
        },
    }
    leaked_keys = sorted(set(_walk_keys(formal_no_answer_payload)) & FORBIDDEN_HOLDOUT_KEYS)
    assert leaked_keys == []
    assert package["inventory"]["prewrittenOutcomeLabels"] == 0


def test_qiantang_g3c_holdout_ablation_evaluation(app_client, isolated, monkeypatch):
    history_events = G3B._load_history_events()
    holdout_package = load_json(HOLDOUT_DIR / "package.json")
    holdout_events = load_json(HOLDOUT_DIR / "holdout_events.json")["events"]
    evaluation_spec = load_json(HOLDOUT_DIR / "evaluation_spec.json")

    assert G3C_FROZEN_T0 == "2026-09-04T13:10:55Z"
    assert max(_parse_utc(event["createdAt"]) for event in history_events) < _parse_utc(G3C_FROZEN_T0)

    G3B._import_g2_knowledge()
    for event in history_events:
        _save_event(event)
    G3B._resolve_events(isolated["regionalRepo"], history_events)
    cases = _materialize_g3b_cases(app_client, isolated, monkeypatch, history_events)
    assert len(cases) == 8
    assert max(_parse_utc(case.completed_at) for case in cases if case.completed_at) < _parse_utc(G3C_FROZEN_T0)

    for event in holdout_events:
        _save_event(event)
        binding = _resolve_event(isolated["regionalRepo"], event["eventId"])
        expected = event["validation"]["expectedCanonicalLocation"]
        assert binding["regionId"] == expected["regionId"]
        assert binding.get("roadId") == expected.get("roadId")
        assert binding.get("intersectionId") == expected.get("intersectionId")

    case_count_before = _table_count(isolated["eventDb"], "traffic_case_memories")
    assert case_count_before == 8

    import backend.agent.collaboration.orchestrator as orchestrator_mod
    import backend.planning.planner as planning_planner

    monkeypatch.setattr(orchestrator_mod, "LLM_ENABLED", False)
    current_plan_id = {"value": "plan_g3c_unset"}
    monkeypatch.setattr(planning_planner, "generate_plan_id", lambda: current_plan_id["value"])

    assembler = GroundedEventContextAssembler(regional_repository=isolated["regionalRepo"])
    results: List[Dict[str, Any]] = []
    full_contexts: Dict[str, Dict[str, Any]] = {}

    for event in holdout_events:
        event_id = event["eventId"]
        full_context = assembler.assemble(
            event_id,
            query=f"{event['roadName']} {event['eventTypeCn']} 处置原则 证据依据",
            authoritative_event=hydrate_authoritative_event(event_id),
            history_window_days=int(evaluation_spec["historyWindowDays"]),
        ).to_dict()
        full_contexts[event_id] = full_context
        for group in ABLATION_GROUPS:
            current_plan_id["value"] = f"plan_g3c_{event_id.lower()}_{group.lower()}"
            summary = _run_single_ablation(
                event=event,
                group=group,
                grounding=mask_grounding_context(full_context, group),
                isolated=isolated,
                suffix="main",
            )
            results.append(summary)

    assert len(results) == 32
    assert _table_count(isolated["eventDb"], "traffic_case_memories") == case_count_before

    d_results = [item for item in results if item["group"] == "FULL_GROUNDING"]
    assert sum(1 for item in d_results if item["grounding"]["caseCount"] > 0) >= 3
    assert sum(item["grounding"]["rejectedCaseRefs"] for item in d_results) >= 1
    assert sum(item["grounding"]["completedCaseRefs"] for item in d_results) >= 1
    assert _sum_leakage(results, "regionalNonCanonical") == 0
    assert _sum_leakage(results, "historyFuture") == 0
    assert _sum_leakage(results, "historyWrongRegion") == 0
    assert _sum_leakage(results, "historyCurrentEventSelf") == 0
    assert _sum_leakage(results, "knowledgeIneligible") == 0
    assert _sum_leakage(results, "caseWrongRegion") == 0
    assert _sum_leakage(results, "caseFuture") == 0
    assert sum(int(item["brokenEvidenceRefCount"]) for item in results) == 0

    c_evidence = sum(item["grounding"]["evidenceRefCount"] for item in results if item["group"] == "REGIONAL_HISTORY_KNOWLEDGE")
    d_evidence = sum(item["grounding"]["evidenceRefCount"] for item in d_results)
    assert d_evidence > c_evidence

    replay_drift = 0
    for event in holdout_events[:2]:
        current_plan_id["value"] = f"plan_g3c_{event['eventId'].lower()}_full_grounding_replay"
        replay = _run_single_ablation(
            event=event,
            group="FULL_GROUNDING",
            grounding=mask_grounding_context(full_contexts[event["eventId"]], "FULL_GROUNDING"),
            isolated=isolated,
            suffix="replay",
        )
        original = next(
            item
            for item in d_results
            if item["eventId"] == event["eventId"] and item["group"] == "FULL_GROUNDING"
        )
        if (
            stable_hash(replay["grounding"]) != stable_hash(original["grounding"])
            or replay["agentOutputHash"] != original["agentOutputHash"]
            or replay["planFingerprint"] != original["planFingerprint"]
        ):
            replay_drift += 1
    assert replay_drift == 0
    assert _table_count(isolated["eventDb"], "traffic_case_memories") == case_count_before

    report = build_report(
        package=holdout_package,
        spec=evaluation_spec,
        holdout_events=holdout_events,
        results=results,
        deterministic_replay_drift_count=replay_drift,
        holdout_case_memory_created=_table_count(isolated["eventDb"], "traffic_case_memories") - case_count_before,
    )
    write_reports(HOLDOUT_DIR, report)

    assert report["expectedAgentEvalRuns"] == 32
    assert report["actualAgentEvalRuns"] == 32
    assert report["holdoutCaseMemoryCreated"] == 0
    assert report["caseMemoryAddsTraceableContext"] is True
    assert report["aggregate"]["A_CURRENT"]["evidenceRefCount"] == 0
    assert report["aggregate"]["B_REGIONAL"]["leakageCount"] == 0
    assert report["aggregate"]["C_REGION_HISTORY_KNOWLEDGE"]["leakageCount"] == 0
    assert report["aggregate"]["D_FULL"]["leakageCount"] == 0
    assert _production_db_state() == isolated["productionDbBefore"]


def _materialize_g3b_cases(app_client, isolated, monkeypatch, history_events: List[Dict[str, Any]]):
    by_id = {event["eventId"]: event for event in history_events}
    seeds = [by_id[event_id] for event_id in G3B.FROZEN_SEED_IDS]
    G3B.FixedDateTime.current = G3B._parse_utc(G3B.G3B_EXECUTION_TIME)

    with monkeypatch.context() as m:
        async def fake_agent_call(agent_name: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
            event_id = ctx["groundedContext"]["currentEvent"]["eventId"]
            return {
                "agentName": agent_name,
                "findings": [f"{agent_name} 基于当前事件、区域、历史与知识完成确定性验证研判"],
                "confidence": 0.86,
                "suggestion": "建议通知值守人员并进入处置闭环",
                "urgency": "high",
                "proposed_actions": [
                    {"actionType": "notify_wechat", "params": {"message": f"{event_id} closure validation"}}
                ],
            }

        m.setattr("backend.agent.collaboration.executor._call_agent_function", fake_agent_call)

        import backend.workflow.nodes.action as action_mod

        original_dispatch = action_mod._dispatch_action

        async def safe_dispatch(action_type: str, params: Dict[str, Any], state) -> Dict[str, Any]:
            if action_type == "notify_wechat":
                return {"sent": True, "channel": "wechat", "validationSink": "temp_no_external_dispatch"}
            if action_type == "save_result":
                return {
                    "saved": True,
                    "eventId": state.current_event.get("eventId"),
                    "validationSink": "temp_no_event_overwrite",
                }
            return await original_dispatch(action_type, params, state)

        m.setattr(action_mod, "_dispatch_action", safe_dispatch)

        from backend.planning.api import PlanRunRequest, _load_plan_from_metadata, _resolve_plan_run_event

        service = TrafficCaseMemoryService(regional_repo=isolated["regionalRepo"])
        executor = WorkflowExecutor(repository=isolated["workflowRepo"])
        created_cases = []
        for seed in seeds:
            response = app_client.post("/agent/routed_analyze/stream", json={
                "eventId": seed["eventId"],
                "content": "请基于真实事件、区域、历史、知识和案例做协同研判",
                "contextPolicy": "fresh_event",
            })
            assert response.status_code == 200, response.text
            agent_run = G3B._run_from_response(response.text, isolated["collabRepo"], seed["eventId"])
            grounding = _json_field(agent_run["grounding_context"], {})
            assert grounding["caseMemoryContext"].get("cases", []) == []

            plan_response = app_client.post("/planning/plans/from-agent", json={
                "eventId": seed["eventId"],
                "sessionId": agent_run["session_id"],
                "collaborationRunId": agent_run["run_id"],
            })
            assert plan_response.status_code == 200, plan_response.text
            plan_body = plan_response.json()
            definition = isolated["workflowRepo"].get_definition(plan_body["planId"])
            assert definition is not None
            plan = _load_plan_from_metadata(definition.metadata)
            assert plan is not None
            initial_event = _resolve_plan_run_event(
                plan,
                PlanRunRequest(event={}, sessionId=agent_run["session_id"], triggeredBy="phase21_g3b_case_seed"),
            )
            workflow_started = asyncio.run(_drain(executor.start(
                definition.id,
                session_id=agent_run["session_id"],
                initial_event=initial_event,
                triggered_by="phase21_g3b_case_seed",
            )))
            workflow_run_id = _workflow_run_id_from_events(workflow_started)
            decision = G3B.APPROVAL_DECISIONS[seed["eventId"]]
            if decision == "approve":
                asyncio.run(executor.approve(workflow_run_id, reviewer="G3-B synthetic operator"))
                asyncio.run(_drain(executor.resume(workflow_run_id)))
                expected_status = WorkflowRunStatus.COMPLETED
            else:
                asyncio.run(executor.reject(workflow_run_id, reviewer="G3-B synthetic operator"))
                expected_status = WorkflowRunStatus.REJECTED

            terminal_run = isolated["workflowRepo"].get_run(workflow_run_id)
            assert terminal_run is not None
            assert terminal_run.status == expected_status
            assert terminal_run.completed_at
            automatically_projected = isolated["caseRepo"].get_case_by_source_workflow_run_id(
                workflow_run_id
            )
            assert automatically_projected is not None
            built = service.build_from_workflow_run(workflow_run_id)
            assert built.created is False
            assert built.case.case_id == automatically_projected.case_id
            assert built.case.final_status == expected_status.value
            created_cases.append(built.case)

    assert {case.final_status for case in created_cases} == {"completed", "rejected"}
    assert sum(1 for case in created_cases if case.final_status == "completed") == 4
    assert sum(1 for case in created_cases if case.final_status == "rejected") == 4
    return created_cases


def _run_single_ablation(
    *,
    event: Mapping[str, Any],
    group: str,
    grounding: Mapping[str, Any],
    isolated: Mapping[str, Any],
    suffix: str,
) -> Dict[str, Any]:
    event_id = str(event["eventId"])
    current_event = compact_event_context(hydrate_authoritative_event(event_id))
    current_event["originalInput"] = f"{event['roadName']} {event['eventTypeCn']} holdout ablation"
    current_event["contextPolicy"] = "fresh_event"
    info = _get_event_info(current_event)
    info["originalInput"] = current_event["originalInput"]
    info["contextPolicy"] = "fresh_event"
    routing = route_agents(info)
    selected = routing["selectedAgents"][:4]
    run_id = f"run_g3c_{event_id.lower()}_{group.lower()}_{suffix}"
    session_id = f"sess_g3c_{event_id.lower()}_{group.lower()}_{suffix}"
    events = asyncio.run(_drain(CollaborationOrchestrator().execute(
        run_id,
        session_id,
        info,
        selected,
        routing.get("skippedAgents", []),
        routing.get("routingReasons", []),
        ExecutionBudget(max_agents=4, max_agent_calls=2, max_retries=1, max_total_seconds=90),
        previous_run_context=None,
        grounding_context=dict(grounding),
    )))
    assert any("event: run_completed" in item for item in events)
    collab_repo: SQLiteCollaborationRepository = isolated["collabRepo"]
    agent_run = collab_repo.get_run(run_id)
    assert agent_run is not None
    assert agent_run["status"] == "completed"
    task_outputs = _task_outputs(collab_repo.list_tasks(run_id))
    assert task_outputs
    plan = asyncio.run(_create_plan_from_agent(event_id, session_id, run_id, isolated["workflowRepo"]))
    leakage = _leakage_counts(event, grounding, isolated["regionalRepo"])
    broken_refs = _broken_evidence_ref_count(grounding)
    return run_summary(
        event=event,
        group=group,
        grounding=grounding,
        agent_run=agent_run,
        task_outputs=task_outputs,
        plan=plan,
        leakage=leakage,
        broken_ref_count=broken_refs,
    )


async def _create_plan_from_agent(
    event_id: str,
    session_id: str,
    run_id: str,
    workflow_repo: SQLiteWorkflowRepository,
) -> Dict[str, Any]:
    planning_input = build_planning_input_from_agent(event_id, session_id, run_id)
    ctx = build_planning_context(
        raw_event=planning_input.event,
        user_goal=planning_input.goal,
        rag_evidence=planning_input.ragEvidence,
        memory_context=planning_input.memoryContext,
        constraints=planning_input.constraints,
    )
    result = await build_plan_with_mode(ctx, "deterministic")
    plan = result.plan
    plan.definitionStatus = PlanDefinitionStatus.VALIDATED
    plan.plannerAudit = result.planner_audit.to_dict()
    plan.metadata.update(planning_input.planMetadata)
    issues = validate_plan(plan)
    assert not has_errors(issues), [item.to_dict() for item in issues]
    plan.definitionStatus = PlanDefinitionStatus.ACTIVE
    definition = plan_to_definition(plan)
    definition.metadata["validation"] = {
        "valid": True,
        "issueCount": len(issues),
        "issues": [item.to_dict() for item in issues],
    }
    workflow_repo.save_definition(definition)
    reloaded = workflow_repo.get_definition(plan.planId)
    assert reloaded is not None
    return plan.to_dict()


def _task_outputs(tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    outputs = []
    for task in tasks:
        output = _json_field(task.get("output_snapshot"), {})
        if isinstance(output, dict) and output:
            outputs.append(output)
    return outputs


def _leakage_counts(
    event: Mapping[str, Any],
    grounding: Mapping[str, Any],
    regional_repo: SQLiteRegionalRepository,
) -> Dict[str, int]:
    expected = event["validation"]["expectedCanonicalLocation"]
    regional = grounding.get("regionalContext") if isinstance(grounding.get("regionalContext"), dict) else {}
    location = regional.get("location") if isinstance(regional.get("location"), dict) else {}
    regional_noncanonical = int(
        regional.get("status") == "READY"
        and (
            location.get("regionId") != expected.get("regionId")
            or location.get("roadId") != expected.get("roadId")
            or location.get("intersectionId") != expected.get("intersectionId")
        )
    )

    event_time = _parse_utc(event["createdAt"])
    history_future = 0
    history_wrong_region = 0
    history_current_self = 0
    history = grounding.get("historicalContext") if isinstance(grounding.get("historicalContext"), dict) else {}
    for ref in history.get("recentEventRefs") or []:
        if not isinstance(ref, dict):
            continue
        if ref.get("eventId") == event["eventId"]:
            history_current_self += 1
        if _parse_utc(ref.get("createdAt")) >= event_time:
            history_future += 1
        binding = regional_repo.get_active_event_location_binding(str(ref.get("eventId") or ""))
        if not binding or binding.get("regionId") != REGION_ID:
            history_wrong_region += 1

    knowledge_ineligible = 0
    knowledge = grounding.get("knowledgeContext") if isinstance(grounding.get("knowledgeContext"), dict) else {}
    for item in knowledge.get("evidence") or []:
        if not isinstance(item, dict):
            continue
        if item.get("scopeMatch") == "legacy_unscoped":
            knowledge_ineligible += 1
            continue
        doc_event_type = str(item.get("eventType") or "").strip()
        if doc_event_type and doc_event_type not in {"generic", "*", "all", event.get("eventType")}:
            knowledge_ineligible += 1
            continue
        if item.get("regionId") and item.get("regionId") != REGION_ID:
            knowledge_ineligible += 1
            continue
        effective_from = item.get("effectiveFrom")
        effective_to = item.get("effectiveTo")
        if effective_from and _parse_utc(effective_from) > event_time:
            knowledge_ineligible += 1
            continue
        if effective_to and not (event_time < _parse_utc(effective_to)):
            knowledge_ineligible += 1

    case_wrong_region = 0
    case_future = 0
    case_context = grounding.get("caseMemoryContext") if isinstance(grounding.get("caseMemoryContext"), dict) else {}
    for item in case_context.get("cases") or []:
        if not isinstance(item, dict):
            continue
        if item.get("regionId") != REGION_ID:
            case_wrong_region += 1
        if item.get("completedAt") and _parse_utc(item["completedAt"]) >= event_time:
            case_future += 1

    return {
        "regionalNonCanonical": regional_noncanonical,
        "historyFuture": history_future,
        "historyWrongRegion": history_wrong_region,
        "historyCurrentEventSelf": history_current_self,
        "knowledgeIneligible": knowledge_ineligible,
        "caseWrongRegion": case_wrong_region,
        "caseFuture": case_future,
    }


def _broken_evidence_ref_count(grounding: Mapping[str, Any]) -> int:
    regional = grounding.get("regionalContext") if isinstance(grounding.get("regionalContext"), dict) else {}
    history = grounding.get("historicalContext") if isinstance(grounding.get("historicalContext"), dict) else {}
    knowledge = grounding.get("knowledgeContext") if isinstance(grounding.get("knowledgeContext"), dict) else {}
    cases = grounding.get("caseMemoryContext") if isinstance(grounding.get("caseMemoryContext"), dict) else {}
    knowledge_ids = {
        (item.get("evidenceId"), item.get("documentId"), item.get("chunkId"))
        for item in knowledge.get("evidence") or []
        if isinstance(item, dict)
    }
    case_ids = {item.get("caseId") for item in cases.get("cases") or [] if isinstance(item, dict)}
    broken = 0
    for ref in grounding.get("groundingRefs") or []:
        if not isinstance(ref, dict):
            broken += 1
            continue
        ref_type = ref.get("type")
        if ref_type == "regional_location":
            location = regional.get("location") if isinstance(regional.get("location"), dict) else {}
            if ref.get("regionId") != location.get("regionId"):
                broken += 1
        elif ref_type == "historical_traffic":
            if history.get("status") != "READY":
                broken += 1
        elif ref_type == "knowledge_evidence":
            key = (ref.get("evidenceId"), ref.get("documentId"), ref.get("chunkId"))
            if key not in knowledge_ids:
                broken += 1
        elif ref_type == "case_memory":
            if ref.get("caseId") not in case_ids:
                broken += 1
        else:
            broken += 1
    return broken


def _sum_leakage(results: Iterable[Mapping[str, Any]], key: str) -> int:
    return sum(int((item.get("leakage") or {}).get(key) or 0) for item in results)
