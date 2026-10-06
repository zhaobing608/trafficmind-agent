"""Phase21 G3-B Qiantang system-closure case seed validation.

The G3-B pack is a deterministic recipe over the frozen G3-A synthetic
validation history. Tests run the existing Agent -> Plan -> Workflow ->
Approval -> CaseMemoryBuilder path in isolated temporary stores only.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import backend.agent.collaboration.db_repository as collab_db
import backend.chat.chat_db as chat_db
import backend.config as cfg
import backend.tools.db_tools as db_tools
from backend.agent.collaboration.db_repository import (
    SQLiteCollaborationRepository,
    init_collaboration_tables,
)
from backend.case_memory.models import TrafficCaseMemory
from backend.case_memory.repository import SQLiteCaseMemoryRepository, init_case_memory_tables
from backend.case_memory.service import TrafficCaseMemoryService
from backend.memory.store import init_memory_tables
from backend.regional.importer import load_context_pack_from_directory
from backend.regional.repository import SQLiteRegionalRepository, init_regional_tables
from backend.regional.resolver import EventLocationBindingService
from backend.tools.event_identity import compact_event_context, hydrate_authoritative_event
from backend.workflow.executor import WorkflowExecutor
from backend.workflow.models import WorkflowRunStatus
from backend.workflow.repository import SQLiteWorkflowRepository, init_workflow_tables


REGION_ID = "QT_BY_XIASHA_PILOT_001"
HISTORY_DATASET_ID = "QT_BY_XIASHA_SYNTH_HISTORY_001"
HISTORY_DATASET_VERSION = "1.0.0"
CASE_SEED_PACK_ID = "QT_BY_XIASHA_CASE_SEED_G3B"
CASE_SEED_VERSION = "1.0.0"
HISTORY_PACK_DIR = Path(__file__).resolve().parents[1] / "data" / "pilot_history" / "qt_by_xiasha_pilot_001"
REGION_PACK_DIR = Path(__file__).resolve().parents[1] / "data" / "pilot_regions" / "qt_by_xiasha_pilot_001"
KNOWLEDGE_PACK_DIR = Path(__file__).resolve().parents[1] / "data" / "pilot_knowledge" / "qt_by_xiasha_pilot_001"
CASE_SEED_DIR = Path(__file__).resolve().parents[1] / "data" / "pilot_case_seed" / "qt_by_xiasha_pilot_001"
PRODUCTION_DB = Path(__file__).resolve().parents[1] / "data" / "trafficmind.db"
REPORT_PATH = Path(os.getenv("PHASE21_G3B_REPORT_PATH", "/tmp/phase21_g3b_case_seed_report.json"))
G3B_EXECUTION_TIME = "2026-09-04T13:00:00Z"
G3C_FROZEN_T0 = "2026-09-04T13:10:55Z"

FROZEN_SEED_IDS = [
    "SYN_QT_HIST_0001",
    "SYN_QT_HIST_0002",
    "SYN_QT_HIST_0003",
    "SYN_QT_HIST_0019",
    "SYN_QT_HIST_0027",
    "SYN_QT_HIST_0032",
    "SYN_QT_HIST_0044",
    "SYN_QT_HIST_0068",
]
APPROVAL_DECISIONS = {
    event_id: ("approve" if index % 2 == 1 else "reject")
    for index, event_id in enumerate(FROZEN_SEED_IDS, start=1)
}
SAFE_G3_FROM = "2024-08-01T00:00:00Z"
FORBIDDEN_RECIPE_KEYS = {
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
    "holdoutScenario",
    "holdoutGroup",
    "hiddenLabel",
    "modelPrompt",
}
FORBIDDEN_CASE_STRINGS = {
    "providerPrompt",
    "provider prompt",
    "seed_manifest",
    "execution_spec",
    "approvalPolicy",
    "odd_approve",
    "even_reject",
    "orderRule",
    "hiddenLabel",
    "modelPrompt",
    "sourceRecipe",
    "RAW_EVENT_SENTINEL",
    "FULL_RESULT_SENTINEL",
}


class FixedDateTime(datetime):
    current = datetime(2026, 9, 2, 0, 0, 0, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        value = cls.current
        if tz is not None:
            return value.astimezone(tz)
        return value.astimezone(timezone.utc).replace(tzinfo=None)


def _load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def _load_history_events() -> List[Dict[str, Any]]:
    return _load_json(HISTORY_PACK_DIR / "events.json")["events"]


def _load_g2_documents() -> List[Dict[str, Any]]:
    return _load_json(KNOWLEDGE_PACK_DIR / "documents.json")


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def _fmt_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256(path: Path) -> str:
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


def _table_count(db_path: str, table: str) -> int:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if row is None:
            return 0
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _json_field(value: Any, default: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value) if value else default
        except json.JSONDecodeError:
            return default
    return value if value is not None else default


def _walk_keys(value: Any) -> Iterable[str]:
    if isinstance(value, dict):
        for key, nested in value.items():
            yield key
            yield from _walk_keys(nested)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_keys(item)


def _save_event(event: Dict[str, Any]) -> None:
    standard_event = {
        key: event[key]
        for key in (
            "eventId",
            "eventType",
            "eventTypeCn",
            "roadName",
            "direction",
            "avgSpeed",
            "queueLength",
            "duration",
            "vehicleCount",
            "confidence",
            "weather",
            "timePeriod",
            "isMainRoad",
            "nearbySchool",
            "nearbyHospital",
        )
        if key in event
    }
    if isinstance(event.get("rawEvent"), dict) and event["rawEvent"].get("provenance"):
        standard_event["provenance"] = event["rawEvent"]["provenance"]
    FixedDateTime.current = _parse_utc(event.get("updatedAt", event["createdAt"]))
    assert db_tools.save_event_analysis({
        "eventId": event["eventId"],
        "standardEvent": standard_event,
        "riskScore": event["riskScore"],
        "riskLevel": event["riskLevel"],
        "status": event["status"],
        "report": "Phase21 G3-B synthetic system-closure seed event.",
        "analyzedAt": event["createdAt"],
    })


def _import_g2_knowledge() -> None:
    from backend.knowledge.service import create_document

    for document in _load_g2_documents():
        create_document(
            name=document["title"],
            doc_type=document["docType"],
            content=document["content"],
            metadata=document["metadata"],
        )


def _resolve_event(repo: SQLiteRegionalRepository, event_id: str) -> Dict[str, Any]:
    result = EventLocationBindingService(repo).resolve_and_bind(event_id, region_id=REGION_ID)
    assert result["binding"] is not None
    assert result["resolution"]["status"] == "resolved"
    assert result["resolution"]["regionId"] == REGION_ID
    return result["binding"]


def _resolve_events(repo: SQLiteRegionalRepository, events: List[Dict[str, Any]]) -> None:
    for event in events:
        binding = _resolve_event(repo, event["eventId"])
        expected = event["validation"]["expectedCanonicalLocation"]
        assert binding["regionId"] == expected["regionId"]
        assert binding.get("roadId") == expected.get("roadId")
        assert binding.get("intersectionId") == expected.get("intersectionId")


def _run_from_response(text: str, collab_repo: SQLiteCollaborationRepository, event_id: str) -> Dict[str, Any]:
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line == "event: run_created" and index + 1 < len(lines):
            payload = lines[index + 1]
            if payload.startswith("data: "):
                run_id = json.loads(payload.removeprefix("data: "))["runId"]
                run = collab_repo.get_run(run_id)
                if run:
                    return run
    rows = collab_repo.list_runs_by_event_id(event_id, limit=1)
    assert rows
    return rows[0]


async def _drain(generator) -> List[str]:
    events: List[str] = []
    async for item in generator:
        events.append(item)
    return events


def _workflow_run_id_from_events(events: List[str]) -> str:
    for event in events:
        if "event: workflow_started" not in event:
            continue
        for line in event.splitlines():
            if line.startswith("data: "):
                payload = json.loads(line.removeprefix("data: "))
                if payload.get("runId"):
                    return payload["runId"]
    raise AssertionError("workflow_started runId missing")


def _status_counts(cases: List[TrafficCaseMemory]) -> Dict[str, int]:
    return dict(Counter(case.final_status for case in cases))


def _insert_wrong_region_case(case_repo: SQLiteCaseMemoryRepository, completed_at: str) -> str:
    case = TrafficCaseMemory(
        case_id="case_g3b_wrong_region_boundary",
        region_id="QT_WRONG_REGION_BOUNDARY",
        event_id="SYN_QT_WRONG_REGION_BOUNDARY",
        event_type="pedestrian_intrusion",
        road_id="QT_BY_RD_XUEYUAN",
        intersection_id=None,
        source_workflow_run_id="wfrun_wrong_region_boundary",
        source_session_id="sess_wrong_region_boundary",
        source_collaboration_run_id="run_wrong_region_boundary",
        source_plan_id="plan_wrong_region_boundary",
        final_status="completed",
        quality_status="validated",
        event_snapshot={
            "eventId": "SYN_QT_WRONG_REGION_BOUNDARY",
            "eventType": "pedestrian_intrusion",
            "roadName": "学源街",
        },
        workflow_outcome={"businessOutcome": {"status": "unknown_without_external_evidence"}},
        completed_at=completed_at,
        source_type="test_boundary_fixture",
        source_reference="wrong_region_retrieval_guard",
    )
    case_repo.insert_case(case)
    return case.case_id


def _write_report(report: Dict[str, Any]) -> None:
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    event_db = str(tmp_path / "phase21_g3b_case_seed_events.db")
    rag_db = str(tmp_path / "phase21_g3b_case_seed_rag.db")
    fts_path = str(tmp_path / "phase21_g3b_case_seed_fts.db")
    chroma_path = str(tmp_path / "phase21_g3b_case_seed_chroma")
    assert event_db != str(PRODUCTION_DB)
    assert PRODUCTION_DB.exists()
    production_db_before = _production_db_state()

    monkeypatch.setattr(cfg, "DB_PATH", event_db)
    monkeypatch.setattr(db_tools, "DB_PATH", event_db)
    monkeypatch.setattr(db_tools, "datetime", FixedDateTime)
    monkeypatch.setattr(chat_db, "DB_PATH", event_db)
    monkeypatch.setattr(collab_db, "DB_PATH", event_db)
    monkeypatch.setattr(collab_db, "datetime", FixedDateTime)

    import backend.planning.models as planning_models
    import backend.agent.collaboration.state as collaboration_state
    import backend.case_memory.models as case_models
    import backend.workflow.nodes.action as workflow_action_node
    import backend.workflow.nodes.human_approval as workflow_approval_node
    import backend.regional.repository as regional_repository_module
    import backend.workflow.executor as workflow_executor
    import backend.workflow.models as workflow_models
    import backend.workflow.repository as workflow_repository_module
    import backend.workflow.state as workflow_state

    monkeypatch.setattr(case_models, "datetime", FixedDateTime)
    monkeypatch.setattr(collaboration_state, "datetime", FixedDateTime)
    monkeypatch.setattr(regional_repository_module, "datetime", FixedDateTime)
    monkeypatch.setattr(planning_models, "datetime", FixedDateTime)
    monkeypatch.setattr(workflow_action_node, "datetime", FixedDateTime)
    monkeypatch.setattr(workflow_approval_node, "datetime", FixedDateTime)
    monkeypatch.setattr(workflow_executor, "datetime", FixedDateTime)
    monkeypatch.setattr(workflow_models, "datetime", FixedDateTime)
    monkeypatch.setattr(workflow_repository_module, "datetime", FixedDateTime)
    monkeypatch.setattr(workflow_state, "datetime", FixedDateTime)
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
            "traceId": "g3b_memory_stub",
            "writeResults": [],
        }

    class RecallStub:
        def recall_and_inject(self, **kwargs):
            return {
                "eventThreadId": "thread_g3b_case_seed",
                "intent": "none",
                "candidateCount": 0,
                "selectedCount": 0,
                "rejectedCount": 0,
                "latencyMs": 0,
                "tokenEstimate": 0,
                "routingContext": {},
                "agentInjectionMap": {},
                "injectionContext": {
                    "sessionGoal": None,
                    "stableFacts": [],
                    "userCorrections": [],
                    "confirmedDecisions": [],
                    "recentRunSummaries": [],
                    "recallCount": 0,
                },
            }

    monkeypatch.setattr(app_mod, "_run_memory_extraction", memory_stub)
    monkeypatch.setattr("backend.memory.coordinator.MemoryCoordinator", RecallStub)
    return TestClient(app_mod.app)


def test_g3b_case_seed_recipe_contract_has_no_answer_labels():
    package = _load_json(CASE_SEED_DIR / "package.json")
    manifest = _load_json(CASE_SEED_DIR / "seed_manifest.json")
    execution_spec = _load_json(CASE_SEED_DIR / "execution_spec.json")

    assert package["packId"] == CASE_SEED_PACK_ID
    assert package["packVersion"] == CASE_SEED_VERSION
    assert package["regionId"] == REGION_ID
    assert package["datasetReality"] == "synthetic_system_closure_recipe"
    assert package["caseGeneratedBySystem"] is True
    assert package["productionTrafficCase"] is False
    assert package["holdoutIncluded"] is False
    assert manifest["seedSelectionChanged"] is False
    assert manifest["seedEventIds"] == FROZEN_SEED_IDS
    assert execution_spec["approvalPolicy"]["visibleToAgent"] is False
    assert [item["decision"] for item in execution_spec["approvalPolicy"]["decisions"]] == [
        APPROVAL_DECISIONS[event_id] for event_id in FROZEN_SEED_IDS
    ]
    assert package["inventory"]["holdoutEvents"] == 0
    assert package["inventory"]["prewrittenCaseContent"] == 0

    forbidden_content = manifest["forbiddenContent"]
    assert all(value is False for value in forbidden_content.values())

    recipe_without_policy_or_false_guards = {
        "package": package,
        "manifest": {
            key: value
            for key, value in manifest.items()
            if key != "forbiddenContent"
        },
        "executionSpec": {
            key: value
            for key, value in execution_spec.items()
            if key != "approvalPolicy"
        },
    }
    leaked_keys = sorted(set(_walk_keys(recipe_without_policy_or_false_guards)) & FORBIDDEN_RECIPE_KEYS)
    assert leaked_keys == []


def test_qiantang_g3b_eight_seed_system_closure(app_client, isolated, monkeypatch):
    events = _load_history_events()
    by_id = {event["eventId"]: event for event in events}
    seeds = [by_id[event_id] for event_id in FROZEN_SEED_IDS]
    location_keys = {
        (
            seed["validation"]["locationGranularity"],
            seed["validation"]["expectedCanonicalLocation"].get("intersectionId")
            or seed["validation"]["expectedCanonicalLocation"].get("roadId"),
        )
        for seed in seeds
    }
    assert len(seeds) == 8
    assert len({seed["eventType"] for seed in seeds}) >= 4
    assert len(location_keys) >= 5
    assert len({seed["riskLevel"] for seed in seeds}) >= 3
    assert sum(seed["validation"]["locationGranularity"] == "intersection" for seed in seeds) >= 2
    assert sum(seed["validation"]["locationGranularity"] == "road" for seed in seeds) >= 2

    _import_g2_knowledge()
    for event in events:
        _save_event(event)
    _resolve_events(isolated["regionalRepo"], events)
    assert _table_count(isolated["eventDb"], "traffic_case_memories") == 0
    FixedDateTime.current = _parse_utc(G3B_EXECUTION_TIME)

    captured_inputs: Dict[Tuple[str, str], Dict[str, Any]] = {}

    async def fake_agent_call(agent_name: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
        event_id = ctx["groundedContext"]["currentEvent"]["eventId"]
        captured_inputs[(event_id, agent_name)] = ctx
        return {
            "agentName": agent_name,
            "findings": [f"{agent_name} 基于当前事件、区域、历史与知识完成确定性验证研判"],
            "confidence": 0.86,
            "suggestion": "建议通知值守人员并进入处置闭环",
            "urgency": "high",
            "proposed_actions": [
                {
                    "actionType": "notify_wechat",
                    "params": {"message": f"{event_id} closure validation"},
                }
            ],
        }

    monkeypatch.setattr("backend.agent.collaboration.executor._call_agent_function", fake_agent_call)

    import backend.workflow.nodes.action as action_mod

    original_dispatch = action_mod._dispatch_action

    async def safe_dispatch(action_type: str, params: Dict[str, Any], state) -> Dict[str, Any]:
        if action_type == "notify_wechat":
            return {
                "sent": True,
                "channel": "wechat",
                "validationSink": "temp_no_external_dispatch",
            }
        if action_type == "save_result":
            return {
                "saved": True,
                "eventId": state.current_event.get("eventId"),
                "validationSink": "temp_no_event_overwrite",
            }
        return await original_dispatch(action_type, params, state)

    monkeypatch.setattr(action_mod, "_dispatch_action", safe_dispatch)

    from backend.planning.api import PlanRunRequest, _load_plan_from_metadata, _resolve_plan_run_event

    service = TrafficCaseMemoryService(regional_repo=isolated["regionalRepo"])
    executor = WorkflowExecutor(repository=isolated["workflowRepo"])
    rows: List[Dict[str, Any]] = []
    first_workflow_run_id = ""
    first_case_id = ""

    for seed in seeds:
        response = app_client.post("/agent/routed_analyze/stream", json={
            "eventId": seed["eventId"],
            "content": "请基于真实事件、区域、历史、知识和案例做协同研判",
            "contextPolicy": "fresh_event",
        })
        assert response.status_code == 200
        assert "event: grounding_ready" in response.text
        assert "event: run_completed" in response.text
        agent_run = _run_from_response(response.text, isolated["collabRepo"], seed["eventId"])
        assert agent_run["status"] == "completed"

        grounding = _json_field(agent_run["grounding_context"], {})
        assert grounding["currentEvent"]["eventId"] == seed["eventId"]
        assert grounding["regionalContext"]["status"] == "READY"
        assert grounding["historicalContext"]["status"] == "READY"
        assert grounding["historicalContext"]["eventCount"] >= 0
        assert grounding["knowledgeContext"]["status"] == "READY"
        assert grounding["knowledgeContext"]["evidence"]
        assert grounding["caseMemoryContext"]["status"] in {"EMPTY", "UNAVAILABLE"}
        assert grounding["caseMemoryContext"].get("cases", []) == []
        assert all(
            not str(ref.get("caseId", "")).startswith("case_")
            for ref in grounding.get("groundingRefs", [])
            if ref.get("type") == "case_memory"
        )

        serialized_agent_run = json.dumps(agent_run, ensure_ascii=False, sort_keys=True)
        assert "approvalPolicy" not in serialized_agent_run
        assert "odd_approve" not in serialized_agent_run
        assert "seed_manifest" not in serialized_agent_run

        assert any(key[0] == seed["eventId"] for key in captured_inputs)
        for (event_id, _agent_name), ctx in captured_inputs.items():
            if event_id != seed["eventId"]:
                continue
            assert ctx["groundedContext"]["currentEvent"]["eventId"] == seed["eventId"]
            assert ctx["groundedContext"]["caseMemoryContext"].get("cases", []) == []
            assert "approvalPolicy" not in json.dumps(ctx, ensure_ascii=False, sort_keys=True)

        plan_response = app_client.post("/planning/plans/from-agent", json={
            "eventId": seed["eventId"],
            "sessionId": agent_run["session_id"],
            "collaborationRunId": agent_run["run_id"],
        })
        assert plan_response.status_code == 200, plan_response.text
        plan_body = plan_response.json()
        assert plan_body["sourceAgent"]["collaborationRunId"] == agent_run["run_id"]
        assert plan_body["plan"]["eventId"] == seed["eventId"]
        assert plan_body["plan"]["metadata"]["eventSnapshot"]["eventId"] == seed["eventId"]
        assert plan_body["plan"]["metadata"]["sourceAgent"]["collaborationRunId"] == agent_run["run_id"]
        assert any(item["actionType"] == "notify_wechat" for item in plan_body["agentRecommendationAudit"]["accepted"])

        definition = isolated["workflowRepo"].get_definition(plan_body["planId"])
        assert definition is not None
        plan = _load_plan_from_metadata(definition.metadata)
        assert plan is not None
        initial_event = _resolve_plan_run_event(
            plan,
            PlanRunRequest(
                event={},
                sessionId=agent_run["session_id"],
                triggeredBy="phase21_g3b_case_seed",
            ),
        )
        assert initial_event["eventId"] == seed["eventId"]
        workflow_started = asyncio.run(_drain(executor.start(
            definition.id,
            session_id=agent_run["session_id"],
            initial_event=initial_event,
            triggered_by="phase21_g3b_case_seed",
        )))
        workflow_run_id = _workflow_run_id_from_events(workflow_started)
        run = isolated["workflowRepo"].get_run(workflow_run_id)
        assert run is not None
        assert run.status == WorkflowRunStatus.AWAITING_APPROVAL
        assert run.state["currentEvent"]["eventId"] == seed["eventId"]
        assert isolated["workflowRepo"].count_runs(event_id=seed["eventId"]) == 1
        assert [item.run_id for item in isolated["workflowRepo"].list_runs(event_id=seed["eventId"])] == [workflow_run_id]

        decision = APPROVAL_DECISIONS[seed["eventId"]]
        if decision == "approve":
            approval_result = asyncio.run(executor.approve(
                workflow_run_id,
                reviewer="G3-B synthetic operator",
                comment="G3-B deterministic approve",
            ))
            assert approval_result["decision"] == "approved"
            asyncio.run(_drain(executor.resume(workflow_run_id)))
            expected_status = WorkflowRunStatus.COMPLETED
        else:
            approval_result = asyncio.run(executor.reject(
                workflow_run_id,
                reviewer="G3-B synthetic operator",
                comment="G3-B deterministic reject",
            ))
            assert approval_result["decision"] == "rejected"
            expected_status = WorkflowRunStatus.REJECTED

        terminal_run = isolated["workflowRepo"].get_run(workflow_run_id)
        assert terminal_run is not None
        assert terminal_run.status == expected_status
        assert terminal_run.completed_at
        assert terminal_run.state["currentEvent"]["eventId"] == seed["eventId"]
        assert terminal_run.state.get("simulationRefs", {}) == {}

        automatically_projected = isolated["caseRepo"].get_case_by_source_workflow_run_id(
            workflow_run_id
        )
        assert automatically_projected is not None
        build_result = service.build_from_workflow_run(workflow_run_id)
        case = build_result.case
        assert build_result.created is False
        assert case.case_id == automatically_projected.case_id
        assert case.event_id == seed["eventId"]
        assert case.source_workflow_run_id == workflow_run_id
        assert case.source_plan_id == plan_body["planId"]
        assert case.source_collaboration_run_id == agent_run["run_id"]
        assert case.source_session_id == agent_run["session_id"]
        assert case.region_id == REGION_ID
        assert case.event_type == seed["eventType"]
        assert case.final_status == expected_status.value
        assert case.completed_at == terminal_run.completed_at
        assert case.workflow_outcome["businessOutcome"]["status"] == "unknown_without_external_evidence"
        assert case.event_snapshot.get("sourcePayloadStored") == {
            "rawEvent": True,
            "fullResult": True,
        }
        assert case.provenance.get("rawTranscriptStored") is False

        expected_location = seed["validation"]["expectedCanonicalLocation"]
        assert case.road_id == expected_location.get("roadId")
        assert case.intersection_id == expected_location.get("intersectionId")
        approvals = isolated["workflowRepo"].list_approvals(workflow_run_id)
        assert approvals
        assert approvals[-1].decision.value == ("approved" if decision == "approve" else "rejected")
        if decision == "reject":
            assert any(item.get("decision") == "rejected" for item in case.human_decisions)
            assert any(item.get("type") == "human_approval_rejected" for item in case.lessons)

        case_payload = json.dumps(case.to_dict(), ensure_ascii=False, sort_keys=True)
        case_leaks = sorted(item for item in FORBIDDEN_CASE_STRINGS if item in case_payload)
        assert case_leaks == []

        if not first_workflow_run_id:
            first_workflow_run_id = workflow_run_id
            first_case_id = case.case_id

        rows.append({
            "seedEventId": seed["eventId"],
            "decision": decision,
            "agentRunId": agent_run["run_id"],
            "sessionId": agent_run["session_id"],
            "planId": plan_body["planId"],
            "workflowRunId": workflow_run_id,
            "workflowStatus": terminal_run.status.value,
            "caseId": case.case_id,
            "caseStatus": case.final_status,
            "completedAt": case.completed_at,
        })

    cases = [
        item
        for seed in seeds
        for item in isolated["caseRepo"].list_cases_for_source_event(seed["eventId"])
    ]
    assert len(cases) == 8
    counts = _status_counts(cases)
    assert counts["completed"] == 4
    assert counts["rejected"] == 4
    assert _table_count(isolated["eventDb"], "traffic_case_memories") == 8

    rejected = next(case for case in cases if case.final_status == "rejected")
    rejected_query = service.query_cases(
        region_id=REGION_ID,
        event_type=rejected.event_type,
        final_status="rejected",
        for_agent=True,
        limit=10,
    )
    assert rejected.case_id in [case.case_id for case in rejected_query["cases"]]

    before_idempotent_count = _table_count(isolated["eventDb"], "traffic_case_memories")
    second = service.build_from_workflow_run(first_workflow_run_id)
    rebuild = service.build_from_workflow_run(first_workflow_run_id, rebuild=True)
    after_idempotent_count = _table_count(isolated["eventDb"], "traffic_case_memories")
    assert second.case.case_id == first_case_id
    assert rebuild.case.case_id == first_case_id
    assert second.created is False and second.rebuilt is False
    assert rebuild.created is False and rebuild.rebuilt is True
    assert after_idempotent_count == before_idempotent_count

    max_history_created = max(_parse_utc(event["createdAt"]) for event in events)
    max_case_completed = max(_parse_utc(case.completed_at) for case in cases if case.completed_at)
    recommended_t0 = G3C_FROZEN_T0
    assert _parse_utc(recommended_t0) > max_case_completed
    assert _parse_utc(recommended_t0) > max_history_created
    assert _parse_utc(recommended_t0) >= _parse_utc(SAFE_G3_FROM)

    latest_case = max(cases, key=lambda item: _parse_utc(item.completed_at))
    latest_seed = by_id[latest_case.event_id]
    target_after = {
        **latest_seed,
        "eventId": "SYN_QT_G3B_RETRIEVAL_TARGET_AFTER",
        "createdAt": recommended_t0,
        "updatedAt": recommended_t0,
        "status": "待研判",
        "rawEvent": {
            **latest_seed["rawEvent"],
            "eventId": "SYN_QT_G3B_RETRIEVAL_TARGET_AFTER",
            "provenance": {
                **latest_seed["rawEvent"].get("provenance", {}),
                "sourceReference": "test_only_retrieval_preview_target",
            },
        },
    }
    _save_event(target_after)
    _resolve_event(isolated["regionalRepo"], target_after["eventId"])
    after_context = service.get_case_context_for_event(target_after["eventId"], limit=5)
    assert latest_case.case_id in [item["caseId"] for item in after_context["cases"]]

    target_equal = {
        **latest_seed,
        "eventId": "SYN_QT_G3B_RETRIEVAL_TARGET_EQUAL",
        "createdAt": latest_case.completed_at,
        "updatedAt": latest_case.completed_at,
        "status": "待研判",
        "rawEvent": {
            **latest_seed["rawEvent"],
            "eventId": "SYN_QT_G3B_RETRIEVAL_TARGET_EQUAL",
            "provenance": {
                **latest_seed["rawEvent"].get("provenance", {}),
                "sourceReference": "test_only_equal_boundary_target",
            },
        },
    }
    _save_event(target_equal)
    _resolve_event(isolated["regionalRepo"], target_equal["eventId"])
    equal_context = service.get_case_context_for_event(target_equal["eventId"], limit=5)
    assert latest_case.case_id not in [item["caseId"] for item in equal_context["cases"]]

    wrong_region_case_id = _insert_wrong_region_case(isolated["caseRepo"], _fmt_utc(_parse_utc(recommended_t0) - timedelta(seconds=30)))
    after_wrong_region_context = service.get_case_context_for_event(target_after["eventId"], limit=10)
    assert wrong_region_case_id not in [item["caseId"] for item in after_wrong_region_context["cases"]]

    production_recheck = _sha256(PRODUCTION_DB)
    assert production_recheck == isolated["productionDbBefore"]["sha256"]
    assert isolated["eventDb"].startswith(isolated["tmpRoot"])
    assert isolated["ragDb"].startswith(isolated["tmpRoot"])
    assert isolated["ftsPath"].startswith(isolated["tmpRoot"])
    assert isolated["chromaPath"].startswith(isolated["tmpRoot"])

    generated_payload = json.dumps([case.to_dict() for case in cases], ensure_ascii=False, sort_keys=True)
    assert "simulationRunId" not in generated_payload
    assert "simulation_refs" not in generated_payload
    assert "simulationRefs" not in generated_payload

    _write_report({
        "packId": CASE_SEED_PACK_ID,
        "regionId": REGION_ID,
        "seedCount": len(seeds),
        "caseCount": len(cases),
        "statusCounts": counts,
        "caseCreatedOnlyThroughBuilder": True,
        "approvalDecisions": APPROVAL_DECISIONS,
        "rows": rows,
        "firstIdempotentWorkflowRunId": first_workflow_run_id,
        "firstIdempotentCaseId": first_case_id,
        "duplicateCountAfterIdempotency": after_idempotent_count - before_idempotent_count,
        "recommendedG3cT0": recommended_t0,
        "retrievalPreview": {
            "targetEventId": target_after["eventId"],
            "candidateCaseIds": [item["caseId"] for item in after_context["cases"]],
        },
        "strictTimeBoundary": {
            "equalTargetEventId": target_equal["eventId"],
            "excludedCaseId": latest_case.case_id,
            "candidateCaseIds": [item["caseId"] for item in equal_context["cases"]],
        },
        "wrongRegionExcludedCaseId": wrong_region_case_id,
        "productionDbSha256": production_recheck,
        "isolatedStores": {
            "eventDb": isolated["eventDb"],
            "ragDb": isolated["ragDb"],
            "ftsPath": isolated["ftsPath"],
            "chromaPath": isolated["chromaPath"],
        },
    })
