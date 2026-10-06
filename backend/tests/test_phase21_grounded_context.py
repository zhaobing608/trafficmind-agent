"""Phase21 Wave F GroundedEventContext assembly tests.

All tests use isolated temporary SQLite/RAG/Chroma state. They must not touch
backend/data/trafficmind.db or the active production knowledge index.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import backend.config as cfg
import backend.tools.db_tools as db_tools
from backend.case_memory.models import CaseMemoryQuality, TrafficCaseMemory
from backend.case_memory.repository import SQLiteCaseMemoryRepository, init_case_memory_tables
from backend.grounding.assembler import GroundedEventContextAssembler
from backend.regional.repository import SQLiteRegionalRepository


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    production_db = str(Path(__file__).resolve().parents[1] / "data" / "trafficmind.db")
    event_db = str(tmp_path / "phase21_wave_f_events.db")
    rag_db = str(tmp_path / "phase21_wave_f_rag.db")
    chroma_path = str(tmp_path / "phase21_wave_f_chroma")
    fts_path = str(tmp_path / "phase21_wave_f_fts.db")
    assert event_db != production_db

    monkeypatch.setattr(cfg, "DB_PATH", event_db)
    monkeypatch.setattr(db_tools, "DB_PATH", event_db)
    db_tools.init_db()
    init_case_memory_tables()

    import backend.rag.v2.config as v2cfg
    import backend.rag.v2.dense_index as dense_idx
    import backend.rag.v2.document_repository as doc_repo
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
    monkeypatch.setattr("backend.knowledge.service.get_embedding_provider", lambda: fake_provider)

    sparse_idx.init_fts()
    doc_repo.init_db()

    regional_repo = SQLiteRegionalRepository(db_path=event_db)
    regional_repo.import_context_pack(_region_a_pack())
    regional_repo.import_context_pack(_region_b_pack())
    return {
        "db": event_db,
        "productionDb": production_db,
        "regionalRepo": regional_repo,
        "caseRepo": SQLiteCaseMemoryRepository(),
    }


def _region_a_pack() -> Dict[str, Any]:
    return {
        "packageVersion": 1,
        "sourceType": "synthetic_fixture",
        "sourceReference": "backend/tests/test_phase21_grounded_context.py",
        "verificationStatus": "synthetic",
        "region": {
            "regionId": "TEST_REGION_A",
            "name": "测试区域A",
            "city": "测试市",
            "timezone": "Asia/Shanghai",
        },
        "roads": [
            {"roadId": "ROAD_A_PEOPLE", "regionId": "TEST_REGION_A", "name": "人民路"},
            {"roadId": "ROAD_A_LIBERATION", "regionId": "TEST_REGION_A", "name": "解放路"},
        ],
        "intersections": [
            {
                "intersectionId": "INT_A_PEOPLE_LIBERATION",
                "regionId": "TEST_REGION_A",
                "name": "人民路-解放路路口",
            }
        ],
        "roadRelations": [
            {
                "relationId": "REL_A_PEOPLE_CONNECT",
                "regionId": "TEST_REGION_A",
                "fromEntityType": "road",
                "fromEntityId": "ROAD_A_PEOPLE",
                "toEntityType": "intersection",
                "toEntityId": "INT_A_PEOPLE_LIBERATION",
                "relationType": "connects",
            },
            {
                "relationId": "REL_A_LIBERATION_CONNECT",
                "regionId": "TEST_REGION_A",
                "fromEntityType": "intersection",
                "fromEntityId": "INT_A_PEOPLE_LIBERATION",
                "toEntityType": "road",
                "toEntityId": "ROAD_A_LIBERATION",
                "relationType": "connects",
            },
        ],
        "pois": [
            {
                "poiId": "POI_A_SCHOOL",
                "regionId": "TEST_REGION_A",
                "name": "实验小学",
                "type": "school",
                "intersectionId": "INT_A_PEOPLE_LIBERATION",
                "importance": "high",
            }
        ],
    }


def _region_b_pack() -> Dict[str, Any]:
    return {
        "packageVersion": 1,
        "sourceType": "synthetic_fixture",
        "sourceReference": "backend/tests/test_phase21_grounded_context.py",
        "verificationStatus": "synthetic",
        "region": {
            "regionId": "TEST_REGION_B",
            "name": "测试区域B",
            "city": "测试市",
            "timezone": "Asia/Shanghai",
        },
        "roads": [
            {"roadId": "ROAD_B_OTHER", "regionId": "TEST_REGION_B", "name": "外环路"},
        ],
        "intersections": [
            {
                "intersectionId": "INT_B_OTHER",
                "regionId": "TEST_REGION_B",
                "name": "外环路-支路路口",
            }
        ],
        "roadRelations": [
            {
                "relationId": "REL_B_OTHER_CONNECT",
                "regionId": "TEST_REGION_B",
                "fromEntityType": "road",
                "fromEntityId": "ROAD_B_OTHER",
                "toEntityType": "intersection",
                "toEntityId": "INT_B_OTHER",
                "relationType": "connects",
            }
        ],
        "pois": [],
    }


def _seed_event(
    event_id: str,
    *,
    region_label: str = "人民路-解放路路口",
    event_type: str = "accident",
    analyzed_at: str = "2026-06-30T08:00:00Z",
    risk_score: int = 92,
    status: str = "待派单",
) -> None:
    event_type_cn = "事故" if event_type == "accident" else "拥堵"
    assert db_tools.save_event_analysis({
        "eventId": event_id,
        "standardEvent": {
            "eventId": event_id,
            "eventType": event_type,
            "eventTypeCn": event_type_cn,
            "roadName": region_label,
            "direction": "东向西",
            "avgSpeed": 9,
            "queueLength": 180,
            "duration": 3600,
            "weather": "rain",
            "timePeriod": "morning_peak",
            "isMainRoad": True,
            "nearbySchool": True,
            "debugOnly": "RAW_EVENT_SENTINEL",
        },
        "riskScore": risk_score,
        "riskLevel": "重大风险",
        "status": status,
        "report": "synthetic fixture report",
        "analyzedAt": analyzed_at,
        "debugPayload": "FULL_RESULT_SENTINEL",
    })


def _bind_event(
    repo: SQLiteRegionalRepository,
    event_id: str,
    *,
    region_id: str = "TEST_REGION_A",
    road_id: str = "ROAD_A_PEOPLE",
    intersection_id: str = "INT_A_PEOPLE_LIBERATION",
    re_resolve: bool = False,
) -> None:
    repo.save_resolved_event_location_binding({
        "eventId": event_id,
        "status": "resolved",
        "resolutionMethod": "TEST_BINDING",
        "regionId": region_id,
        "roadId": road_id,
        "intersectionId": intersection_id,
        "matchedAlias": "人民路",
    }, re_resolve=re_resolve)


def _create_doc(name: str, content: str, metadata: Dict[str, Any]) -> dict:
    from backend.knowledge.service import create_document

    return create_document(
        name=name,
        doc_type="rule",
        content=f"## {name}\n\n{content}",
        metadata={
            "sourceId": f"test:{name}",
            "authorityLevel": "official",
            **metadata,
        },
    )


def _insert_case(
    repo: SQLiteCaseMemoryRepository,
    case_id: str,
    *,
    region_id: str = "TEST_REGION_A",
    road_id: str = "ROAD_A_PEOPLE",
    intersection_id: str = "INT_A_PEOPLE_LIBERATION",
    event_id: str | None = None,
    event_type: str = "accident",
    completed_at: str = "2026-06-20T08:00:00Z",
    quality: CaseMemoryQuality = CaseMemoryQuality.VALIDATED,
) -> None:
    repo.insert_case(TrafficCaseMemory(
        case_id=case_id,
        region_id=region_id,
        event_id=event_id or f"E_{case_id}",
        event_type=event_type,
        road_id=road_id,
        intersection_id=intersection_id,
        source_workflow_run_id=f"wfrun_{case_id}",
        source_collaboration_run_id=f"collab_{case_id}",
        source_plan_id=f"plan_{case_id}",
        final_status="completed",
        quality_status=quality,
        generated_summary=f"{case_id} 处置复盘摘要",
        lessons=[{"type": "dispatch", "severity": "high", "summary": f"{case_id} 经验"}],
        started_at="2026-06-20T07:30:00Z",
        completed_at=completed_at,
        source_type="synthetic_fixture",
    ))


def _serialized(context: Dict[str, Any]) -> str:
    return json.dumps(context, ensure_ascii=False, sort_keys=True)


def test_grounded_context_integrates_regional_history_knowledge_and_cases(isolated):
    repo = isolated["regionalRepo"]
    case_repo = isolated["caseRepo"]
    _seed_event("E_TARGET", analyzed_at="2026-06-30T08:00:00Z")
    _seed_event("E_HIST_PAST", analyzed_at="2026-06-20T08:00:00Z", risk_score=80)
    _seed_event("E_HIST_FUTURE", analyzed_at="2026-07-01T08:00:00Z", risk_score=100)
    _seed_event("E_HIST_REGION_B", region_label="外环路-支路路口", analyzed_at="2026-06-18T08:00:00Z")
    _bind_event(repo, "E_TARGET")
    _bind_event(repo, "E_HIST_PAST")
    _bind_event(repo, "E_HIST_FUTURE")
    _bind_event(
        repo,
        "E_HIST_REGION_B",
        region_id="TEST_REGION_B",
        road_id="ROAD_B_OTHER",
        intersection_id="INT_B_OTHER",
    )

    include_regional = _create_doc(
        "人民路事故处置规则",
        "学校周边事故应先保护行人与救援通道。",
        {
            "regionId": "TEST_REGION_A",
            "roadId": "ROAD_A_PEOPLE",
            "intersectionId": "INT_A_PEOPLE_LIBERATION",
            "eventType": "accident",
            "effectiveFrom": "2026-01-01T00:00:00Z",
        },
    )
    include_global = _create_doc(
        "事故通用处置原则",
        "重大风险事故需要先控流、再清障。",
        {"groundingScope": "GLOBAL", "eventType": "accident"},
    )
    excluded_future = _create_doc(
        "未来才生效事故规则",
        "未来规则不得进入历史事件研判。",
        {
            "regionId": "TEST_REGION_A",
            "eventType": "accident",
            "effectiveFrom": "2026-07-01T00:00:00Z",
        },
    )
    excluded_region = _create_doc(
        "外环路事故规则",
        "错误区域规则不得进入测试区域A。",
        {
            "regionId": "TEST_REGION_B",
            "roadId": "ROAD_B_OTHER",
            "eventType": "accident",
            "effectiveFrom": "2026-01-01T00:00:00Z",
        },
    )
    _insert_case(case_repo, "case_past_a", completed_at="2026-06-25T08:00:00Z")
    _insert_case(case_repo, "case_future_a", completed_at="2026-07-01T08:00:00Z")
    _insert_case(
        case_repo,
        "case_region_b",
        region_id="TEST_REGION_B",
        road_id="ROAD_B_OTHER",
        intersection_id="INT_B_OTHER",
        completed_at="2026-06-21T08:00:00Z",
    )

    context = GroundedEventContextAssembler(regional_repository=repo).assemble(
        "E_TARGET",
        query="学校周边事故处置",
    ).to_dict()
    serialized = _serialized(context)

    assert context["groundingStatus"] == "FULL"
    assert context["currentEvent"]["eventId"] == "E_TARGET"
    assert context["currentEvent"]["roadName"] == "人民路-解放路路口"
    assert context["regionalContext"]["location"]["regionId"] == "TEST_REGION_A"
    assert context["regionalContext"]["location"]["intersectionId"] == "INT_A_PEOPLE_LIBERATION"
    assert context["regionalContext"]["nearbyPois"][0]["poiId"] == "POI_A_SCHOOL"
    assert len(context["regionalContext"]["connectedRoads"]) <= 5
    assert context["historicalContext"]["eventCount"] == 1
    assert [ref["eventId"] for ref in context["historicalContext"]["recentEventRefs"]] == ["E_HIST_PAST"]
    assert {item["documentId"] for item in context["knowledgeContext"]["evidence"]} >= {
        include_regional["documentId"],
        include_global["documentId"],
    }
    assert [case["caseId"] for case in context["caseMemoryContext"]["cases"]] == ["case_past_a"]
    assert any(ref["type"] == "knowledge_evidence" for ref in context["groundingRefs"])
    assert any(ref["type"] == "case_memory" for ref in context["groundingRefs"])

    assert "E_HIST_FUTURE" not in serialized
    assert "E_HIST_REGION_B" not in serialized
    assert excluded_future["documentId"] not in serialized
    assert excluded_region["documentId"] not in serialized
    assert "case_future_a" not in serialized
    assert "case_region_b" not in serialized
    assert "RAW_EVENT_SENTINEL" not in serialized
    assert "FULL_RESULT_SENTINEL" not in serialized
    assert "rawEvent" not in serialized
    assert "fullResult" not in serialized


def test_grounded_context_degrades_without_location_but_keeps_explicit_global_knowledge(isolated):
    _seed_event("E_UNRESOLVED", analyzed_at="2026-06-30T08:00:00Z")
    global_doc = _create_doc(
        "事故全局安全原则",
        "证据不足时不得推断具体道路关系。",
        {"groundingScope": "GLOBAL", "eventType": "accident"},
    )

    context = GroundedEventContextAssembler(
        regional_repository=isolated["regionalRepo"],
    ).assemble("E_UNRESOLVED", query="事故").to_dict()
    serialized = _serialized(context)

    assert context["groundingStatus"] == "MINIMAL"
    assert context["regionalContext"]["status"] == "UNRESOLVED"
    assert context["historicalContext"]["status"] == "UNAVAILABLE"
    assert context["caseMemoryContext"]["status"] == "UNRESOLVED"
    assert context["knowledgeContext"]["status"] == "READY"
    assert [item["documentId"] for item in context["knowledgeContext"]["evidence"]] == [global_doc["documentId"]]
    assert "TEST_REGION_A" not in serialized
    assert "ROAD_A_PEOPLE" not in serialized


def test_grounded_context_degrades_when_optional_sources_fail(isolated):
    class RaisingHistorical:
        def get_historical_context_for_event(self, *args, **kwargs):
            raise RuntimeError("history unavailable")

    class RaisingKnowledge:
        def get_context_for_event(self, *args, **kwargs):
            raise RuntimeError("knowledge unavailable")

    class RaisingCases:
        def get_case_context_for_event(self, *args, **kwargs):
            raise RuntimeError("case memory unavailable")

    repo = isolated["regionalRepo"]
    _seed_event("E_DEGRADED", analyzed_at="2026-06-30T08:00:00Z")
    _bind_event(repo, "E_DEGRADED")

    context = GroundedEventContextAssembler(
        regional_repository=repo,
        historical_service=RaisingHistorical(),
        knowledge_service=RaisingKnowledge(),
        case_memory_service=RaisingCases(),
    ).assemble("E_DEGRADED").to_dict()

    assert context["groundingStatus"] == "PARTIAL"
    assert context["regionalContext"]["status"] == "READY"
    assert context["historicalContext"]["status"] == "UNAVAILABLE"
    assert context["knowledgeContext"]["status"] == "UNAVAILABLE"
    assert context["caseMemoryContext"]["status"] == "UNAVAILABLE"
    assert context["currentEvent"]["eventId"] == "E_DEGRADED"
