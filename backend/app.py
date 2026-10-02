"""
TrafficMind Agent - FastAPI 主应用
--------------------------------
提供交通事件智能分析 API：
  POST /analyze_event     - 分析交通事件
  GET  /history           - 查询历史记录
  GET  /event/{event_id}  - 查询单条事件详情
  POST /event/{event_id}/status - 更新事件状态
"""

import sys
import os
import json
import uuid
from datetime import datetime
from contextlib import asynccontextmanager

# 确保 backend 的父目录在 sys.path 中，以便 `from backend.xxx` 可正常工作
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict
from typing import Any, Dict, List, Optional

from backend.agent.graph import build_graph
from backend.tools.db_tools import (
    init_db,
    get_history,
    get_event_by_id,
    update_event_status,
    get_stats,
)
from backend.tools.event_identity import (
    EventIdentityError,
    compact_event_context,
    extract_event_id,
    hydrate_authoritative_event,
)
from backend.config import EVENT_STATUSES, LLM_ENABLED

# -------------------- 应用生命周期 --------------------

def _new_collaboration_session_id() -> str:
    return f"sess_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"


def seed_workflow_templates(repo: Optional[Any] = None) -> int:
    """Seed bundled workflow templates once per business template identity."""
    from backend.workflow.templates import get_all_templates
    from backend.workflow.repository import SQLiteWorkflowRepository

    wf_repo = repo or SQLiteWorkflowRepository()
    seeded = 0
    for build_fn in get_all_templates():
        try:
            definition = build_fn()
            existing = wf_repo.get_definition(definition.id)
            if existing is None:
                existing = wf_repo.find_definition_by_template_identity(
                    definition.name,
                    definition.category,
                )
            if existing is None:
                wf_repo.save_definition(definition)
                seeded += 1
                print(f"  [Workflow Seed] {definition.id}: {definition.name}")
        except Exception:
            pass  # 模板 seeding 失败不阻止启动
    return seeded


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用启动时初始化数据库。"""
    init_db()
    # Phase 6: Chat tables
    from backend.chat.chat_db import init_chat_tables
    init_chat_tables()
    # Phase 9: Collaboration tables (idempotent)
    from backend.agent.collaboration.db_repository import init_collaboration_tables
    init_collaboration_tables()
    # Phase 10: Memory V2 tables (idempotent)
    from backend.memory.store import init_memory_tables
    init_memory_tables()
    # Phase 12: Workflow V1 tables (idempotent)
    from backend.workflow.repository import init_workflow_tables
    init_workflow_tables()
    from backend.workflow.wait_scheduler import _migrate_wait_columns
    _migrate_wait_columns()
    # Phase 13: Simulation tables (idempotent)
    from backend.simulation.repository import init_simulation_tables
    init_simulation_tables()
    # Phase 21: Regional Core tables (idempotent)
    from backend.regional.repository import init_regional_tables
    init_regional_tables()
    # Phase 21: Traffic Case Memory tables (idempotent)
    from backend.case_memory.repository import init_case_memory_tables
    init_case_memory_tables()
    # Phase 13 Round 2: Seed simulation_bridge template
    seed_workflow_templates()
    # Phase 12: Wait Scheduler
    from backend.workflow.wait_scheduler import get_wait_scheduler
    wait_scheduler = get_wait_scheduler()
    await wait_scheduler.start()
    # Phase 17 Round 3: Planning RunDriver
    from backend.workflow.run_driver import get_run_driver
    run_driver = get_run_driver()
    await run_driver.start()
    llm_status = "已启用 (DeepSeek)" if LLM_ENABLED else "未配置，将使用本地模板"
    print(f"TrafficMind Agent 启动完成")
    print(f"  LLM 状态: {llm_status}")
    print(f"  API 文档: http://localhost:8000/docs")
    yield
    await run_driver.stop()
    await wait_scheduler.stop()


# -------------------- 创建 FastAPI 应用 --------------------

app = FastAPI(
    title="TrafficMind Agent API",
    description="面向智慧交通的事件研判与闭环处置 Agent - 第一阶段 MVP",
    version="1.0.0",
    lifespan=lifespan,
)

# 允许跨域（方便后续前端对接）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------------------- 请求/响应模型 --------------------

class AnalyzeEventRequest(BaseModel):
    """分析事件请求体（宽松模式，额外字段会被保留）"""
    eventId: str
    eventType: str
    cameraId: Optional[str] = ""
    roadName: str
    direction: Optional[str] = ""
    lane: Optional[str] = ""
    avgSpeed: float
    queueLength: float
    duration: float
    vehicleCount: Optional[int] = 0
    weather: Optional[str] = "clear"
    timePeriod: Optional[str] = "off_peak"
    isMainRoad: Optional[bool] = False
    nearbySchool: Optional[bool] = False
    nearbyHospital: Optional[bool] = False
    confidence: Optional[float] = 0.9

    model_config = ConfigDict(extra="allow")  # 允许额外字段


class StatusUpdateRequest(BaseModel):
    """状态更新请求体"""
    status: str


# -------------------- API 接口 --------------------

@app.post("/analyze_event", summary="分析交通事件")
async def analyze_event(request: AnalyzeEventRequest):
    """
    输入一条交通事件 JSON，Agent 自动完成：
      1. 事件解析与标准化
      2. 风险评分与等级判定
      3. 预案匹配
      4. 处置建议生成
      5. 调度话术生成
      6. 结构化报告生成
      7. 结果持久化保存

    返回完整的分析结果 JSON。
    """
    # 构建初始状态
    raw_event = request.model_dump()
    initial_state = {
        "raw_event": raw_event,
        "standard_event": {},
        "risk_result": {},
        "matched_rule": {},
        "suggestions": [],
        "dispatch_message": "",
        "public_message": "",
        "report": "",
        "result": {},
        "step": "",
        "error": None,
    }

    # 执行 LangGraph 工作流
    graph = build_graph()
    final_state = graph.invoke(initial_state)

    # 如果工作流出错
    if final_state.get("error"):
        raise HTTPException(status_code=400, detail=final_state["error"])

    # 返回结果
    return final_state.get("result", {})


@app.get("/history", summary="查询历史事件记录")
async def history(limit: int = 50):
    """
    查询 SQLite 中保存的历史事件分析记录。
    按更新时间倒序排列，默认返回最近 50 条。

    Args:
        limit: 最大返回条数（默认50）
    """
    records = get_history(limit=limit)
    return {"total": len(records), "records": records}


# ── Phase 20: 真实事件批量导入（确定性管线）────────────────────────────────
# 复用既有 标准化/评分/存储 路径；无 LLM、无 Agent 触发、无 Workflow、无删除。

EVENT_IMPORT_MAX_BATCH = 200
# 仅允许 Event 模型字段：白名单过滤，其余键一律丢弃、不持久化
EVENT_IMPORT_ALLOWED_FIELDS = {
    "eventId", "eventType", "cameraId", "roadName", "direction", "lane",
    "avgSpeed", "queueLength", "duration", "vehicleCount", "confidence",
    "weather", "timePeriod", "isMainRoad", "nearbySchool", "nearbyHospital",
}


class EventImportRequest(BaseModel):
    """批量导入事件请求体。"""
    events: List[Dict[str, Any]]


class EventIngestRequest(BaseModel):
    """Canonical single-event ingestion request."""
    source: str
    sourceEventId: str
    event: Dict[str, Any]
    occurredAt: Optional[str] = ""
    sourceMetadata: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(extra="forbid")


class EventReplayRequest(BaseModel):
    """Safe replay request.  Replay never grants action execution authority."""
    sessionId: Optional[str] = None
    content: Optional[str] = None

    model_config = ConfigDict(extra="forbid")


@app.post("/events/ingest", summary="接入一个 canonical 交通事件")
async def ingest_canonical_event(body: EventIngestRequest):
    from backend.event_ingestion import EventIngestionError, ingest_event

    try:
        return ingest_event(
            source=body.source,
            source_event_id=body.sourceEventId,
            event=body.event,
            occurred_at=body.occurredAt or "",
            source_metadata=body.sourceMetadata,
        )
    except EventIngestionError as err:
        status_code = 409 if err.code == "ingestion_conflict" else 400
        raise HTTPException(
            status_code=status_code,
            detail={"code": err.code, "message": err.message},
        )


@app.post("/events/import", summary="批量导入真实交通事件（确定性管线）")
async def import_events(body: EventImportRequest):
    """
    批量导入事件，复用既有标准化/评分/存储路径（与 /analyze_event 同一存储表）。

    确定性管线：validate_event → standardize_event → calculate_risk_score → save。
    不调用 LLM、不触发任何 Agent、不启动 Workflow、不删除任何数据。
    已存在的 eventId 一律跳过（绝不静默覆盖）。逐条返回结果（fail-closed per item）。
    """
    from backend.tools.event_tools import validate_event, standardize_event
    from backend.tools.risk_tools import calculate_risk_score
    from backend.tools.report_tools import generate_event_report
    from backend.tools.db_tools import save_event_analysis

    if len(body.events) > EVENT_IMPORT_MAX_BATCH:
        raise HTTPException(
            status_code=400,
            detail=f"单次最多导入 {EVENT_IMPORT_MAX_BATCH} 条事件（收到 {len(body.events)} 条）",
        )

    imported, skipped = 0, 0
    results: List[Dict[str, Any]] = []
    for raw in body.events:
        if not isinstance(raw, dict):
            results.append({"eventId": None, "status": "failed", "message": "事件必须为 JSON 对象"})
            continue

        # 白名单过滤：仅保留 Event 模型字段，其余字段不持久化
        event = {k: v for k, v in raw.items() if k in EVENT_IMPORT_ALLOWED_FIELDS}
        event_id = str(event.get("eventId", "")).strip()

        ok, err = validate_event(event)
        if not ok:
            results.append({"eventId": event_id or None, "status": "failed", "message": err})
            continue
        if not event_id:
            results.append({"eventId": None, "status": "failed", "message": "eventId 不能为空"})
            continue

        # 重复检查：已存在的 eventId 绝不静默覆盖
        if get_event_by_id(event_id) is not None:
            results.append({"eventId": event_id, "status": "skipped", "message": "eventId 已存在，跳过（不覆盖）"})
            skipped += 1
            continue

        standardized = standardize_event(event)
        risk = calculate_risk_score(standardized)
        report_text = generate_event_report(standardized, risk, {"rule": ""}, [], "")
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        saved = save_event_analysis({
            "eventId": event_id,
            "standardEvent": standardized,
            "riskScore": risk.get("riskScore", 0),
            "riskLevel": risk.get("riskLevel", ""),
            "riskReasons": risk.get("riskReasons", []),
            "matchedRule": "",
            "suggestions": [],
            "dispatchMessage": "",
            "publicMessage": "",
            "report": report_text,
            "status": "待派单",
            "analyzedAt": now,
        })
        if not saved:
            results.append({"eventId": event_id, "status": "failed", "message": "数据库保存失败"})
            continue

        results.append({
            "eventId": event_id,
            "status": "imported",
            "message": f"已导入（{risk.get('riskLevel', '')}，{risk.get('riskScore', 0)} 分）",
        })
        imported += 1

    return {
        "total": len(body.events),
        "imported": imported,
        "skipped": skipped,
        "failed": len(body.events) - imported - skipped,
        "results": results,
    }


@app.get("/event/{event_id}", summary="查询单条事件详情")
async def get_event(event_id: str):
    """
    根据 eventId 查询单条事件的完整分析详情。

    Args:
        event_id: 事件编号（如 E202606290001）
    """
    record = get_event_by_id(event_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"事件 {event_id} 不存在")
    return record


@app.get("/events/{event_id}", summary="查询 canonical 事件及真实关系链")
async def get_canonical_event(event_id: str):
    from backend.event_ingestion import project_event_relationships

    record = get_event_by_id(event_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"事件 {event_id} 不存在")
    return {
        "event": record,
        "relationships": project_event_relationships(event_id),
    }


@app.get("/events/{event_id}/status", summary="查询 canonical 事件状态")
async def get_canonical_event_status(event_id: str):
    record = get_event_by_id(event_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"事件 {event_id} 不存在")
    return {
        "eventId": event_id,
        "status": record.get("status", ""),
        "revision": record.get("revision", 1),
        "updatedAt": record.get("updatedAt", ""),
    }


@app.post("/event/{event_id}/status", summary="更新事件状态")
async def update_status(event_id: str, body: StatusUpdateRequest):
    """
    更新事件处理状态。

    有效状态值：
        - 待研判
        - 待派单
        - 处置中
        - 已处置
        - 待复盘
        - 已归档

    Args:
        event_id: 事件编号
        body: { "status": "处置中" }
    """
    # 先检查事件是否存在
    record = get_event_by_id(event_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"事件 {event_id} 不存在")

    new_status = body.status
    if new_status not in EVENT_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"无效状态值 '{new_status}'。有效值: {', '.join(EVENT_STATUSES)}",
        )

    ok = update_event_status(event_id, new_status)
    if not ok:
        raise HTTPException(status_code=500, detail="状态更新失败")

    return {
        "eventId": event_id,
        "status": new_status,
        "message": f"事件状态已更新为「{new_status}」",
    }


# -------------------- 第二阶段新增接口 --------------------

# 导入新增工具模块
from backend.tools.similarity_tools import find_similar_cases, hybrid_similarity
from backend.tools.report_summary_tools import generate_daily_report, generate_weekly_report
from backend.tools.alert_tools import get_unclosed_events
from backend.tools.stat_tools import get_high_risk_roads
# Phase 3
from backend.rag.knowledge_indexer import build_knowledge_index
from backend.rag.semantic_retriever import semantic_search
from backend.rag.rag_service import rag_ask
from backend.rag.vector_store import get_collection_stats
# Phase 4
from backend.agent.react_agent import controlled_react_diagnose
from backend.agent.chat_stream import chat_stream_generator
from backend.agent.streaming import sse_event, sse_error

COLLABORATION_ORCHESTRATOR_ENABLED = os.getenv("COLLABORATION_ORCHESTRATOR_ENABLED", "true").lower() == "true"

from backend.agent.router import route_agents
from backend.agent.conflict_resolver import detect_conflicts, resolve_conflicts
from backend.agent.event_chain import build_event_chain
from backend.agent.multi_agent import multi_agent_analyze, CongestionAgent, AccidentAgent, SignalAgent, DispatchAgent
from backend.chat.chat_db import (
    create_session, list_sessions, get_session, delete_session,
    add_message, get_session_messages, update_session_title,
    get_memory_summary as get_chat_memory_summary,
)
from backend.chat.memory_manager import build_context_for_llm, update_memory_summary
from backend.rag.grounded_answer import generate_grounded_answer
from backend.rag.semantic_retriever import semantic_search


@app.get("/similar_cases/{event_id}", summary="查找历史相似案例")
async def similar_cases(
    event_id: str,
    limit: int = 5,
    min_score: float = 0.4,
):
    """
    根据事件编号查询历史相似案例。

    参数：
      - limit: 返回数量上限（默认 5）
      - min_score: 最低相似度阈值（默认 0.4）

    第一阶段使用规则相似度；第三阶段计划引入 Chroma/FAISS 做语义检索。
    """
    result = find_similar_cases(event_id, limit=limit, min_score=min_score)
    if result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@app.get("/reports/daily", summary="生成交通事件日报")
async def daily_report(date: Optional[str] = None):
    """
    生成某一天的交通事件日报。

    参数：
      - date: 日期，格式 YYYY-MM-DD，默认今天
    """
    return generate_daily_report(date)


@app.get("/reports/weekly", summary="生成交通事件周报")
async def weekly_report(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """
    生成交通事件周报。

    参数：
      - start_date: 开始日期，格式 YYYY-MM-DD，默认 7 天前
      - end_date: 结束日期，格式 YYYY-MM-DD，默认今天
    """
    return generate_weekly_report(start_date, end_date)


@app.get("/alerts/unclosed", summary="未闭环事件提醒")
async def unclosed_alerts(
    hours: int = 24,
    min_risk: str = "中风险",
):
    """
    查询未完成处置闭环的事件。

    参数：
      - hours: 查询最近多少小时内的事件（默认 24）
      - min_risk: 最低风险等级筛选（默认"中风险"）
    """
    valid_risks = {"低风险", "中风险", "高风险", "重大风险"}
    if min_risk not in valid_risks:
        raise HTTPException(
            status_code=400,
            detail=f"无效风险等级 '{min_risk}'。有效值: {', '.join(valid_risks)}",
        )
    return get_unclosed_events(hours=hours, min_risk=min_risk)


@app.get("/stats/high_risk_roads", summary="高风险路口 TopN 统计")
async def high_risk_roads(
    limit: int = 10,
    days: int = 7,
    min_risk: str = "高风险",
):
    """
    统计高风险事件多发的路口。

    参数：
      - limit: 返回数量上限（默认 10）
      - days: 统计最近多少天（默认 7）
      - min_risk: 最低风险等级筛选（默认"高风险"）
    """
    return get_high_risk_roads(limit=limit, days=days, min_risk=min_risk)


# -------------------- 第三阶段接口 --------------------

@app.post("/rag/rebuild_index", summary="重建 RAG 知识库索引")
async def rebuild_rag_index():
    result = build_knowledge_index()
    if not result.get("success"):
        raise HTTPException(status_code=500, detail=result.get("message", "索引构建失败"))
    return result


@app.get("/rag/search", summary="语义检索交通知识库")
async def rag_search(query: str, limit: int = 5, doc_type: Optional[str] = None,
                     event_type: Optional[str] = None, road_name: Optional[str] = None,
                     risk_level: Optional[str] = None):
    return semantic_search(query=query, limit=limit, doc_type=doc_type,
                           event_type=event_type, road_name=road_name, risk_level=risk_level)


class AskRequest(BaseModel):
    question: str
    limit: Optional[int] = 5


@app.post("/rag/ask", summary="RAG 交通知识库问答")
async def rag_ask_endpoint(body: AskRequest):
    return rag_ask(body.question, body.limit or 5)


@app.get("/rag/status", summary="查看向量库状态")
async def rag_status():
    return get_collection_stats()


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 11: RAG V2 生产化升级
# ═══════════════════════════════════════════════════════════════════════════════

try:
    from backend.rag.v2.pipeline import get_pipeline
    from backend.rag.v2.indexer import IncrementalIndexer, load_all_documents
    from backend.rag.v2.providers import get_embedding_provider, get_reranker_provider
    from backend.rag.v2.document_repository import (
        get_trace, get_latest_index_version,
    )
    from backend.rag.v2.models import (
        EvidenceState,
        IndexJobResult,
        RagAnswer,
        RagTrace,
    )
    _RAG_V2_AVAILABLE = True
except ImportError as e:
    _RAG_V2_AVAILABLE = False
    _rag_v2_import_error = str(e)


class RagV2SearchRequest(BaseModel):
    query: str
    top_k: Optional[int] = 10
    filters: Optional[Dict[str, Any]] = None
    session_id: Optional[str] = None
    event_thread_id: Optional[str] = None
    agent_id: Optional[str] = None


class RagV2AskRequest(BaseModel):
    question: str
    session_id: Optional[str] = None
    event_thread_id: Optional[str] = None
    include_trace: Optional[bool] = True


@app.post("/rag/v2/search", summary="[Phase 11] RAG V2 混合检索")
async def rag_v2_search(body: RagV2SearchRequest):
    """Dense + BM25 + Structured → RRF → Reranker 完整检索管线。"""
    if not _RAG_V2_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail=f"RAG V2 不可用: {_rag_v2_import_error}",
        )
    pipeline = get_pipeline()
    result = pipeline.search(
        query=body.query,
        top_k=body.top_k or 10,
        filters=body.filters,
    )
    return result


@app.post("/rag/v2/ask", summary="[Phase 11] RAG V2 知识库问答")
async def rag_v2_ask(body: RagV2AskRequest):
    """端到端 RAG V2 问答：检索→重排→证据评估→生成带引用答案。"""
    if not _RAG_V2_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail=f"RAG V2 不可用: {_rag_v2_import_error}",
        )
    pipeline = get_pipeline()
    answer: RagAnswer = pipeline.ask(question=body.question)
    resp = answer.model_dump(mode="json")
    if body.include_trace and answer.trace_id:
        try:
            trace = get_trace(answer.trace_id)
            if trace:
                resp["trace"] = trace.model_dump(mode="json")
        except Exception:
            pass
    return resp


@app.post("/rag/v2/index", summary="[Phase 11] RAG V2 增量索引")
async def rag_v2_index():
    """增量索引：加载所有知识源文档，checksum 去重，仅更新变化部分。"""
    if not _RAG_V2_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail=f"RAG V2 不可用: {_rag_v2_import_error}",
        )
    try:
        docs = load_all_documents()
        emb_provider = get_embedding_provider()
        indexer = IncrementalIndexer(emb_provider)
        job: IndexJobResult = indexer.index_documents(docs, cleanup_stale=True)
        return job.model_dump(mode="json")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"索引失败: {e}")


@app.get("/rag/v2/status", summary="[Phase 11] RAG V2 索引状态")
async def rag_v2_status():
    """返回最新索引版本信息和文档统计。"""
    if not _RAG_V2_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail=f"RAG V2 不可用: {_rag_v2_import_error}",
        )
    try:
        ver = get_latest_index_version()
        emb = get_embedding_provider()
        reranker = get_reranker_provider()
        from backend.rag.v2.dense_index import get_collection_count, get_active_collection_name
        active_coll = get_active_collection_name()
        return {
            "rag_v2_enabled": True,
            "index_version": ver.model_dump(mode="json") if ver else None,
            "configured_embedding_model": emb.get_model_name(),
            "resolved_embedding_model": emb.get_resolved_model_name(),
            "embedding_dimension": emb.get_dimension(),
            "embedding_provider_class": type(emb).__name__,
            "embedding_degraded": emb.is_degraded(),
            "embedding_degraded_reason": emb.get_degraded_reason(),
            "configured_reranker_model": reranker.get_model_name(),
            "resolved_reranker_model": reranker.get_resolved_model_name(),
            "reranker_degraded": reranker.is_degraded(),
            "reranker_degraded_reason": reranker.get_degraded_reason(),
            "active_collection": active_coll,
            "chunks_in_chroma": get_collection_count(active_coll),
            "v1_collection_preserved": True,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"状态查询失败: {e}")


@app.get("/rag/v2/traces/{trace_id}", summary="[Phase 11] RAG V2 查询 Trace")
async def rag_v2_get_trace(trace_id: str):
    """查询指定 trace_id 的完整 RAG 追踪记录。"""
    if not _RAG_V2_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail=f"RAG V2 不可用: {_rag_v2_import_error}",
        )
    trace: Optional[RagTrace] = get_trace(trace_id)
    if trace is None:
        raise HTTPException(status_code=404, detail=f"Trace {trace_id} 不存在")
    return trace.model_dump(mode="json")


@app.post("/rag/v2/ask/stream", summary="[Phase 11] RAG V2 流式问答（SSE）")
async def rag_v2_ask_stream(body: RagV2AskRequest):
    """SSE 流式 RAG V2 问答。

    事件类型: rag_route_done → rag_query_rewritten → rag_candidates_retrieved
    → rag_rerank_done → rag_evidence_selected → rag_abstained (if insufficient)
    → delta → rag_trace_ready → done
    """
    if not _RAG_V2_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail=f"RAG V2 不可用: {_rag_v2_import_error}",
        )
    from backend.agent.streaming import sse_event
    from fastapi.responses import StreamingResponse
    import asyncio

    async def _stream():
        try:
            pipeline = get_pipeline()
            async for event in pipeline.ask_stream(question=body.question):
                event_name = event.get("event", "message")
                event_data = event.get("data", {})
                yield sse_event(event_name, event_data)
                await asyncio.sleep(0.01)
        except Exception as e:
            import traceback
            # Log full traceback server-side only, never send to client
            print(f"[RAG SSE ERROR] {traceback.format_exc()}")
            # Send safe error to client: no traceback, no local paths
            error_msg = str(e).split("\n")[0][:200]  # first line only, capped
            yield sse_event("error", {
                "message": error_msg,
                "details": "An internal error occurred during RAG processing."
            })
            yield sse_event("done", {"error": True, "degraded": True})

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/similar_cases_hybrid/{event_id}", summary="混合相似案例检索")
async def similar_cases_hybrid(event_id: str, limit: int = 5, min_score: float = 0.4):
    result = hybrid_similarity(event_id, limit=limit, min_score=min_score)
    if result.get("error") or result.get("currentEvent") is None:
        raise HTTPException(status_code=404, detail=result.get("error", f"事件 {event_id} 不存在"))
    return result


class MultiAgentRequest(BaseModel):
    eventId: str; eventType: str; roadName: str
    direction: Optional[str] = ""; avgSpeed: float; queueLength: float; duration: float
    vehicleCount: Optional[int] = 0; weather: Optional[str] = "clear"
    timePeriod: Optional[str] = "off_peak"; isMainRoad: Optional[bool] = False
    nearbySchool: Optional[bool] = False; nearbyHospital: Optional[bool] = False
    confidence: Optional[float] = 0.9
    model_config = ConfigDict(extra="allow")


@app.post("/agent/multi_analyze", summary="多 Agent 协同研判")
async def multi_agent_analyze_endpoint(body: MultiAgentRequest):
    return multi_agent_analyze(body.model_dump())


# -------------------- 第四阶段接口 --------------------

class ReactDiagnoseRequest(BaseModel):
    question: str
    max_steps: Optional[int] = 4


@app.post("/agent/react_diagnose", summary="受控 ReAct 诊断分析")
async def react_diagnose(body: ReactDiagnoseRequest):
    """
    受控 ReAct 诊断 Agent。只能调用只读工具（白名单），
    不允许修改状态、推送通知、改变风险评分。
    每步输出 thought/action/observation，最多 max_steps 步。
    """
    result = controlled_react_diagnose(body.question, body.max_steps or 4)
    return result


class RoutedAnalyzeRequest(BaseModel):
    eventId: str; eventType: str; roadName: str
    direction: Optional[str] = ""; avgSpeed: float; queueLength: float; duration: float
    vehicleCount: Optional[int] = 0; weather: Optional[str] = "clear"
    timePeriod: Optional[str] = "off_peak"; isMainRoad: Optional[bool] = False
    nearbySchool: Optional[bool] = False; nearbyHospital: Optional[bool] = False
    confidence: Optional[float] = 0.9
    model_config = ConfigDict(extra="allow")


@app.post("/agent/routed_analyze", summary="动态路由多 Agent 协同研判（含冲突检测）")
async def routed_analyze(body: RoutedAnalyzeRequest):
    """
    第四阶段增强版多 Agent 协同研判：
    1. 动态路由选择 Agent
    2. 各 Agent 独立分析
    3. 冲突检测与融合
    4. 事件驱动链式协同
    """
    event_info = body.model_dump()
    event_info["eventTypeCn"] = event_info.get("eventType", "")  # will be normalized later

    # Step 1: 路由
    routing = route_agents(event_info)

    # Step 2: 执行选中的 Agent
    agent_map = {
        "CongestionAgent": CongestionAgent,
        "AccidentAgent": AccidentAgent,
        "SignalAgent": SignalAgent,
        "DispatchAgent": DispatchAgent,
    }

    # Normalize event info for agents (add defaults for computed fields)
    from backend.agent.multi_agent import _get_event_info
    normalized_info = _get_event_info(event_info)

    agent_results = []
    for name in routing["selectedAgents"]:
        cls = agent_map.get(name)
        if cls:
            result = cls().analyze(normalized_info)
            agent_results.append(result)

    # Step 3: 事件驱动链
    chain_result = build_event_chain(normalized_info, agent_results)

    # Step 4: 冲突检测与融合
    conflicts = detect_conflicts(agent_results)
    resolution = resolve_conflicts(conflicts, agent_results, normalized_info)

    # Step 5: 风险警告
    risk_warnings = list(resolution.get("riskWarnings", []))
    if normalized_info.get("weather", "clear") in ("rain", "snow", "fog"):
        risk_warnings.append(f"天气预警：{normalized_info['weather']}")

    # Step 6: 报告
    from datetime import datetime
    report_lines = [
        "=" * 50,
        "   TrafficMind Agent 动态路由协同研判报告",
        "=" * 50,
        f"事件类型：{normalized_info.get('eventType', event_info.get('eventType', ''))}",
        f"路段：{normalized_info['roadName']}",
        "",
        f"路由选择：{', '.join(routing['selectedAgents'])}",
        f"路由理由：{'；'.join(routing['routingReasons'][:3])}",
        "",
    ]
    for r in agent_results:
        report_lines.append(f"[{r.get('agentName', '')}] urgency={r.get('urgency', '')}")
        for f in r.get("findings", []):
            report_lines.append(f"  - {f}")
    if conflicts:
        report_lines.append(f"\n检测到 {len(conflicts)} 个建议冲突，已融合处理")
    report_lines += ["", "=" * 50]

    return {
        "eventSummary": {
            "eventType": event_info.get("eventType", ""),
            "roadName": event_info["roadName"],
            "analyzedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
        "selectedAgents": routing["selectedAgents"],
        "routingReasons": routing["routingReasons"],
        "agentResults": agent_results,
        "conflicts": conflicts,
        "resolvedPlan": resolution.get("resolvedPlan", {}),
        "finalDecision": resolution.get("finalDecision", ""),
        "dispatchPlan": {
            "urgency": resolution.get("resolvedPlan", {}).get("urgency", "low"),
            "actions": resolution.get("resolvedPlan", {}).get("mergedSuggestions", []),
        },
        "riskWarnings": risk_warnings,
        "report": "\n".join(report_lines),
        # 链式协同信息
        "eventChain": {
            "triggerReasons": chain_result.get("triggerReasons", []),
            "triggeredAgents": chain_result.get("finalPlan", {}).get("triggeredAgents", []),
        },
    }


# ==================== 第六阶段：Chat 会话 API ====================

@app.post("/chat/sessions", summary="创建新会话")
async def create_chat_session(body: Dict[str, Any] = {"mode": "react"}):
    sid = "sess_" + datetime.now().strftime("%Y%m%d%H%M%S") + "_" + str(int(datetime.now().timestamp() * 1000) % 100000)
    return create_session(sid, body.get("mode", "react"))


@app.get("/chat/sessions", summary="获取最近会话列表")
async def list_chat_sessions(limit: int = 30):
    return {"sessions": list_sessions(limit)}


@app.get("/chat/sessions/{session_id}", summary="获取会话详情")
async def get_chat_session(session_id: str):
    session = get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    messages = get_session_messages(session_id)
    mem = get_chat_memory_summary(session_id) or {}
    return {"session": session, "messages": messages, "memorySummary": mem}


@app.patch("/chat/sessions/{session_id}/title", summary="重命名会话")
async def rename_chat_session(session_id: str, body: Dict[str, Any]):
    session = get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    from backend.chat.chat_db import update_session_title
    update_session_title(session_id, body.get("title", "")[:50])
    return {"success": True, "title": body.get("title", "")[:50]}


@app.delete("/chat/sessions/{session_id}", summary="删除会话")
async def delete_chat_session(session_id: str):
    ok = delete_session(session_id)
    if not ok:
        raise HTTPException(status_code=404, detail="会话不存在")
    return {"success": True, "sessionId": session_id}


class ChatMessageRequest(BaseModel):
    content: str
    mode: Optional[str] = "react"


@app.post("/chat/sessions/{session_id}/messages", summary="发送消息并获取回答")
async def send_chat_message(session_id: str, body: ChatMessageRequest):
    session = get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")

    q = body.content.strip()
    if not q:
        raise HTTPException(status_code=400, detail="消息内容不能为空")
    mode = body.mode or "react"

    # 1. Save user message
    user_msg_id = "um_" + str(int(datetime.now().timestamp() * 1000))
    add_message(user_msg_id, session_id, "user", q, mode)

    # 2. Build context from memory
    memory_ctx = ""
    try:
        memory_ctx = build_context_for_llm(session_id, q)
    except Exception:
        pass

    # 3. RAG retrieval + grounded answer
    rag_results = semantic_search(q, limit=10).get("results", [])
    ga = generate_grounded_answer(q, rag_results, memory_ctx, mode)

    # 4. Save assistant message
    asst_msg_id = "am_" + str(int(datetime.now().timestamp() * 1000))
    result_summary = {
        "confidence": ga["confidence"],
        "usedLLM": ga.get("usedLLM", False),
        "abstained": ga.get("abstained", False),
        "evidenceCount": len(ga.get("evidence", [])),
    }
    add_message(asst_msg_id, session_id, "assistant", ga["answer"], mode, result_summary)

    # 5. Update title if first message
    if session.get("title") == "新对话":
        from backend.chat.chat_db import update_session_title
        update_session_title(session_id, q[:20])

    # 6. Update memory summary
    try:
        update_memory_summary(session_id)
    except Exception:
        pass

    return {
        "sessionId": session_id,
        "userMessage": {"id": user_msg_id, "role": "user", "content": q, "mode": mode},
        "assistantMessage": {
            "id": asst_msg_id, "role": "assistant", "content": ga["answer"],
            "mode": mode, "confidence": ga["confidence"],
            "abstained": ga.get("abstained", False), "usedLLM": ga.get("usedLLM", False),
        },
        "evidence": ga.get("evidence", []),
        "confidence": ga["confidence"],
        "abstained": ga.get("abstained", False),
        "warnings": ga.get("warnings", []),
    }


# -------------------- Phase 8: SSE 流式接口 --------------------

from fastapi.responses import StreamingResponse
from backend.agent.multi_agent import _get_event_info


class StreamRequest(BaseModel):
    sessionId: Optional[str] = None
    content: str
    mode: Optional[str] = "react"


VALID_MODES = {"react", "routed", "rag", "hybrid", "report", "collaboration"}


@app.post("/chat/stream", summary="Chat SSE 流式对话")
async def chat_stream(body: StreamRequest):
    """SSE 流式对话。支持 react/routed/rag/hybrid/report/collaboration。"""
    m = body.mode or "react"
    if m not in VALID_MODES:
        m = "react"

    async def event_generator():
        try:
            async for event in chat_stream_generator(session_id=body.sessionId, content=body.content, mode=m):
                yield event
        except Exception as e:
            yield sse_error(str(e))
    return StreamingResponse(event_generator(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


class RoutedStreamRequest(BaseModel):
    sessionId: Optional[str] = None
    content: Optional[str] = None  # NL content for parsing
    eventId: Optional[str] = None
    eventType: str = "congestion"
    roadName: str = "未知路段"
    direction: Optional[str] = ""
    avgSpeed: Optional[float] = None
    queueLength: Optional[float] = None
    duration: Optional[float] = None
    weather: Optional[str] = "clear"
    timePeriod: Optional[str] = "off_peak"
    isMainRoad: Optional[bool] = False
    nearbySchool: Optional[bool] = False
    nearbyHospital: Optional[bool] = False
    confidence: Optional[float] = 0.9
    contextPolicy: Optional[str] = "fresh_event"
    model_config = ConfigDict(extra="allow")


def _event_identity_http_error(err: EventIdentityError) -> HTTPException:
    status = {
        "event_not_found": 404,
        "event_id_mismatch": 409,
        "event_session_conflict": 409,
        "event_session_corrupt": 409,
        "event_terminal": 409,
    }.get(err.code, 400)
    return HTTPException(status_code=status, detail={"code": err.code, "message": err.message})


def _prepare_event_bound_stream(body: RoutedStreamRequest) -> Optional[Dict[str, Any]]:
    event_id = extract_event_id(body.model_dump())
    if not event_id:
        return None
    try:
        snapshot = hydrate_authoritative_event(event_id, client_event=body.model_dump())
        if body.sessionId:
            from backend.agent.collaboration.db_repository import SQLiteCollaborationRepository
            bound_ids = SQLiteCollaborationRepository().get_session_bound_event_ids(body.sessionId)
            if len(bound_ids) > 1:
                raise EventIdentityError(
                    "event_session_corrupt",
                    f"会话 {body.sessionId} 已存在多个事件绑定，拒绝继续写入",
                )
            if bound_ids and bound_ids[0] != event_id:
                raise EventIdentityError(
                    "event_session_conflict",
                    f"会话 {body.sessionId} 已绑定事件 {bound_ids[0]}，不能写入事件 {event_id}",
                )
        return snapshot
    except EventIdentityError as err:
        raise _event_identity_http_error(err)


@app.post("/agent/routed_analyze/stream", summary="协同分析 SSE 流式")
async def routed_analyze_stream(body: RoutedStreamRequest):
    """多 Agent 协同分析 SSE 流式。

    COLLABORATION_ORCHESTRATOR_ENABLED=true → Orchestrator 执行
    COLLABORATION_ORCHESTRATOR_ENABLED=false → 旧实现
    """
    authoritative_event = _prepare_event_bound_stream(body)
    if COLLABORATION_ORCHESTRATOR_ENABLED:
        return await _orchestrated_analyze_stream(
            body,
            authoritative_event=authoritative_event,
            runtime_metadata={"runKind": "live", "actionExecutionAllowed": True},
        )
    return await _legacy_analyze_stream(
        body,
        authoritative_event=authoritative_event,
        runtime_metadata={"runKind": "live", "actionExecutionAllowed": True},
    )


@app.post("/events/{event_id}/replay", summary="安全重放历史事件到 Agent 分析")
async def replay_canonical_event(event_id: str, body: EventReplayRequest = None):
    """Start a separately identified Agent replay for an existing event.

    Replay keeps the original ``eventId`` for lineage, creates a new Agent run,
    does not advance the event lifecycle, and carries an explicit
    ``actionExecutionAllowed=false`` marker into the persisted run.
    """
    try:
        snapshot = hydrate_authoritative_event(event_id)
    except EventIdentityError as err:
        raise _event_identity_http_error(err)

    request = body or EventReplayRequest()
    replay_id = f"replay_{uuid.uuid4().hex[:16]}"
    event_ctx = compact_event_context(snapshot)
    replay_body = RoutedStreamRequest(
        sessionId=request.sessionId,
        content=request.content or f"重放事件 {event_id}，仅重新研判，不执行外部动作",
        eventId=event_id,
        eventType=event_ctx.get("eventType") or "congestion",
        roadName=event_ctx.get("roadName") or "未知路段",
        direction=event_ctx.get("direction") or "",
        avgSpeed=event_ctx.get("avgSpeed"),
        queueLength=event_ctx.get("queueLength"),
        duration=event_ctx.get("duration"),
        weather=event_ctx.get("weather") or "clear",
        timePeriod=event_ctx.get("timePeriod") or "off_peak",
        isMainRoad=bool(event_ctx.get("isMainRoad")),
        nearbySchool=bool(event_ctx.get("nearbySchool")),
        nearbyHospital=bool(event_ctx.get("nearbyHospital")),
        contextPolicy="fresh_event",
        runKind="replay",
        replayId=replay_id,
        replayOfEventId=event_id,
        actionExecutionAllowed=False,
    )
    # Replay always uses the durable orchestrator path so every replay has its
    # own persisted Agent run identity even when legacy live analysis is enabled.
    response = await _orchestrated_analyze_stream(
        replay_body,
        authoritative_event=snapshot,
        runtime_metadata={
            "runKind": "replay",
            "replayId": replay_id,
            "replayOfEventId": event_id,
            "actionExecutionAllowed": False,
        },
    )
    response.headers["X-TrafficMind-Replay-Id"] = replay_id
    return response


async def _run_memory_extraction(
    session_id: str, run_id: str,
    user_message_id: str, assistant_message_id: str,
    user_input: str, current_event: Dict[str, Any],
    selected_agents: List[str],
    conflicts: List[Dict], arbitration_results: List[Dict],
    fusion_summary: str, final_decision: Any,
    requires_human_review: bool, run_status: str,
    degraded: bool = False,
    event_thread_id: str = "",
    is_first_round: bool = False,
) -> Optional[Dict[str, Any]]:
    """Phase 10 Memory V2: 运行抽取并写入结构化记忆。

    从 CollaborationRepository 读取 Run 最终状态，不依赖 SSE 字符串解析。
    即使无候选也写入 Trace（no_op），失败时发送 memory_write_failed。
    """
    try:
        from backend.memory.coordinator import MemoryCoordinator
        from backend.memory.factory import create_memory_repository
        from backend.memory.models import MemoryTrace
        from backend.agent.collaboration.db_repository import SQLiteCollaborationRepository

        # === Read Run state from collaboration repository (source of truth) ===
        collab_repo = SQLiteCollaborationRepository()
        run_data = collab_repo.get_run(run_id)

        # Derive fields from repository data
        _run_status = run_data.get("status", run_status) if run_data else run_status

        # Parse final_decision from repository
        _fd = final_decision
        if run_data and not _fd:
            raw_fd = run_data.get("final_decision", "")
            if isinstance(raw_fd, str) and raw_fd:
                try:
                    _fd = json.loads(raw_fd)
                except Exception:
                    _fd = {"fusionSummary": raw_fd}
        tasks = []
        conflict_records = []
        try:
            import sqlite3 as _sq
            from backend.config import DB_PATH as _dbp
            conn = _sq.connect(_dbp)
            conn.row_factory = _sq.Row
            trows = conn.execute(
                "SELECT * FROM collaboration_tasks WHERE run_id=? AND status='succeeded'",
                (run_id,),
            ).fetchall()
            tasks = [dict(r) for r in trows]
            crows = conn.execute(
                "SELECT * FROM collaboration_conflicts WHERE run_id=?", (run_id,)
            ).fetchall()
            conflict_records = [dict(r) for r in crows]
            conn.close()
        except Exception:
            pass

        # Build agent_results from tasks
        agent_results = []
        for t in tasks:
            output = t.get("output_snapshot", "{}")
            if isinstance(output, str):
                try:
                    output = json.loads(output)
                except Exception:
                    output = {}
            if output:
                agent_results.append({
                    "agentName": t.get("agent_name", ""),
                    "suggestion": output.get("suggestion", ""),
                    "findings": output.get("findings", []),
                    "urgency": output.get("urgency", "low"),
                    "confidence": output.get("confidence", 0),
                })

        # Use repository-derived final_decision if not already captured
        fd = _fd

        coordinator = MemoryCoordinator()
        result = coordinator.extract_and_write(
            session_id=session_id,
            run_id=run_id,
            user_message_id=user_message_id,
            assistant_message_id=assistant_message_id,
            user_input=user_input,
            current_event=current_event,
            selected_agents=selected_agents,
            agent_results=agent_results,
            conflicts=conflicts or [],
            arbitration_results=arbitration_results or [],
            fusion_summary=fusion_summary or "",
            final_decision=fd or {},
            requires_human_review=requires_human_review,
            run_status=_run_status or run_status,
            degraded=degraded,
            is_first_round=is_first_round,
            event_thread_id=event_thread_id,
        )
        return result
    except Exception:
        import traceback
        traceback.print_exc()
        # Write a no_op trace so the run is never without a trace record
        try:
            repo = create_memory_repository()
            repo.merge_trace(MemoryTrace(
                trace_id=f"memtrace_{run_id}", run_id=run_id,
                session_id=session_id, recall_intent="write_failed",
                write_results_json=json.dumps([
                    {"action": "no_op", "reason": f"memory_extraction_error: {str(e)[:100]}"}
                ], ensure_ascii=False),
                write_latency_ms=0, event_thread_id=event_thread_id,
            ), phase="write")
        except Exception:
            pass
        return {
            "runId": run_id, "sessionId": session_id,
            "candidateCount": 0, "createdCount": 0, "deduplicatedCount": 0,
            "supersededCount": 0, "rejectedCount": 0, "confirmedCount": 0,
            "latencyMs": 0, "traceId": "", "writeResults": [
                {"action": "no_op", "reason": f"memory_extraction_error: {str(e)[:100]}"}
            ], "error": str(e)[:200],
        }


async def _orchestrated_analyze_stream(
    body: RoutedStreamRequest,
    authoritative_event: Optional[Dict[str, Any]] = None,
    runtime_metadata: Optional[Dict[str, Any]] = None,
):
    """使用 CollaborationOrchestrator 执行协同分析。"""
    import asyncio as _asyncio
    from backend.agent.multi_agent import _get_event_info
    from backend.agent.router import route_agents
    from backend.agent.collaboration.orchestrator import CollaborationOrchestrator, AgentExecutionResult
    from backend.agent.collaboration.budget import ExecutionBudget

    async def agent_stream():
        from backend.memory.coordinator import MemoryCoordinator
        from backend.agent.collaboration.event_parser import parse_content_to_event, build_current_event
        from backend.agent.collaboration.db_repository import load_previous_run_context

        # ===== Step 1: Parse CURRENT message ONLY =====
        content_text = body.content or (body.model_dump().get("content", ""))
        context_policy = body.contextPolicy or "fresh_event"

        nl_parsed = {}
        if authoritative_event:
            nl_parsed = compact_event_context(authoritative_event)
        elif content_text:
            nl_parsed = parse_content_to_event(content_text)

        # ===== Step 2: Build currentEvent — STRICTLY from current message =====
        # NEVER merge previous run data into currentEvent
        explicit = body.model_dump()
        if authoritative_event:
            explicit = {**explicit, **compact_event_context(authoritative_event)}
        current_event = build_current_event(nl_parsed, explicit, context_policy)
        field_sources = current_event.get("fieldSources", {})
        if authoritative_event:
            for key in compact_event_context(authoritative_event):
                if key in current_event:
                    field_sources[key] = "authoritative_event_record"
            current_event["snapshotSource"] = "event_records"
            current_event["capturedAt"] = authoritative_event.get("capturedAt", "")
            current_event["fieldSources"] = field_sources
        # Phase 21 replay metadata is server-owned and persisted with the Agent
        # run.  It is intentionally separate from the canonical event row.
        # Runtime safety metadata is server-owned.  RoutedStreamRequest keeps
        # ``extra=allow`` for backwards-compatible traffic fields, but public
        # callers cannot turn a live request into a replay (or re-enable a
        # replay action) by smuggling undeclared JSON keys.
        owned_metadata = dict(runtime_metadata or {})
        run_kind = "replay" if owned_metadata.get("runKind") == "replay" else "live"
        current_event["runKind"] = run_kind
        current_event["actionExecutionAllowed"] = run_kind != "replay"
        if run_kind == "replay":
            current_event["replayId"] = str(owned_metadata.get("replayId") or "")
            current_event["replayOfEventId"] = str(
                owned_metadata.get("replayOfEventId") or extract_event_id(current_event)
            )

        # ===== Step 3: Load previous run context — SEPARATE object =====
        sid = body.sessionId
        if not sid:
            sess = create_session(_new_collaboration_session_id(), "collaboration")
            sid = sess["sessionId"]
            yield sse_event("session_created", {"sessionId": sid})

        previous_run_context = load_previous_run_context(sid) if sid else None

        # ===== Step 4: Build info from currentEvent only =====
        raw = current_event
        raw["originalInput"] = content_text or ""
        raw["contextPolicy"] = context_policy
        raw["fieldSources"] = field_sources
        info = _get_event_info(raw)
        for key in ("runKind", "replayId", "replayOfEventId", "actionExecutionAllowed"):
            if key in current_event:
                info[key] = current_event[key]
        info["fieldSources"] = field_sources
        info["contextPolicy"] = context_policy
        info["originalInput"] = content_text or ""

        run_id = f"run_{int(datetime.now().timestamp() * 1000)}_{uuid.uuid4().hex[:8]}"
        trace_id = f"trace_{run_id}"
        if info.get("runKind") == "replay":
            yield sse_event("replay_started", {
                "replayId": info.get("replayId", ""),
                "eventId": info.get("replayOfEventId") or info.get("eventId", ""),
                "actionExecutionAllowed": False,
            })
        grounding_context = {}
        if authoritative_event:
            try:
                from backend.grounding.assembler import (
                    GroundedEventContextAssembler,
                    minimal_grounded_context_from_event,
                )

                grounding_context = GroundedEventContextAssembler().assemble(
                    extract_event_id(authoritative_event),
                    query=content_text or "",
                    authoritative_event=authoritative_event,
                ).to_dict()
            except Exception:
                grounding_context = minimal_grounded_context_from_event(
                    authoritative_event,
                    reason="GROUNDING_ASSEMBLY_ERROR",
                ).to_dict()

        # Generate title from user query content — not from default roadName
        query_text = content_text or ""
        if info.get("nearbySchool") and "冲突" in query_text:
            title = "学校门口信号冲突研判"
        elif info.get("nearbySchool"):
            title = "学校周边交通研判"
        elif info.get("conflictIntent"):
            title = "交通冲突协同研判"
        elif any(w in query_text for w in ["信号", "绿灯", "配时"]):
            title = "信号灯协同研判"
        elif any(w in query_text for w in ["事故", "碰撞", "追尾"]):
            title = "交通事故协同研判"
        elif any(w in query_text for w in ["拥堵", "堵车", "排队"]):
            title = "拥堵路段协同研判"
        elif info.get("roadName") and "未命名" not in str(info.get("roadName", "")):
            title = f"{str(info.get('roadName', ''))[:12]}协同研判"
        else:
            # Fallback: first 16 chars of user query
            title = (query_text[:16] or "协同分析") + "协同研判"
        # Ensure 8-16 chars
        title = title[:20]

        # Save user message
        user_content = query_text[:100] or "协同分析请求"
        um_id = f"um_{int(datetime.now().timestamp() * 1000)}"
        add_message(um_id, sid, "user", user_content, "collaboration")
        # Generate title on first round only — never overwrite on subsequent rounds
        from backend.chat.chat_db import get_session
        sess_check = get_session(sid)
        if sess_check and (not sess_check.get("title") or sess_check.get("title") == "新对话"):
            update_session_title(sid, title[:20])

        routing = route_agents(info)
        selected = routing["selectedAgents"][:4]  # Cap at 4 agents

        # ===== Phase 10 M3: Memory Recall Pipeline =====
        import copy as _copy
        _current_event_before_recall = _copy.deepcopy(info)

        yield sse_event("memory_recall_started", {
            "runId": run_id, "sessionId": sid, "timestamp": datetime.now().isoformat(),
        })

        _mem_coordinator = MemoryCoordinator()
        _recall_result = _mem_coordinator.recall_and_inject(
            session_id=sid, run_id=run_id,
            user_input=query_text or content_text or "",
            current_event=info,
            agent_targets=selected,
            context_policy=context_policy,
        )

        yield sse_event("memory_recall_completed", {
            "runId": run_id, "sessionId": sid,
            "eventThreadId": _recall_result.get("eventThreadId", ""),
            "intent": _recall_result.get("intent", ""),
            "candidateCount": _recall_result.get("candidateCount", 0),
            "selectedCount": _recall_result.get("selectedCount", 0),
            "rejectedCount": _recall_result.get("rejectedCount", 0),
            "latencyMs": _recall_result.get("latencyMs", 0),
            "tokenEstimate": _recall_result.get("tokenEstimate", 0),
        })

        # Verify currentEvent immutability
        assert _current_event_before_recall == info, (
            "currentEvent was mutated during recall — CRITICAL BUG"
        )

        # Build routingContext from recall result
        _routing_ctx = _recall_result.get("routingContext", {})
        if _routing_ctx:
            # Use routing-enhanced info for route_agents if available
            _routing_info = dict(info)
            for k, v in _routing_ctx.items():
                if k not in ("fieldSources",):
                    if _routing_info.get(k) in (None, "", False) and v is not None:
                        _routing_info[k] = v
            routing = route_agents(_routing_info)
            selected = routing["selectedAgents"][:4]

        # Build agent injection map stats for SSE
        _agent_map = _recall_result.get("agentInjectionMap", {})
        yield sse_event("memory_injection_ready", {
            "runId": run_id,
            "agentTargets": selected,
            "injectedItemCountByAgent": {
                a: m.get("itemCount", 0) for a, m in _agent_map.items()
            },
        })

        if _recall_result.get("error"):
            yield sse_event("memory_recall_failed", {
                "runId": run_id, "sessionId": sid,
                "errorCode": "recall_error",
                "safeMessage": "Memory recall encountered an error, continuing with empty context",
            })

        degraded = False
        fallback_reason = ""
        fusion_summary = ""
        # Phase 10 Memory V2 tracking
        run_status = "unknown"
        conflicts_list = []
        arbitration_list = []
        final_decision_for_memory = None
        requires_human_review_for_memory = False
        try:
            orchestrator = CollaborationOrchestrator()
            budget = ExecutionBudget(max_agents=4, max_agent_calls=2, max_retries=1, max_total_seconds=90)
            async for event_str in orchestrator.execute(
                run_id, sid, info, selected,
                routing.get("skippedAgents", []),
                routing.get("routingReasons", []), budget,
                previous_run_context=previous_run_context,
                grounding_context=grounding_context,
            ):
                # Capture fusionSummary from fusion_done event
                if 'event: fusion_done' in event_str:
                    import re as _re
                    match = _re.search(r'\"fusionSummary\":\s*\"([^\"]+)\"', event_str)
                    if match: fusion_summary = match.group(1)
                # Phase 10 Memory tracking
                if 'event: run_completed' in event_str:
                    run_status = "completed"
                elif 'event: run_partial_success' in event_str:
                    run_status = "partial_success"
                elif 'event: run_failed' in event_str:
                    run_status = "failed"
                yield event_str
            # The orchestrator emits a terminal run_* event.  The HTTP wrapper
            # emits the transport-level ``done`` event after memory persistence.
        except Exception as e:
            # Non-retryable: don't silently fallback
            if any(kw in str(e) for kw in ["ValidationError", "缺少", "未注册", "非法"]):
                run_status = "failed"
                yield sse_event("run_failed", {"runId": run_id, "reason": str(e)[:200], "errorCode": "agent_not_registered"})
                yield sse_event("done", {"runId": run_id, "sessionId": sid, "status": "failed"})
                return
            # System error: degraded fallback
            degraded = True; fallback_reason = str(e)
            yield sse_event("fallback_started", {"reason": fallback_reason, "fallbackFrom": "orchestrator"})
            async for ev in _legacy_analyze_stream_inner(
                body, sid, authoritative_event, runtime_metadata=owned_metadata
            ):
                yield ev
        finally:
            # Guard: ensure collaboration_runs.status is never left in "running" if we crash
            if run_status == "unknown":
                try:
                    from backend.agent.collaboration.db_repository import SQLiteCollaborationRepository
                    repo = SQLiteCollaborationRepository()
                    run_data = repo.get_run(run_id)
                    if run_data and run_data.get("status") not in ("completed", "partial_success", "failed", "interrupted"):
                        run_data["status"] = "failed"
                        run_data["updated_at"] = datetime.now().isoformat()
                        repo.update_run(run_data)
                    run_status = "failed"
                except Exception:
                    pass

        # Save assistant message with REAL fusion summary (not placeholder)
        # The CollaborationRepository commits terminal Agent truth and the
        # canonical 待研判→待派单 transition in one SQLite transaction.
        assistant_content = fusion_summary or "协同分析已完成，请查看运行详情获取融合决策。"
        am_id = f"am_{int(datetime.now().timestamp() * 1000)}"
        add_message(am_id, sid, "assistant", assistant_content, "collaboration",
                    {"runId": run_id, "executionEngine": "orchestrator", "degraded": degraded, "fusionSummary": assistant_content})

        # ===== Phase 10 Memory V2: 写入侧 =====
        if run_status != "failed":
            yield sse_event("memory_write_started", {
                "runId": run_id, "sessionId": sid,
                "timestamp": datetime.now().isoformat(),
            })
            _mem_result = await _run_memory_extraction(
                sid, run_id, um_id, am_id,
                query_text, info, selected,
                conflicts_list, arbitration_list,
                fusion_summary, final_decision_for_memory,
                requires_human_review_for_memory,
                run_status, degraded,
                event_thread_id=_recall_result.get("eventThreadId", ""),
                is_first_round=(previous_run_context is None),
            )
            if _mem_result:
                yield sse_event("memory_write_completed", {
                    "runId": _mem_result["runId"],
                    "sessionId": _mem_result["sessionId"],
                    "candidateCount": _mem_result["candidateCount"],
                    "createdCount": _mem_result["createdCount"],
                    "deduplicatedCount": _mem_result["deduplicatedCount"],
                    "supersededCount": _mem_result["supersededCount"],
                    "rejectedCount": _mem_result["rejectedCount"],
                    "confirmedCount": _mem_result["confirmedCount"],
                    "latencyMs": _mem_result["latencyMs"],
                })

        if not degraded:
            yield sse_event("done", {
                "runId": run_id,
                "sessionId": sid,
                "status": run_status,
                "runKind": info.get("runKind", "live"),
                "replayId": info.get("replayId", ""),
            })

    return StreamingResponse(agent_stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


async def _legacy_analyze_stream(
    body: RoutedStreamRequest,
    authoritative_event: Optional[Dict[str, Any]] = None,
    runtime_metadata: Optional[Dict[str, Any]] = None,
):
    """旧协同分析实现。"""
    import asyncio as _asyncio
    async def agent_stream():
        async for ev in _legacy_analyze_stream_inner(
            body,
            body.sessionId,
            authoritative_event,
            runtime_metadata=runtime_metadata,
        ):
            yield ev
    return StreamingResponse(agent_stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


async def _legacy_analyze_stream_inner(
    body: RoutedStreamRequest,
    sid: str,
    authoritative_event: Optional[Dict[str, Any]] = None,
    runtime_metadata: Optional[Dict[str, Any]] = None,
):
    import asyncio as _asyncio
    if not sid:
        sess = create_session(_new_collaboration_session_id(), "collaboration")
        sid = sess["sessionId"]
        yield sse_event("session_created", {"sessionId": sid})
    user_content = f"协同分析: {body.eventType} {body.roadName} {body.direction or ''}"
    title = f"{body.roadName}协同研判" if body.roadName != "未知路段" else "协同分析"
    update_session_title(sid, title[:20])
    um_id = f"um_{int(datetime.now().timestamp() * 1000)}"
    add_message(um_id, sid, "user", user_content, "collaboration")

    info_source = compact_event_context(authoritative_event) if authoritative_event else body.model_dump()
    info = _get_event_info(info_source)
    owned_metadata = dict(runtime_metadata or {})
    run_kind = "replay" if owned_metadata.get("runKind") == "replay" else "live"
    info["runKind"] = run_kind
    info["actionExecutionAllowed"] = run_kind != "replay"
    info.pop("replayId", None)
    info.pop("replayOfEventId", None)
    if run_kind == "replay":
        info["replayId"] = str(owned_metadata.get("replayId") or "")
        info["replayOfEventId"] = str(
            owned_metadata.get("replayOfEventId") or extract_event_id(info)
        )
        yield sse_event("replay_started", {
            "replayId": info["replayId"],
            "eventId": info["replayOfEventId"],
            "actionExecutionAllowed": False,
        })
    routing = route_agents(info)
    agents_order = [a for a in routing["selectedAgents"] if a != "ReportAgent"]

    yield sse_event("event_parse_start", {"text": "正在解析事件信息..."})
    await _asyncio.sleep(0.3)
    yield sse_event("event_parse_done", {"eventType": info.get("eventType"), "roadName": info.get("roadName")})
    yield sse_event("agent_route_done", {"selectedAgents": routing["selectedAgents"], "routingReasons": routing["routingReasons"]})

    from backend.agent.multi_agent import CongestionAgent, SignalAgent, DispatchAgent
    agent_map = {"CongestionAgent": CongestionAgent, "SignalAgent": SignalAgent, "DispatchAgent": DispatchAgent}
    all_results = []
    for name in agents_order:
        yield sse_event("agent_start", {"agentName": name, "text": f"{name} 正在分析..."})
        await _asyncio.sleep(0.3)
        cls = agent_map.get(name)
        if cls:
            result = cls().analyze(info)
            all_results.append(result)
            yield sse_event("agent_result", result)

    yield sse_event("conflict_check_start", {"text": "正在检测 Agent 建议冲突..."})
    conflicts = detect_conflicts(all_results)
    yield sse_event("conflict_check_done", {"conflicts": conflicts, "conflictCount": len(conflicts)})

    yield sse_event("fusion_start", {"text": "正在融合各 Agent 结论..."})
    fusion = f"综合 {len(all_results)} 个 Agent 的分析，核心风险为{'高' if routing['riskTriggers'] else '待评估'}。紧急度评估为{'高' if len(routing['riskTriggers']) >= 2 else '中'}。"
    if conflicts: fusion += f"检测到 {len(conflicts)} 个建议冲突，已按安全优先和急救通道优先原则融合处理。"
    for r in all_results:
        if r.get("suggestion"): fusion += f"[{r['agentName']}] {r['suggestion']}。"
    for chunk in _text_chunks(fusion):
        yield sse_event("fusion_delta", {"text": chunk})
        await _asyncio.sleep(0.03)
    yield sse_event("fusion_done", {"fusionSummary": fusion})

    am_id = f"am_{int(datetime.now().timestamp() * 1000)}"
    add_message(am_id, sid, "assistant", fusion, "collaboration",
                {"selectedAgents": routing["selectedAgents"], "conflictCount": len(conflicts)})
    if authoritative_event and run_kind != "replay":
        from backend.tools.db_tools import advance_event_status
        advance_event_status(
            extract_event_id(authoritative_event), "待派单", allowed_from=["待研判"]
        )
    yield sse_event("done", {"sessionId": sid, "assistantMessageId": am_id, "title": title[:20], "agentResults": all_results, "fusionSummary": fusion})


def _text_chunks(text: str, size: int = 6) -> list:
    if not text: return [""]
    chunks = []; start = 0
    for i, ch in enumerate(text):
        if ch in "。\n？！，；" and i - start >= size:
            chunks.append(text[start:i + 1]); start = i + 1
    if start < len(text): chunks.append(text[start:])
    return chunks if chunks else [text[i:i + size] for i in range(0, len(text), size)]


# -------------------- 健康检查 --------------------

@app.get("/health", summary="健康检查")
async def health():
    from backend.config import LLM_ENABLED, DEEPSEEK_MODEL
    from backend.rag.vector_store import _CHROMA_AVAILABLE
    from backend.rag.vector_store import get_collection_stats as rag_stats
    return {
        "status": "ok",
        "service": "TrafficMind Agent",
        "llmAvailable": LLM_ENABLED,
        "llmProvider": "DeepSeek" if LLM_ENABLED else None,
        "llmModel": DEEPSEEK_MODEL if LLM_ENABLED else None,
        "llmMode": "llm" if LLM_ENABLED else "template_fallback",
        "chromaAvailable": _CHROMA_AVAILABLE,
        "collaborationOrchestratorEnabled": COLLABORATION_ORCHESTRATOR_ENABLED,
        "collaborationProtocolVersion": "1.0",
        "collaborationRepositoryType": "sqlite",
        "collaborationFallbackEnabled": True,
        "memoryV2Enabled": True,
        "memoryV2RepositoryType": "sqlite",
        "ragV2Enabled": bool(globals().get("_RAG_V2_AVAILABLE", False)),
        "ragV2EmbeddingModel": get_embedding_provider().get_model_name() if globals().get("_RAG_V2_AVAILABLE", False) else None,
        "ragV2RerankerModel": get_reranker_provider().get_model_name() if globals().get("_RAG_V2_AVAILABLE", False) else None,
    }


# -------------------- 仪表盘统计 --------------------

@app.get("/stats", summary="仪表盘统计数据")
async def stats():
    """
    返回仪表盘所需的聚合统计数据：
      - 总事件数 / 高风险事件数 / 平均风险分 / 待派单数
      - 风险等级分布 / 事件类型分布 / 状态分布
      - 近 7 天每日事件趋势
    """
    return get_stats()


# -------------------- Phase 9.3: 协作审计 API --------------------

@app.get("/collaboration/runs", summary="按真实事件查询协作运行列表")
async def list_event_collaboration_runs(
    event_id: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """Read-only event-bound collaboration discovery. Uses exact persisted eventId only."""
    from backend.agent.collaboration.db_repository import SQLiteCollaborationRepository
    canonical_event_id = str(event_id or "").strip()
    if not canonical_event_id:
        raise HTTPException(status_code=400, detail="event_id 不能为空")
    repo = SQLiteCollaborationRepository()
    total = repo.count_runs_by_event_id(canonical_event_id)
    rows = repo.list_runs_by_event_id(canonical_event_id, limit=limit, offset=offset)
    return {
        "eventId": canonical_event_id,
        "total": total,
        "limit": limit,
        "offset": offset,
        "runs": [_safe_parse_json_fields(dict(r)) for r in rows],
    }


@app.get("/collaboration/runs/{run_id}", summary="查询协作运行详情")
async def get_collaboration_run(run_id: str):
    """返回某次协作运行的完整审计记录。"""
    from backend.agent.collaboration.db_repository import SQLiteCollaborationRepository
    repo = SQLiteCollaborationRepository()
    run = repo.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} 不存在")
    return {
        "run": _safe_parse_json_fields(run),
        "tasks": [_safe_parse_json_fields(t) for t in _list_collab_tasks(run_id)],
        "messages": repo.list_messages(run_id),
        "conflicts": repo.list_conflicts(run_id),
        "events": repo.list_events(run_id),
    }


def _safe_parse_json_fields(d: dict) -> dict:
    """将 SQLite 中的 JSON 字符串字段解析为对象。"""
    json_fields = ["selected_agents", "skipped_agents", "failed_agents", "normalized_event",
                   "budget_usage", "final_decision", "grounding_context", "depends_on", "input_snapshot",
                   "output_snapshot", "payload", "proposals"]
    for key in json_fields:
        if key in d and isinstance(d[key], str):
            try: d[key] = json.loads(d[key])
            except: pass
    return d


def _list_collab_tasks(run_id: str):
    from backend.agent.collaboration.db_repository import SQLiteCollaborationRepository
    repo = SQLiteCollaborationRepository()
    init_collab = __import__('backend.agent.collaboration.db_repository', fromlist=['init_collaboration_tables']).init_collaboration_tables
    init_collab()
    import sqlite3
    from backend.config import DB_PATH
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM collaboration_tasks WHERE run_id=?", (run_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/collaboration/sessions/{session_id}/runs", summary="查询会话的协作运行列表")
async def get_session_collaboration_runs(session_id: str):
    from backend.agent.collaboration.db_repository import SQLiteCollaborationRepository
    repo = SQLiteCollaborationRepository()
    init_collab = __import__('backend.agent.collaboration.db_repository', fromlist=['init_collaboration_tables']).init_collaboration_tables
    init_collab()
    import sqlite3
    from backend.config import DB_PATH
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM collaboration_runs WHERE session_id=? ORDER BY started_at ASC, run_id ASC", (session_id,)).fetchall()
    conn.close()
    return {"runs": [dict(r) for r in rows]}


# ==================== Phase 10 Memory V2 API ====================

@app.get("/memory/sessions/{session_id}", summary="查询会话结构化记忆")
async def get_session_memory(session_id: str):
    """返回 Session 的结构化 Memory 视图。零 SQLite 直连。"""
    from backend.memory.factory import create_memory_repository
    from backend.memory.store import init_memory_tables
    init_memory_tables()
    repo = create_memory_repository()

    from backend.chat.chat_db import get_session
    sess = get_session(session_id)
    if not sess:
        raise HTTPException(status_code=404, detail=f"会话 {session_id} 不存在")

    return repo.get_session_memory_view(session_id)


@app.get("/memory/runs/{run_id}/trace", summary="查询 Run 的 Memory Trace")
async def get_memory_trace(run_id: str):
    """返回 Run 的 Memory Trace 完整记录。零 SQLite 直连。"""
    from backend.memory.factory import create_memory_repository
    from backend.memory.store import init_memory_tables
    init_memory_tables()
    repo = create_memory_repository()

    trace = repo.get_trace_by_run(run_id)
    if not trace:
        from backend.agent.collaboration.db_repository import SQLiteCollaborationRepository
        collab = SQLiteCollaborationRepository()
        run = collab.get_run(run_id)
        if run:
            return {
                "runId": run_id,
                "hasTrace": False,
                "message": "该运行创建于 Memory V2 之前，无记忆追踪数据。",
            }
        raise HTTPException(status_code=404, detail=f"Run {run_id} 不存在")

    return {
        "runId": run_id,
        "hasTrace": True,
        "traceId": trace.trace_id,
        "sessionId": trace.session_id,
        "eventThreadId": getattr(trace, "event_thread_id", ""),
        "recallIntent": trace.recall_intent,
        "recallDecision": _safe_json(trace.recall_decision_json),
        "recallPlan": _safe_json(trace.recall_plan_json),
        "candidates": _safe_json(trace.candidates_json),
        "selected": _safe_json(trace.selected_json),
        "rejected": _safe_json(trace.rejected_json),
        "injectionMap": _safe_json(trace.injection_map_json),
        "writeCandidates": _safe_json(trace.write_candidates_json),
        "writeResults": _safe_json(trace.write_results_json),
        "tokenEstimate": trace.token_estimate,
        "recallLatencyMs": trace.recall_latency_ms,
        "writeLatencyMs": trace.write_latency_ms,
        "createdAt": trace.created_at,
        "updatedAt": trace.updated_at,
    }


@app.get("/memory/items/{memory_id}", summary="查询单条 Memory")
async def get_memory_item(memory_id: str):
    """返回单条 Memory 及来源链。零 SQLite 直连。"""
    from backend.memory.factory import create_memory_repository
    from backend.memory.store import init_memory_tables
    init_memory_tables()
    repo = create_memory_repository()

    item = repo.get_item(memory_id)
    if not item:
        raise HTTPException(status_code=404, detail=f"Memory {memory_id} 不存在")

    supersedes = None
    if item.supersedes_id:
        old = repo.get_item(item.supersedes_id)
        if old:
            supersedes = old.to_dict()
    superseded_by = repo.get_item_superseded_by(memory_id)

    thread = None
    if item.event_thread_id:
        t = repo.get_event_thread(item.event_thread_id)
        thread = t.to_dict() if t else None

    return {
        "item": item.to_dict(),
        "supersedes": supersedes,
        "supersededBy": superseded_by.to_dict() if superseded_by else None,
        "sourceRunId": item.source_run_id,
        "sourceMessageId": item.source_message_id,
        "eventThread": thread,
        "provenance": {
            "sourceType": item.source_type,
            "sourceRunId": item.source_run_id,
            "authorityLevel": item.authority_level,
            "confidence": item.confidence,
        },
    }


@app.get("/memory/sessions/{session_id}/threads", summary="查询 Event Thread 列表")
async def list_event_threads(session_id: str):
    """返回 Session 的 Event Thread 列表。零 SQLite 直连。"""
    from backend.memory.factory import create_memory_repository
    from backend.memory.store import init_memory_tables
    init_memory_tables()
    repo = create_memory_repository()

    raw_threads = repo.list_event_threads(session_id)
    threads = []
    for t in raw_threads:
        td = t.to_dict()
        td["itemCount"] = repo.count_event_thread_items(session_id, t.id)
        td["runCount"] = repo.count_event_thread_runs(session_id, t.id)
        threads.append(td)

    return {"sessionId": session_id, "threads": threads}


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 12: Workflow V1 Router
# ═══════════════════════════════════════════════════════════════════════════════

from backend.workflow.api import router as workflow_router
from backend.knowledge.api import router as knowledge_router
app.include_router(workflow_router)
app.include_router(knowledge_router)

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 17: Adaptive Planning V1 Router
# ═══════════════════════════════════════════════════════════════════════════════

from backend.planning.api import router as planning_router
app.include_router(planning_router)

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 13: Traffic Map & Simulation V1 Router
# ═══════════════════════════════════════════════════════════════════════════════

from backend.simulation.api import router as simulation_router
app.include_router(simulation_router)

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 14: Workflow Observability V1 Router
# ═══════════════════════════════════════════════════════════════════════════════

from backend.observability.api import router as observability_router
app.include_router(observability_router)

# Phase 14 Round 3: Evaluation Dashboard
from backend.evaluation.eval_api import router as eval_router
app.include_router(eval_router)

# Phase 21: Pilot Region Grounding Layer
from backend.regional.api import router as regional_router
app.include_router(regional_router)

# Phase 21: Traffic Case Memory
from backend.case_memory.api import router as case_memory_router
app.include_router(case_memory_router)


def _safe_json(s: str):
    """安全解析 JSON 字符串。"""
    if not s:
        return {} if s in ("", "{}") else []
    try:
        import json as _j
        return _j.loads(s)
    except Exception:
        return s
