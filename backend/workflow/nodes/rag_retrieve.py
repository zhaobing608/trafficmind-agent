"""
rag_retrieve 节点 — RAG 知识检索。

调用 RAG V2（回退到 V1）检索相关知识：
  - 预案规则
  - 历史相似案例
  - 处置经验

检索结果存入 rag_context，不覆盖 current_event。
"""

from typing import Any, Dict

from backend.workflow.models import NodeConfig
from backend.workflow.state import TrafficWorkflowState


async def execute_rag_retrieve(
    state: TrafficWorkflowState, config: NodeConfig
) -> Dict[str, Any]:
    """执行 RAG 知识检索。

    Args:
        state: 工作流状态
        config: 节点配置（config.query_template 可用于自定义查询）

    Returns:
        检索结果（存入 rag_context）
    """
    event = state.current_event or {}
    event_type = event.get("eventTypeCn", event.get("eventType", ""))
    road_name = event.get("roadName", "")
    risk_level = state.risk_assessment.get("riskLevel", "")

    # 构建查询
    query_template = config.config.get("query_template", "{event_type} {road_name} 处置预案")
    query = query_template.format(
        event_type=event_type,
        road_name=road_name,
        risk_level=risk_level,
    )

    results = []
    trace_id = ""
    degraded = False

    # 尝试 RAG V2
    try:
        from backend.rag.v2.pipeline import get_pipeline
        pipeline = get_pipeline()
        rag_result = pipeline.search(
            query=query,
            top_k=config.config.get("top_k", 5),
            filters={
                "event_type": event.get("eventType"),
            } if event.get("eventType") else None,
        )
        results = rag_result.get("results", rag_result.get("candidates", []))
        trace_value = rag_result.get("trace")
        trace_payload = trace_value if isinstance(trace_value, dict) else {}
        trace_id = str(
            rag_result.get("traceId")
            or rag_result.get("trace_id")
            or trace_payload.get("traceId")
            or trace_payload.get("trace_id")
            or ""
        )
    except Exception as e:
        degraded = True
        # 回退到 RAG V1
        try:
            from backend.rag.semantic_retriever import semantic_search
            v1_result = semantic_search(
                query=query,
                limit=config.config.get("top_k", 5),
                event_type=event.get("eventType"),
            )
            results = v1_result.get("results", [])
        except Exception:
            results = []

    # 存入 rag_context（不覆盖 current_event）
    rag_ctx = {
        "query": query,
        "results": results,
        "resultCount": len(results),
        "traceId": trace_id,
        "degraded": degraded,
    }
    state.set_rag_context(rag_ctx)

    if trace_id:
        state.rag_trace_ids.append(trace_id)

    state.add_audit_event("rag_retrieved", config.node_id, {
        "query": query,
        "resultCount": len(results),
        "traceId": trace_id or None,
        "degraded": degraded,
    })

    return {"rag_context": rag_ctx}
