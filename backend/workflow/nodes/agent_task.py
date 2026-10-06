"""
agent_task 节点 — Agent 执行。

调用现有 Multi-Agent 执行分析任务。

设计约束：
  - 只传 summary 和 evidence refs（不传完整内部状态）
  - Agent 不自行创建节点或循环
  - 错误时记录但不阻止后续节点
"""

from typing import Any, Dict

from backend.workflow.models import NodeConfig
from backend.workflow.state import TrafficWorkflowState
from backend.workflow.action_execution import contains_sensitive_key
from backend.tools.event_tools import safe_float


async def execute_agent_task(
    state: TrafficWorkflowState, config: NodeConfig
) -> Dict[str, Any]:
    """执行 Agent 分析任务。

    Args:
        state: 工作流状态
        config: 节点配置
          - config.agent_name: Agent 名称（如 "CongestionAgent"）
          - config.task_prompt: 自定义任务提示（可选）

    Returns:
        Agent 输出（summary + evidence refs only）
    """
    agent_name = config.config.get("agent_name", "")
    if not agent_name:
        return {"error": "agent_task 缺少 agent_name 配置"}

    event = state.current_event or {}
    rag_ctx = state.rag_context or {}
    mem_ctx = state.memory_context or {}

    findings = []
    suggestion = ""
    urgency = "low"
    confidence = 0.5
    evidence_refs: list = []
    result: Dict[str, Any] = {}     # 初始化在 try 之前避免 UnboundLocalError

    try:
        # 使用 multi_agent 模块的 Agent 类
        from backend.agent.multi_agent import (
            CongestionAgent, SignalAgent, AccidentAgent, DispatchAgent,
            ReportAgent,
            _get_event_info,
        )

        # 构建 Agent 输入
        info = _get_event_info(event)

        # Phase 13: 注入 simulation spatial context（独立于 current_event）
        sim_refs = state.simulation_refs or {}
        if sim_refs.get("simulationRunId") or sim_refs.get("simulation_run_id"):
            try:
                from backend.simulation.tools import _spatial_context_to_dict
                from backend.simulation.demo_provider import get_demo_provider
                sim_run_id = sim_refs.get("simulationRunId") or sim_refs.get("simulation_run_id")
                event_id = sim_refs.get("trafficEventId") or sim_refs.get("traffic_event_id")
                decision_snap_id = sim_refs.get("decisionSnapshotId") or sim_refs.get("decision_snapshot_id")
                sp_ref = sim_refs.get("spatialContextRef", {})

                if sim_run_id and (event_id or sp_ref.get("trafficEventId")):
                    evt_id = event_id or sp_ref.get("trafficEventId")
                    provider = get_demo_provider()
                    ctx = provider.build_spatial_context(sim_run_id, evt_id)
                    info["simulation_context"] = _spatial_context_to_dict(ctx)
                    info["simulation_refs"] = {
                        "simulationRunId": sim_run_id,
                        "trafficEventId": evt_id,
                        "decisionSnapshotId": decision_snap_id or sp_ref.get("snapshotId", ""),
                    }
                    state.add_audit_event("agent_task_spatial_context", config.node_id, {
                        "simulationRunId": sim_run_id,
                        "trafficEventId": evt_id,
                    })
            except Exception:
                pass  # simulation context 不可用时不影响 agent_task

        agent_map = {
            "CongestionAgent": CongestionAgent,
            "SignalAgent": SignalAgent,
            "AccidentAgent": AccidentAgent,
            "DispatchAgent": DispatchAgent,
            "ReportAgent": ReportAgent,
        }

        result: Dict[str, Any] = {}
        cls = agent_map.get(agent_name)
        if cls is not None:
            instance = cls()
            result = instance.analyze(info)
            findings = result.get("findings", [])
            suggestion = result.get("suggestion", "")
            urgency = result.get("urgency", "low")
            confidence = safe_float(result.get("confidence"), 0.5)
        else:
            findings = [f"[{agent_name}] 未找到对应 Agent 实现，使用默认分析"]
            suggestion = "按常规流程处置"
            urgency = "low"
            confidence = 0.3

    except Exception as e:
        import traceback
        traceback.print_exc()
        findings = [f"[{agent_name}] 执行异常: {str(e)[:200]}"]
        suggestion = "Agent 执行失败，建议人工介入"
        urgency = "high"
        confidence = 0.0
        state.record_error(config.node_id, str(e))

    # 提取 RAG evidence refs
    if isinstance(rag_ctx, dict):
        for r in rag_ctx.get("results", [])[:3]:
            rid = r.get("id", r.get("evidenceId", ""))
            if rid:
                evidence_refs.append(rid)

    # Phase 13: 提取 Agent proposed_actions → state.proposed_actions
    agent_proposed = result.get("proposed_actions", []) if result else []
    if agent_proposed:
        existing = list(state.proposed_actions or [])
        for pa in agent_proposed:
            if isinstance(pa, dict):
                if contains_sensitive_key(pa):
                    state.add_audit_event(
                        "action_proposal_blocked",
                        config.node_id,
                        {"agentName": agent_name, "reason": "credential-shaped field"},
                    )
                    continue
                safe_proposal = dict(pa)
                safe_proposal["source"] = agent_name
                existing.append(safe_proposal)
        state.proposed_actions = existing

    # 只在 state 中存 summary + evidence refs
    state.record_agent_output(agent_name, suggestion, evidence_refs)

    output = {
        "agent_name": agent_name,
        "findings": findings,
        "suggestion": suggestion,
        "urgency": urgency,
        "confidence": confidence,
        "evidence_refs": evidence_refs,
        "status": "succeeded" if confidence > 0 else "failed",
    }

    state.add_audit_event("agent_completed", config.node_id, {
        "agentName": agent_name,
        "confidence": confidence,
        "urgency": urgency,
        "findingCount": len(findings),
    })

    return output
