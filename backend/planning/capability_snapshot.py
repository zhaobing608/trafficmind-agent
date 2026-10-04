"""
PlannerCapabilitySnapshot — Phase 18 Round 1

能力快照：只暴露 REGISTERED ∧ IMPLEMENTED ∧ POLICY-KNOWN ∧ SCHEMA-DEFINED 的
planner-eligible capability。

关键约束：
  - 内部对象可含 executionAgentType / executionActionType（供 compiler 映射）
  - LLM prompt 绝不能看到这些 runtime identifier —— 必须经 to_prompt_dict() 投影
  - 禁止简单 snapshot.to_dict() 直接塞 prompt
  - deterministic ordering + stable serialization + snapshotHash
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from backend.agent.tool_registry import get_tool_registry
from backend.planning.models import EXECUTABLE_AGENT_TYPES
from backend.planning.param_schema import PLANNER_PARAM_SCHEMAS

# snapshot 版本号（schema 结构变更时递增）
SNAPSHOT_VERSION = 1

# 端到端真实有业务语义的 action（provider 真实产生 semantic effect，非 no-op）
#   - update_event_status → canonical Event 状态 CAS 更新
#   - create_dispatch_task → durable 内部处置任务
#   - send_notification → reliable NotificationProvider（local / webhook）
#   - notify_wechat / notify_dingtalk → send_wechat_work / send_dingtalk（真实推送）
#   - save_result → save_event_analysis（真实持久化）
#   - simulation_traffic_diversion / simulation_signal_adjustment → DemoSimulationProvider
#     TRAFFIC_DIVERSION / SIGNAL_ADJUSTMENT（真实分流/配时）
#   - simulation_monitor / simulation_close / simulation_lane_control /
#     simulation_dispatch_coordination → DemoSimulationProvider no-op → 不列入
END_TO_END_IMPLEMENTED_ACTIONS = frozenset({
    "update_event_status",
    "create_dispatch_task",
    "send_notification",
    "notify_wechat",
    "notify_dingtalk",
    "save_result",
    "simulation_traffic_diversion",
    "simulation_signal_adjustment",
})

# LLM prompt 公开能力仍保持 Phase 18 的小集合；Phase 21.3 reliable
# actions 先通过 persisted Agent structured recommendation → deterministic
# planner 接入，不把新的生产副作用能力无审计地扩大到 LLM prompt。
PROPOSABLE_ACTIONS = frozenset({
    "notify_wechat",
    "notify_dingtalk",
    "simulation_traffic_diversion",
    "simulation_signal_adjustment",
})

# capability ID（LLM 看到）→ execution agent type（内部）
AGENT_CAPABILITY_MAP: Dict[str, str] = {
    "congestion_analysis": "CongestionAgent",
    "accident_analysis": "AccidentAgent",
    "signal_analysis": "SignalAgent",
    "dispatch_analysis": "DispatchAgent",
}

# capability ID（LLM 看到）→ execution action type（内部）
ACTION_CAPABILITY_MAP: Dict[str, str] = {
    "notify_wechat": "notify_wechat",
    "notify_dingtalk": "notify_dingtalk",
    "simulate_traffic_diversion": "simulation_traffic_diversion",
    "simulate_signal_adjustment": "simulation_signal_adjustment",
}

# 反向：execution agent type → capability ID（compiler 校验用）
_EXECUTION_AGENT_TO_CAPABILITY = {v: k for k, v in AGENT_CAPABILITY_MAP.items()}

# evidence capability（LLM 声明的 evidenceNeeds → runtime 映射）
EVIDENCE_CAPABILITIES = ["historical_cases", "traffic_rules", "current_traffic_state", "simulation_context"]


def is_planner_executable_action(action_type: str) -> bool:
    """唯一 no-op capability 判定 helper（snapshot builder 与 compiler 共用）。

    判定 = end-to-end implemented ∧ ToolRegistry registered ∧ param schema defined。
    无真实端到端 semantic effect → plannerEligible=false。
    """
    return (
        action_type in END_TO_END_IMPLEMENTED_ACTIONS
        and get_tool_registry().is_registered(action_type)
        and action_type in PLANNER_PARAM_SCHEMAS
    )


def is_planner_agent(agent_type: str) -> bool:
    """agent 是否可执行（EXECUTABLE_AGENT_TYPES）。"""
    return agent_type in EXECUTABLE_AGENT_TYPES


@dataclass
class AgentCapability:
    """LLM-facing agent capability。execution_agent_type 仅供 compiler 内部。"""
    agentCapabilityId: str
    description: str
    supportedIntents: List[str]
    executionAgentType: str = ""      # 内部，不进 prompt
    plannerEligible: bool = True

    def to_prompt_dict(self) -> Dict[str, Any]:
        return {
            "agentCapabilityId": self.agentCapabilityId,
            "description": self.description,
            "supportedIntents": list(self.supportedIntents),
        }


@dataclass
class ActionCapability:
    """LLM-facing action capability。execution_action_type 仅供 compiler 内部。"""
    actionCapabilityId: str
    description: str
    intentFamily: str
    sideEffect: bool
    riskLevel: str
    approvalRequired: bool
    idempotent: bool
    businessParamSchema: Dict[str, Any] = field(default_factory=dict)
    executionActionType: str = ""     # 内部，不进 prompt
    plannerEligible: bool = True

    def to_prompt_dict(self) -> Dict[str, Any]:
        return {
            "actionCapabilityId": self.actionCapabilityId,
            "description": self.description,
            "intentFamily": self.intentFamily,
            "sideEffect": self.sideEffect,
            "riskLevel": self.riskLevel,
            "approvalRequired": self.approvalRequired,
            "idempotent": self.idempotent,
            "businessParamSchema": self.businessParamSchema,
        }


@dataclass
class PlannerCapabilitySnapshot:
    """可哈希、确定性序列化的能力快照。"""
    snapshotVersion: int
    snapshotHash: str
    agents: List[AgentCapability] = field(default_factory=list)
    actions: List[ActionCapability] = field(default_factory=list)
    evidenceCapabilities: List[str] = field(default_factory=list)
    constraints: List[Dict[str, Any]] = field(default_factory=list)

    def get_agent_capability(self, capability_id: str) -> Optional[AgentCapability]:
        for a in self.agents:
            if a.agentCapabilityId == capability_id:
                return a
        return None

    def get_action_capability(self, capability_id: str) -> Optional[ActionCapability]:
        for a in self.actions:
            if a.actionCapabilityId == capability_id:
                return a
        return None

    def to_prompt_dict(self) -> Dict[str, Any]:
        """LLM prompt 投影：不含 executionAgentType / executionActionType / raw 标识。"""
        return {
            "snapshotVersion": self.snapshotVersion,
            "agents": [a.to_prompt_dict() for a in self.agents],
            "actions": [a.to_prompt_dict() for a in self.actions],
            "evidenceCapabilities": list(self.evidenceCapabilities),
            "constraints": [dict(c) for c in self.constraints],
        }


# ── agent capability 描述（单一真相，按 EXECUTABLE_AGENT_TYPES 顺序）────────────────

_AGENT_DEFS = [
    ("congestion_analysis", "拥堵研判：分析拥堵程度、蔓延趋势与分流需求",
     ["analyze_congestion", "congestion_impact", "diversion_need"]),
    ("accident_analysis", "事故研判：分析事故影响、清障优先级与联动需求",
     ["analyze_accident", "accident_impact", "clearance_priority"]),
    ("signal_analysis", "信号研判：分析信号异常与配时优化需求",
     ["analyze_signal", "signal_optimization"]),
    ("dispatch_analysis", "调度研判：确定联动部门与派单优先级",
     ["dispatch_coordination", "dispatch_priority"]),
]

# ── action capability 描述（intent family）───────────────────────────────────────

_ACTION_DEFS = [
    ("notify_wechat", "企业微信通知（高风险事件）", "notification"),
    ("notify_dingtalk", "钉钉通知（高风险事件）", "notification"),
    ("simulate_traffic_diversion", "仿真交通分流动作", "traffic_diversion"),
    ("simulate_signal_adjustment", "仿真信号配时调整动作", "signal_control"),
]


def build_planner_capability_snapshot() -> PlannerCapabilitySnapshot:
    """构建确定性能力快照。stable ordering + snapshotHash。"""
    registry = get_tool_registry()

    agents: List[AgentCapability] = []
    for cap_id, desc, intents in _AGENT_DEFS:
        exec_type = AGENT_CAPABILITY_MAP[cap_id]
        eligible = is_planner_agent(exec_type)
        agents.append(AgentCapability(
            agentCapabilityId=cap_id,
            description=desc,
            supportedIntents=intents,
            executionAgentType=exec_type,
            plannerEligible=eligible,
        ))

    actions: List[ActionCapability] = []
    for cap_id, desc, family in _ACTION_DEFS:
        exec_type = ACTION_CAPABILITY_MAP[cap_id]
        meta = registry.get(exec_type)
        eligible = is_planner_executable_action(exec_type)
        if meta is None:
            # 未注册 → 不可能 eligible；保留为 ineligible 供观测
            actions.append(ActionCapability(
                actionCapabilityId=cap_id, description=desc, intentFamily=family,
                sideEffect=True, riskLevel="unknown", approvalRequired=False,
                idempotent=False, businessParamSchema=PLANNER_PARAM_SCHEMAS.get(exec_type, {}),
                executionActionType=exec_type, plannerEligible=False,
            ))
            continue
        actions.append(ActionCapability(
            actionCapabilityId=cap_id, description=desc, intentFamily=family,
            sideEffect=meta.sideEffect, riskLevel=meta.riskLevel.value,
            approvalRequired=meta.approvalRequired, idempotent=meta.idempotent,
            businessParamSchema=PLANNER_PARAM_SCHEMAS.get(exec_type, {}),
            executionActionType=exec_type, plannerEligible=eligible,
        ))

    snapshot = PlannerCapabilitySnapshot(
        snapshotVersion=SNAPSHOT_VERSION,
        snapshotHash="",
        agents=agents,
        actions=actions,
        evidenceCapabilities=EVIDENCE_CAPABILITIES,
        constraints=[],
    )
    snapshot.snapshotHash = compute_snapshot_hash(snapshot)
    return snapshot


def compute_snapshot_hash(snapshot: PlannerCapabilitySnapshot) -> str:
    """deterministic snapshotHash（stable ordering + sorted keys）。"""
    canon: Dict[str, Any] = {
        "snapshotVersion": snapshot.snapshotVersion,
        "agents": sorted(
            [
                {
                    "agentCapabilityId": a.agentCapabilityId,
                    "description": a.description,
                    "supportedIntents": sorted(a.supportedIntents),
                    "plannerEligible": a.plannerEligible,
                }
                for a in snapshot.agents
            ],
            key=lambda x: x["agentCapabilityId"],
        ),
        "actions": sorted(
            [
                {
                    "actionCapabilityId": a.actionCapabilityId,
                    "intentFamily": a.intentFamily,
                    "riskLevel": a.riskLevel,
                    "approvalRequired": a.approvalRequired,
                    "plannerEligible": a.plannerEligible,
                }
                for a in snapshot.actions
            ],
            key=lambda x: x["actionCapabilityId"],
        ),
        "evidenceCapabilities": sorted(snapshot.evidenceCapabilities),
        "constraints": sorted(
            [json.dumps(c, sort_keys=True, ensure_ascii=False) for c in snapshot.constraints]
        ),
    }
    payload = json.dumps(canon, sort_keys=True, ensure_ascii=False, default=str)
    return "snap_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
