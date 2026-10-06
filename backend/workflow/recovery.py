"""
Recovery Safety Classifier — Phase17 Round3

集中分类 stale node 的可重放性。禁止散落分类逻辑。

ACTION：读 ToolRegistry 元数据（riskLevel / sideEffect / idempotent）。
非 ACTION：显式 NodeType allowlist（代码证据证明无外部副作用才 READ_ONLY）。

UNKNOWN → fail-closed，不 auto replay。
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional


class RecoverySafetyClass(str, Enum):
    READ_ONLY = "read_only"
    WRITE_IDEMPOTENT = "write_idempotent"
    HIGH_RISK_NON_IDEMPOTENT = "high_risk_non_idempotent"
    UNKNOWN = "unknown"


# 非 ACTION 节点显式 allowlist：无外部副作用、可安全重执行
_READ_ONLY_NODE_TYPES = frozenset({
    "validate_event",
    "rule_router",
    "rag_retrieve",
    "memory_context",
    "agent_task",
    "evidence_evaluate",
    "risk_gate",
})

# 结构节点：不是 semantic step，无需 replay 分类（不进 recovery 重放）
_STRUCTURAL_NODE_TYPES = frozenset({
    "trigger", "close", "human_approval", "wait", "parallel", "join", "monitor",
})


class RecoverySafetyClassifier:
    """stale node 可重放性分类。"""

    def classify_node(self, node_type: str, action_type: Optional[str] = None) -> RecoverySafetyClass:
        """返回节点分类。ACTION 走 ToolRegistry；非 ACTION 走 allowlist。"""
        if node_type == "action":
            return self._classify_action(action_type or "")

        if node_type in _READ_ONLY_NODE_TYPES:
            return RecoverySafetyClass.READ_ONLY
        if node_type in _STRUCTURAL_NODE_TYPES:
            return RecoverySafetyClass.READ_ONLY  # 结构节点无副作用，重放无害
        return RecoverySafetyClass.UNKNOWN

    def _classify_action(self, action_type: str) -> RecoverySafetyClass:
        """按 ToolRegistry 元数据分类 ACTION。"""
        if not action_type:
            return RecoverySafetyClass.UNKNOWN
        try:
            from backend.agent.tool_registry import ToolRisk, get_tool_registry
            meta = get_tool_registry().get(action_type)
            if meta is None:
                return RecoverySafetyClass.UNKNOWN

            if meta.riskLevel == ToolRisk.READ_ONLY:
                return RecoverySafetyClass.READ_ONLY

            if meta.riskLevel == ToolRisk.HIGH_RISK and meta.sideEffect:
                # 高风险副作用：即使误标 idempotent 也默认 fail-safe
                return RecoverySafetyClass.HIGH_RISK_NON_IDEMPOTENT

            if meta.riskLevel == ToolRisk.WRITE and meta.idempotent:
                return RecoverySafetyClass.WRITE_IDEMPOTENT

            return RecoverySafetyClass.UNKNOWN
        except Exception:
            return RecoverySafetyClass.UNKNOWN


def detect_unknown_outcome(repo, run_id: str) -> List[Dict[str, Any]]:
    """检测 dispatch started 但无 durable terminal result 的 Action。

    返回 UNKNOWN_OUTCOME 候选列表（每个含 actionId/actionType/nodeId）。
    有 durable terminal result（SUCCEEDED/FAILED）→ 不返回（known outcome）。

    Phase 21.3 executors that advertise reconciliation are fenced as UNKNOWN
    regardless of risk class.  Even an idempotent internal write may have
    committed before the process died; replay must first establish durable
    truth instead of leaving its RUNNING marker stranded.
    """
    classifier = RecoverySafetyClassifier()
    unknowns: List[Dict[str, Any]] = []
    for record in repo.list_executing_action_records(run_id):
        cls = classifier.classify_node("action", record.action_type)
        if (
            record.reconciliation_supported
            or cls == RecoverySafetyClass.HIGH_RISK_NON_IDEMPOTENT
        ):
            unknowns.append({
                "actionId": record.action_id,
                "actionType": record.action_type,
                "nodeId": record.node_id,
                "idempotencyKey": record.idempotency_key,
            })
    return unknowns
