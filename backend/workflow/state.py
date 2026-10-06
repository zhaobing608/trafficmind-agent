"""
TrafficWorkflowState — Phase 12

Workflow 执行过程中的完整状态容器。

设计原则：
  - current_event 不可被 Memory/RAG 覆盖
  - dynamic_observations 不得写成 stable_facts
  - RAG Context 与 Memory Context 分离
  - Agent 之间只传 summary 和 Evidence Ref
  - 不保存或暴露隐藏思维过程
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from backend.workflow.models import WorkflowRunStatus


# ═══════════════════════════════════════════════════════════════════════════════
# 合法状态转换表（同 CollaborationRunState 模式）
# ═══════════════════════════════════════════════════════════════════════════════

VALID_TRANSITIONS: Dict[WorkflowRunStatus, set] = {
    WorkflowRunStatus.PENDING: {WorkflowRunStatus.RUNNING, WorkflowRunStatus.FAILED, WorkflowRunStatus.CANCELLED},
    WorkflowRunStatus.RUNNING: {
        WorkflowRunStatus.PAUSED,
        WorkflowRunStatus.AWAITING_APPROVAL,
        WorkflowRunStatus.COMPLETED,
        WorkflowRunStatus.FAILED,
        WorkflowRunStatus.CANCELLED,
    },
    WorkflowRunStatus.PAUSED: {
        WorkflowRunStatus.RUNNING,
        WorkflowRunStatus.CANCELLED,
        WorkflowRunStatus.FAILED,
    },
    WorkflowRunStatus.AWAITING_APPROVAL: {
        WorkflowRunStatus.PENDING,
        WorkflowRunStatus.RUNNING,
        WorkflowRunStatus.COMPLETED,
        WorkflowRunStatus.REJECTED,
        WorkflowRunStatus.CANCELLED,
    },
    WorkflowRunStatus.COMPLETED: set(),   # 终止状态，不可转换
    # FAILED is terminal for automatic execution, but an explicit validated
    # retry may create a new durable attempt from the failed cursor.
    WorkflowRunStatus.FAILED: {WorkflowRunStatus.PENDING},
    WorkflowRunStatus.CANCELLED: set(),    # 终止状态，不可转换
    WorkflowRunStatus.REJECTED: set(),     # 终止状态，不可转换
}


# ═══════════════════════════════════════════════════════════════════════════════
# TrafficWorkflowState
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class TrafficWorkflowState:
    """Workflow 执行完整状态。

    字段分组：
      - 标识：workflow_run_id, workflow_definition_id, workflow_version,
              session_id, event_thread_id
      - 输入：current_event（只读）、original_input
      - 事实：stable_facts（持久化）、dynamic_observations（不持久化）
      - 检索上下文：rag_context（RAG 检索结果）、memory_context（Memory 召回结果）— 分离存储
      - Agent 输出：agent_outputs（summary + evidence refs only）
      - 风险评估：risk_assessment
      - 动作：proposed_actions → approved_actions → action_results
      - 控制：current_node, status, attempt_counts, pending_approval
      - 追踪：errors, audit_events
    """

    # ── 标识 ──────────────────────────────────────────────────────────────
    workflow_run_id: str = ""
    workflow_definition_id: str = ""
    workflow_version: int = 1
    session_id: str = ""
    event_thread_id: str = ""

    # ── 输入（current_event 只读，不可被 Memory/RAG 覆盖）─────────────────
    current_event: Dict[str, Any] = field(default_factory=dict)
    original_input: Dict[str, Any] = field(default_factory=dict)

    # ── 事实 ──────────────────────────────────────────────────────────────
    stable_facts: Dict[str, Any] = field(default_factory=dict)
    dynamic_observations: Dict[str, Any] = field(default_factory=dict)

    # ── 检索上下文（RAG 与 Memory 分离存储，不可互相覆盖）──────────────────
    rag_context: Dict[str, Any] = field(default_factory=dict)
    memory_context: Dict[str, Any] = field(default_factory=dict)

    # ── 证据引用 ──────────────────────────────────────────────────────────
    evidence_refs: List[Dict[str, Any]] = field(default_factory=list)

    # ── Agent 输出（只传 summary 和 evidence refs，不传完整内部状态）────────
    agent_outputs: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # ── 风险评估 ──────────────────────────────────────────────────────────
    risk_assessment: Dict[str, Any] = field(default_factory=dict)

    # ── 动作管理 ──────────────────────────────────────────────────────────
    proposed_actions: List[Dict[str, Any]] = field(default_factory=list)
    approved_actions: List[Dict[str, Any]] = field(default_factory=list)
    action_results: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # ── 节点输出（持久化，供条件 DSL 和恢复场景使用）───────────────────
    node_outputs: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # ── 控制 ──────────────────────────────────────────────────────────────
    current_node: str = ""
    status: WorkflowRunStatus = WorkflowRunStatus.PENDING
    attempt_counts: Dict[str, int] = field(default_factory=dict)
    completed_steps: List[str] = field(default_factory=list)
    retry_count: int = 0
    pending_approval: Optional[Dict[str, Any]] = None
    cancel_reason: str = ""
    cancelled_at: str = ""

    # ── 追踪 ──────────────────────────────────────────────────────────────
    errors: List[Dict[str, Any]] = field(default_factory=list)
    audit_events: List[Dict[str, Any]] = field(default_factory=list)

    # ── Phase 13: Simulation Refs（独立于 current_event）──────────────
    simulation_refs: Dict[str, Any] = field(default_factory=dict)
    # keys: simulationRunId, trafficEventId, decisionSnapshotId,
    #       latestSnapshotId, workflowRunId, spatialContextRef
    # spatialContextRef = {simulationRunId, trafficEventId, snapshotId}
    # 恢复时通过 build_spatial_context() 根据 refs 重建

    # ── 关联追踪 ID ──────────────────────────────────────────────────────
    rag_trace_ids: List[str] = field(default_factory=list)
    agent_run_ids: List[str] = field(default_factory=list)
    approval_ids: List[str] = field(default_factory=list)
    action_record_ids: List[str] = field(default_factory=list)

    # ── 时间戳 ────────────────────────────────────────────────────────────
    started_at: str = ""
    updated_at: str = ""
    finished_at: str = ""

    def __post_init__(self):
        if not self.updated_at:
            self.updated_at = _utc_now_iso()

    # ── 状态转换 ──────────────────────────────────────────────────────

    def transition(self, new_status: WorkflowRunStatus) -> None:
        """安全状态转换。"""
        allowed = VALID_TRANSITIONS.get(self.status, set())
        if new_status not in allowed:
            raise ValueError(
                f"非法状态转换: '{self.status.value}' → '{new_status.value}'。"
                f"允许: {[s.value for s in allowed]}"
            )
        self.status = new_status
        self.updated_at = _utc_now_iso()
        if new_status == WorkflowRunStatus.RUNNING and not self.started_at:
            self.started_at = self.updated_at
        if new_status in {
            WorkflowRunStatus.COMPLETED,
            WorkflowRunStatus.FAILED,
            WorkflowRunStatus.CANCELLED,
            WorkflowRunStatus.REJECTED,
        }:
            self.finished_at = self.updated_at
        elif new_status == WorkflowRunStatus.PENDING:
            self.finished_at = ""

    def is_terminal(self) -> bool:
        """是否为终止状态。"""
        return self.status in {
            WorkflowRunStatus.COMPLETED,
            WorkflowRunStatus.FAILED,
            WorkflowRunStatus.CANCELLED,
            WorkflowRunStatus.REJECTED,
        }

    # ── 事件只读保护 ──────────────────────────────────────────────────

    def get_current_event_snapshot(self) -> Dict[str, Any]:
        """获取 current_event 的只读深拷贝。"""
        return deepcopy(self.current_event)

    def assert_current_event_unchanged(self, before: Dict[str, Any]) -> None:
        """断言 current_event 未被覆盖。"""
        if self.current_event != before:
            raise RuntimeError(
                "current_event 被意外修改！Memory/RAG 不得覆盖 current_event。"
            )

    # ── 上下文注入（安全方式）─────────────────────────────────────────

    def set_rag_context(self, ctx: Dict[str, Any]) -> None:
        """设置 RAG 检索上下文（不修改 current_event）。"""
        self.rag_context = deepcopy(ctx)

    def set_memory_context(self, ctx: Dict[str, Any]) -> None:
        """设置 Memory 召回上下文（不修改 current_event）。"""
        self.memory_context = deepcopy(ctx)

    def add_stable_fact(self, key: str, value: Any) -> None:
        """添加稳定事实（不做覆盖，需显式确认）。"""
        self.stable_facts[key] = value

    def add_dynamic_observation(self, key: str, value: Any) -> None:
        """添加动态观察（不得写成稳定事实）。"""
        self.dynamic_observations[key] = value

    # ── Agent 输出管理 ───────────────────────────────────────────────

    def record_agent_output(self, agent_name: str, summary: str,
                            evidence_refs: List[str]) -> None:
        """记录 Agent 输出（只存 summary 和 evidence refs）。"""
        self.agent_outputs[agent_name] = {
            "summary": summary,
            "evidenceRefs": evidence_refs,
            "recordedAt": _utc_now_iso(),
        }

    # ── 错误追踪 ──────────────────────────────────────────────────────

    def record_error(self, node_id: str, error: str,
                     attempt: int = 1) -> None:
        """记录节点执行错误。"""
        self.errors.append({
            "nodeId": node_id,
            "error": error,
            "attempt": attempt,
            "timestamp": _utc_now_iso(),
        })

    # ── 审计事件 ──────────────────────────────────────────────────────

    def add_audit_event(self, event_type: str, node_id: str,
                        payload: Dict[str, Any] = None) -> None:
        """添加审计事件。"""
        self.audit_events.append({
            "eventType": event_type,
            "nodeId": node_id,
            "payload": payload or {},
            "timestamp": _utc_now_iso(),
        })

    # ── 序列化 ────────────────────────────────────────────────────────

    def to_dict(self) -> Dict[str, Any]:
        """序列化为可持久化的 dict。"""
        return {
            "workflowRunId": self.workflow_run_id,
            "workflowDefinitionId": self.workflow_definition_id,
            "workflowVersion": self.workflow_version,
            "sessionId": self.session_id,
            "eventThreadId": self.event_thread_id,
            "currentEvent": self.current_event,
            "originalInput": self.original_input,
            "stableFacts": self.stable_facts,
            "dynamicObservations": self.dynamic_observations,
            "ragContext": self.rag_context,
            "memoryContext": self.memory_context,
            "evidenceRefs": self.evidence_refs,
            "agentOutputs": self.agent_outputs,
            "riskAssessment": self.risk_assessment,
            "proposedActions": self.proposed_actions,
            "approvedActions": self.approved_actions,
            "actionResults": self.action_results,
            "nodeOutputs": self.node_outputs,
            "currentNode": self.current_node,
            "status": self.status.value,
            "attemptCounts": self.attempt_counts,
            "completedSteps": self.completed_steps,
            "retryCount": self.retry_count,
            "pendingApproval": self.pending_approval,
            "cancelReason": self.cancel_reason,
            "cancelledAt": self.cancelled_at,
            "errors": self.errors,
            "auditEvents": self.audit_events,
            "simulationRefs": self.simulation_refs,
            "ragTraceIds": self.rag_trace_ids,
            "agentRunIds": self.agent_run_ids,
            "approvalIds": self.approval_ids,
            "actionRecordIds": self.action_record_ids,
            "startedAt": self.started_at,
            "updatedAt": self.updated_at,
            "finishedAt": self.finished_at,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TrafficWorkflowState":
        """从持久化的 dict 反序列化。"""
        status_raw = d.get("status", "pending")
        if isinstance(status_raw, str):
            status = WorkflowRunStatus(status_raw)
        else:
            status = status_raw

        return cls(
            workflow_run_id=d.get("workflowRunId", ""),
            workflow_definition_id=d.get("workflowDefinitionId", ""),
            workflow_version=d.get("workflowVersion", 1),
            session_id=d.get("sessionId", ""),
            event_thread_id=d.get("eventThreadId", ""),
            current_event=d.get("currentEvent", {}),
            original_input=d.get("originalInput", {}),
            stable_facts=d.get("stableFacts", {}),
            dynamic_observations=d.get("dynamicObservations", {}),
            rag_context=d.get("ragContext", {}),
            memory_context=d.get("memoryContext", {}),
            evidence_refs=d.get("evidenceRefs", []),
            agent_outputs=d.get("agentOutputs", {}),
            risk_assessment=d.get("riskAssessment", {}),
            proposed_actions=d.get("proposedActions", []),
            approved_actions=d.get("approvedActions", []),
            action_results=d.get("actionResults", {}),
            node_outputs=d.get("nodeOutputs", {}),
            current_node=d.get("currentNode", ""),
            status=status,
            attempt_counts=d.get("attemptCounts", {}),
            completed_steps=d.get("completedSteps", []),
            retry_count=int(d.get("retryCount", 0) or 0),
            pending_approval=d.get("pendingApproval"),
            cancel_reason=d.get("cancelReason", ""),
            cancelled_at=d.get("cancelledAt", ""),
            errors=d.get("errors", []),
            audit_events=d.get("auditEvents", []),
            simulation_refs=d.get("simulationRefs", {}),
            rag_trace_ids=d.get("ragTraceIds", []),
            agent_run_ids=d.get("agentRunIds", []),
            approval_ids=d.get("approvalIds", []),
            action_record_ids=d.get("actionRecordIds", []),
            started_at=d.get("startedAt", ""),
            updated_at=d.get("updatedAt", ""),
            finished_at=d.get("finishedAt", ""),
        )


def _utc_now_iso() -> str:
    """返回 UTC 时间 ISO 格式字符串。"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
