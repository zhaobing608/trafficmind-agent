"""
Workflow V1 数据模型 — Phase 12

定义所有核心数据结构：
  - WorkflowDefinition: 流程模板定义
  - WorkflowDefinitionVersion: 不可变版本快照
  - WorkflowRun: 一次流程执行实例
  - WorkflowNodeRun: 单节点执行记录
  - WorkflowEvent: 审计事件
  - WorkflowApproval: 人工审批记录
  - WorkflowActionRecord: 外部动作记录（含幂等键）
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


# ═══════════════════════════════════════════════════════════════════════════════
# 枚举定义
# ═══════════════════════════════════════════════════════════════════════════════


class WorkflowRunStatus(str, Enum):
    """Workflow 运行状态。"""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    AWAITING_APPROVAL = "awaiting_approval"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class NodeType(str, Enum):
    """Workflow 节点类型。"""
    TRIGGER = "trigger"
    VALIDATE_EVENT = "validate_event"
    RULE_ROUTER = "rule_router"
    RAG_RETRIEVE = "rag_retrieve"
    MEMORY_CONTEXT = "memory_context"
    AGENT_TASK = "agent_task"
    PARALLEL = "parallel"
    JOIN = "join"
    EVIDENCE_EVALUATE = "evidence_evaluate"
    RISK_GATE = "risk_gate"
    HUMAN_APPROVAL = "human_approval"
    ACTION = "action"
    WAIT = "wait"
    MONITOR = "monitor"
    CLOSE = "close"


class NodeStatus(str, Enum):
    """节点执行状态。"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRYING = "retrying"
    SKIPPED = "skipped"
    TIMED_OUT = "timed_out"
    AWAITING_APPROVAL = "awaiting_approval"
    PAUSED = "paused"


class ApprovalDecision(str, Enum):
    """审批决策。"""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EDITED = "edited"


class ActionStatus(str, Enum):
    """Durable action-execution status.

    ``EXECUTING`` is retained for rows written before Phase 21.3.  New
    executions use ``RUNNING`` and stale legacy EXECUTING rows are treated as
    an unknown outcome rather than being dispatched again.
    """
    PENDING = "pending"
    RUNNING = "running"
    EXECUTING = "executing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    UNKNOWN = "unknown"
    CANCELLED = "cancelled"
    BLOCKED = "blocked"


class DefinitionStatus(str, Enum):
    """Workflow 定义状态。"""
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"


class WaitConditionType(str, Enum):
    """等待条件类型。"""
    TIME_DELAY = "time_delay"        # 固定时长等待
    EXTERNAL_EVENT = "external_event"  # 外部事件触发


class MonitorConditionType(str, Enum):
    """监控条件类型。"""
    STATUS_CHANGE = "status_change"
    THRESHOLD_CROSS = "threshold_cross"
    TIME_WINDOW = "time_window"


# 终止状态集合
TERMINAL_STATUSES = {
    WorkflowRunStatus.COMPLETED,
    WorkflowRunStatus.FAILED,
    WorkflowRunStatus.CANCELLED,
    WorkflowRunStatus.REJECTED,
}

# 可中断状态集合
INTERRUPTIBLE_STATUSES = {
    WorkflowRunStatus.RUNNING,
    WorkflowRunStatus.PAUSED,
    WorkflowRunStatus.AWAITING_APPROVAL,
}


# ═══════════════════════════════════════════════════════════════════════════════
# 节点配置
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class NodeConfig:
    """单个节点的配置。

    Attributes:
        node_id: 节点唯一标识
        node_type: 节点类型
        label: 显示名称
        description: 节点描述
        config: 节点类型特定配置（如 agent 名称、超时、重试次数等）
        next_nodes: 后继节点 ID 列表（用于顺序/分支边）
        parallel_branches: 并行分支定义（仅 parallel 节点使用）
        condition: 条件表达式（用于条件边）
        timeout_seconds: 节点超时秒数（默认 60）
        max_attempts: 最大尝试次数（默认 1）
        retry_delay_seconds: 重试间隔秒数（默认 5）
    """
    node_id: str
    node_type: NodeType
    label: str = ""
    description: str = ""
    config: Dict[str, Any] = field(default_factory=dict)
    next_nodes: List[str] = field(default_factory=list)
    parallel_branches: List[List[str]] = field(default_factory=list)
    condition: Optional[str] = None  # Python 表达式字符串
    timeout_seconds: int = 60
    max_attempts: int = 1
    retry_delay_seconds: int = 5

    def to_dict(self) -> Dict[str, Any]:
        return {
            "nodeId": self.node_id,
            "nodeType": self.node_type.value,
            "label": self.label,
            "description": self.description,
            "config": self.config,
            "nextNodes": self.next_nodes,
            "parallelBranches": self.parallel_branches,
            "condition": self.condition,
            "timeoutSeconds": self.timeout_seconds,
            "maxAttempts": self.max_attempts,
            "retryDelaySeconds": self.retry_delay_seconds,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "NodeConfig":
        return cls(
            node_id=d["nodeId"],
            node_type=NodeType(d["nodeType"]),
            label=d.get("label", ""),
            description=d.get("description", ""),
            config=d.get("config", {}),
            next_nodes=d.get("nextNodes", []),
            parallel_branches=d.get("parallelBranches", []),
            condition=d.get("condition"),
            timeout_seconds=d.get("timeoutSeconds", 60),
            max_attempts=d.get("maxAttempts", 1),
            retry_delay_seconds=d.get("retryDelaySeconds", 5),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# WorkflowDefinition
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class WorkflowDefinition:
    """Workflow 定义模板。

    Attributes:
        id: 全局唯一 ID
        name: 名称
        description: 描述
        category: 分类（如 "拥堵处置"、"事故联动"）
        status: 状态（draft/active/deprecated）
        nodes: 节点配置列表
        entry_node_id: 入口节点 ID
        metadata: 扩展元数据
        created_at: 创建时间
        updated_at: 更新时间
    """
    id: str
    name: str
    description: str = ""
    category: str = ""
    status: DefinitionStatus = DefinitionStatus.DRAFT
    nodes: List[NodeConfig] = field(default_factory=list)
    entry_node_id: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = _utc_now_iso()
        if not self.updated_at:
            self.updated_at = self.created_at

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "category": self.category,
            "status": self.status.value,
            "nodes": [n.to_dict() for n in self.nodes],
            "entryNodeId": self.entry_node_id,
            "metadata": self.metadata,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "WorkflowDefinition":
        return cls(
            id=d["id"],
            name=d["name"],
            description=d.get("description", ""),
            category=d.get("category", ""),
            status=DefinitionStatus(d.get("status", "draft")),
            nodes=[NodeConfig.from_dict(n) for n in d.get("nodes", [])],
            entry_node_id=d.get("entryNodeId", ""),
            metadata=d.get("metadata", {}),
            created_at=d.get("createdAt", ""),
            updated_at=d.get("updatedAt", ""),
        )

    def get_node(self, node_id: str) -> Optional[NodeConfig]:
        """按 ID 获取节点配置。"""
        for n in self.nodes:
            if n.node_id == node_id:
                return n
        return None

    def get_entry_node(self) -> Optional[NodeConfig]:
        """获取入口节点。"""
        return self.get_node(self.entry_node_id)

    def validate(self) -> List[str]:
        """校验定义完整性。返回问题列表，空列表表示合法。"""
        issues: List[str] = []
        node_ids = {n.node_id for n in self.nodes}

        if not self.entry_node_id:
            issues.append("缺少入口节点 (entry_node_id)")
        elif self.entry_node_id not in node_ids:
            issues.append(f"入口节点 '{self.entry_node_id}' 不在节点列表中")

        entry = self.get_entry_node()
        if entry and entry.node_type != NodeType.TRIGGER:
            issues.append(f"入口节点必须是 trigger 类型，当前为 {entry.node_type.value}")

        # 必须有 close 节点
        has_close = any(n.node_type == NodeType.CLOSE for n in self.nodes)
        if not has_close:
            issues.append("缺少 close 节点")

        # 检查所有 next_nodes 引用有效
        for n in self.nodes:
            for next_id in n.next_nodes:
                if next_id not in node_ids:
                    issues.append(f"节点 '{n.node_id}' 引用不存在的后继节点 '{next_id}'")

        # 条件节点必须有 condition，且 condition 必须通过 DSL 校验
        from backend.workflow.condition import validate_condition_structure
        import json as _json
        for n in self.nodes:
            if n.node_type == NodeType.RISK_GATE:
                if not n.condition:
                    issues.append(f"risk_gate 节点 '{n.node_id}' 缺少条件表达式")
                else:
                    # 尝试解析并校验 DSL
                    cond = n.condition
                    cond_obj = None
                    if isinstance(cond, str) and cond.strip().startswith("{"):
                        try:
                            cond_obj = _json.loads(cond)
                        except _json.JSONDecodeError:
                            pass
                    if cond_obj is not None:
                        dsl_issues = validate_condition_structure(cond_obj)
                        for di in dsl_issues:
                            issues.append(f"risk_gate '{n.node_id}' 条件: {di}")
                    elif isinstance(cond, str):
                        # 命名条件：无需额外校验
                        from backend.workflow.condition import condition_from_expr
                        try:
                            condition_from_expr(cond)
                        except Exception as e:
                            issues.append(f"risk_gate '{n.node_id}' 条件无效: {e}")

        # parallel 节点必须有 parallel_branches
        for n in self.nodes:
            if n.node_type == NodeType.PARALLEL and not n.parallel_branches:
                issues.append(f"parallel 节点 '{n.node_id}' 缺少并行分支定义")

        return issues


# ═══════════════════════════════════════════════════════════════════════════════
# WorkflowDefinitionVersion
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class WorkflowDefinitionVersion:
    """Workflow 定义的不可变版本快照。

    每次修改 Definition 时递增版本号，Run 绑定到特定版本。
    已启动的 Run 不受后续 Definition 修改影响。

    Attributes:
        id: 版本唯一 ID
        definition_id: 所属 Definition ID
        version: 版本号（从 1 开始递增）
        definition_json: 完整 DAG JSON（冻结快照）
        changelog: 变更说明
        created_at: 创建时间
    """
    id: str
    definition_id: str
    version: int
    definition_json: Dict[str, Any] = field(default_factory=dict)
    changelog: str = ""
    created_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = _utc_now_iso()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "definitionId": self.definition_id,
            "version": self.version,
            "definitionJson": self.definition_json,
            "changelog": self.changelog,
            "createdAt": self.created_at,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "WorkflowDefinitionVersion":
        return cls(
            id=d["id"],
            definition_id=d["definitionId"],
            version=d["version"],
            definition_json=d.get("definitionJson", {}),
            changelog=d.get("changelog", ""),
            created_at=d.get("createdAt", ""),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# WorkflowRun
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class WorkflowRun:
    """一次 Workflow 执行实例。

    Attributes:
        run_id: 运行唯一 ID
        definition_id: 使用的 Definition ID
        version: 绑定的版本号
        session_id: 关联的 Chat Session ID
        event_thread_id: 关联的 Memory Event Thread ID
        status: 运行状态
        current_node_id: 当前执行节点 ID
        state: TrafficWorkflowState 的 dict 序列化
        started_at: 启动时间
        updated_at: 最后更新时间
        completed_at: 完成时间
        triggered_by: 触发者（用户 ID 或系统）
    """
    run_id: str
    definition_id: str = ""
    version: int = 1
    session_id: str = ""
    event_thread_id: str = ""
    status: WorkflowRunStatus = WorkflowRunStatus.PENDING
    current_node_id: str = ""
    state: Dict[str, Any] = field(default_factory=dict)
    started_at: str = ""
    updated_at: str = ""
    completed_at: str = ""
    triggered_by: str = "system"

    def __post_init__(self):
        now = _utc_now_iso()
        if not self.updated_at:
            self.updated_at = now

    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def is_interruptible(self) -> bool:
        return self.status in INTERRUPTIBLE_STATUSES

    def to_dict(self) -> Dict[str, Any]:
        return {
            "runId": self.run_id,
            "definitionId": self.definition_id,
            "version": self.version,
            "sessionId": self.session_id,
            "eventThreadId": self.event_thread_id,
            "status": self.status.value,
            "currentNodeId": self.current_node_id,
            "state": self.state,
            "startedAt": self.started_at,
            "updatedAt": self.updated_at,
            "completedAt": self.completed_at,
            "triggeredBy": self.triggered_by,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "WorkflowRun":
        status_raw = d.get("status", "pending")
        if isinstance(status_raw, str):
            status = WorkflowRunStatus(status_raw)
        else:
            status = status_raw
        return cls(
            run_id=d["runId"],
            definition_id=d.get("definitionId", ""),
            version=d.get("version", 1),
            session_id=d.get("sessionId", ""),
            event_thread_id=d.get("eventThreadId", ""),
            status=status,
            current_node_id=d.get("currentNodeId", ""),
            state=d.get("state", {}),
            started_at=d.get("startedAt", ""),
            updated_at=d.get("updatedAt", ""),
            completed_at=d.get("completedAt", ""),
            triggered_by=d.get("triggeredBy", "system"),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# WorkflowNodeRun
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class WorkflowNodeRun:
    """单节点执行记录。

    Attributes:
        node_run_id: 节点执行唯一 ID
        run_id: 所属 Run ID
        node_id: 节点标识
        node_type: 节点类型
        status: 执行状态
        attempt: 当前尝试次数
        max_attempts: 最大尝试次数
        input_snapshot: 执行前的输入快照（摘要）
        output_snapshot: 执行后的输出快照（摘要）
        error: 错误信息
        started_at: 开始时间
        completed_at: 完成时间
        duration_ms: 执行耗时（毫秒）
    """
    node_run_id: str
    run_id: str
    node_id: str
    node_type: NodeType = NodeType.TRIGGER
    status: NodeStatus = NodeStatus.PENDING
    attempt: int = 0
    max_attempts: int = 1
    input_snapshot: Dict[str, Any] = field(default_factory=dict)
    output_snapshot: Dict[str, Any] = field(default_factory=dict)
    error: str = ""
    started_at: str = ""
    completed_at: str = ""
    duration_ms: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "nodeRunId": self.node_run_id,
            "runId": self.run_id,
            "nodeId": self.node_id,
            "nodeType": self.node_type.value,
            "status": self.status.value,
            "attempt": self.attempt,
            "maxAttempts": self.max_attempts,
            "inputSnapshot": self.input_snapshot,
            "outputSnapshot": self.output_snapshot,
            "error": self.error,
            "startedAt": self.started_at,
            "completedAt": self.completed_at,
            "durationMs": self.duration_ms,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "WorkflowNodeRun":
        return cls(
            node_run_id=d["nodeRunId"],
            run_id=d["runId"],
            node_id=d["nodeId"],
            node_type=NodeType(d.get("nodeType", "trigger")),
            status=NodeStatus(d.get("status", "pending")),
            attempt=d.get("attempt", 0),
            max_attempts=d.get("maxAttempts", 1),
            input_snapshot=d.get("inputSnapshot", {}),
            output_snapshot=d.get("outputSnapshot", {}),
            error=d.get("error", ""),
            started_at=d.get("startedAt", ""),
            completed_at=d.get("completedAt", ""),
            duration_ms=d.get("durationMs", 0),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# WorkflowEvent
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class WorkflowEvent:
    """审计事件。

    Attributes:
        event_id: 事件唯一 ID
        run_id: 所属 Run ID
        node_id: 触发节点 ID（可为空）
        event_type: 事件类型
        payload: 事件负载
        sequence: 事件序号
        created_at: 创建时间
    """
    event_id: str
    run_id: str
    node_id: str = ""
    event_type: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    sequence: int = 0
    created_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = _utc_now_iso()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "eventId": self.event_id,
            "runId": self.run_id,
            "nodeId": self.node_id,
            "eventType": self.event_type,
            "payload": self.payload,
            "sequence": self.sequence,
            "createdAt": self.created_at,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "WorkflowEvent":
        return cls(
            event_id=d["eventId"],
            run_id=d["runId"],
            node_id=d.get("nodeId", ""),
            event_type=d.get("eventType", ""),
            payload=d.get("payload", {}),
            sequence=d.get("sequence", 0),
            created_at=d.get("createdAt", ""),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# WorkflowApproval
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class WorkflowApproval:
    """人工审批记录。

    Attributes:
        approval_id: 审批唯一 ID
        run_id: 所属 Run ID
        node_id: 审批节点 ID
        proposed_actions: 提议的动作列表
        edited_actions: 编辑后的动作列表（edit_and_approve 时使用）
        decision: 审批决策
        reviewer: 审批人
        comment: 审批意见
        created_at: 创建时间
        decided_at: 决策时间
    """
    approval_id: str
    run_id: str
    node_id: str = ""
    proposed_actions: List[Dict[str, Any]] = field(default_factory=list)
    edited_actions: List[Dict[str, Any]] = field(default_factory=list)
    decision: ApprovalDecision = ApprovalDecision.PENDING
    reviewer: str = ""
    comment: str = ""
    created_at: str = ""
    decided_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = _utc_now_iso()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "approvalId": self.approval_id,
            "runId": self.run_id,
            "nodeId": self.node_id,
            "proposedActions": self.proposed_actions,
            "editedActions": self.edited_actions,
            "decision": self.decision.value,
            "reviewer": self.reviewer,
            "comment": self.comment,
            "createdAt": self.created_at,
            "decidedAt": self.decided_at,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "WorkflowApproval":
        return cls(
            approval_id=d["approvalId"],
            run_id=d["runId"],
            node_id=d.get("nodeId", ""),
            proposed_actions=d.get("proposedActions", []),
            edited_actions=d.get("editedActions", []),
            decision=ApprovalDecision(d.get("decision", "pending")),
            reviewer=d.get("reviewer", ""),
            comment=d.get("comment", ""),
            created_at=d.get("createdAt", ""),
            decided_at=d.get("decidedAt", ""),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# WorkflowActionRecord
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class WorkflowActionRecord:
    """外部动作执行记录。

    幂等键设计: workflowRunId:nodeId:actionType:semanticActionVersion
    重复 resume 或 retry 不得重复执行已成功动作。

    Attributes:
        action_id: 动作唯一 ID
        run_id: 所属 Run ID
        node_id: 触发节点 ID
        action_type: 动作类型（如 "notify_wechat", "adjust_signal"）
        idempotency_key: 幂等键（格式含 run/node/action/semantic version）
        params: 动作参数
        result: 执行结果
        status: 执行状态
        error: 错误信息
        created_at: 创建时间
        completed_at: 完成时间
    """
    action_id: str
    run_id: str
    node_id: str = ""
    action_type: str = ""
    idempotency_key: str = ""
    params: Dict[str, Any] = field(default_factory=dict)
    result: Dict[str, Any] = field(default_factory=dict)
    status: ActionStatus = ActionStatus.PENDING
    error: str = ""
    created_at: str = ""
    completed_at: str = ""
    event_id: str = ""
    semantic_action_version: str = "v1"
    attempt: int = 0
    started_at: str = ""
    finished_at: str = ""
    request_metadata: Dict[str, Any] = field(default_factory=dict)
    external_reference: str = ""
    last_reconciled_at: str = ""
    unknown_since: str = ""
    reconciliation_attempts: int = 0
    reconciliation_supported: bool = False
    retryable: bool = False
    reconciliation_message: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = _utc_now_iso()
        if not self.idempotency_key:
            self.idempotency_key = compute_action_idempotency_key(
                self.run_id, self.node_id, self.action_type,
                self.semantic_action_version,
            )
        if not self.started_at and self.status in {
            ActionStatus.RUNNING,
            ActionStatus.EXECUTING,
            ActionStatus.SUCCEEDED,
            ActionStatus.FAILED,
            ActionStatus.UNKNOWN,
        }:
            self.started_at = self.created_at
        if not self.finished_at and self.completed_at:
            self.finished_at = self.completed_at

    def to_dict(self) -> Dict[str, Any]:
        return {
            "actionId": self.action_id,
            "actionExecutionId": self.action_id,
            "runId": self.run_id,
            "workflowRunId": self.run_id,
            "nodeId": self.node_id,
            "eventId": self.event_id,
            "actionType": self.action_type,
            "idempotencyKey": self.idempotency_key,
            "semanticActionVersion": self.semantic_action_version,
            "attempt": self.attempt,
            "params": self.params,
            "requestMetadata": self.request_metadata,
            "result": self.result,
            "status": self.status.value,
            "error": self.error,
            "createdAt": self.created_at,
            "completedAt": self.completed_at,
            "startedAt": self.started_at or self.created_at,
            "finishedAt": self.finished_at or self.completed_at,
            "externalReference": self.external_reference or None,
            "lastReconciledAt": self.last_reconciled_at or None,
            "unknownSince": self.unknown_since or None,
            "reconciliationAttempts": int(self.reconciliation_attempts or 0),
            "reconciliationSupported": self.reconciliation_supported,
            "retryable": self.retryable,
            "reconciliationMessage": self.reconciliation_message or None,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "WorkflowActionRecord":
        return cls(
            action_id=d["actionId"],
            run_id=d["runId"],
            node_id=d.get("nodeId", ""),
            action_type=d.get("actionType", ""),
            idempotency_key=d.get("idempotencyKey", ""),
            params=d.get("params", {}),
            result=d.get("result", {}),
            status=ActionStatus(d.get("status", "pending")),
            error=d.get("error", ""),
            created_at=d.get("createdAt", ""),
            completed_at=d.get("completedAt", ""),
            event_id=d.get("eventId", ""),
            semantic_action_version=d.get("semanticActionVersion", "v1"),
            attempt=int(d.get("attempt", 0) or 0),
            started_at=d.get("startedAt", ""),
            finished_at=d.get("finishedAt", ""),
            request_metadata=d.get("requestMetadata", {}),
            external_reference=d.get("externalReference", "") or "",
            last_reconciled_at=d.get("lastReconciledAt", "") or "",
            unknown_since=d.get("unknownSince", "") or "",
            reconciliation_attempts=int(d.get("reconciliationAttempts", 0) or 0),
            reconciliation_supported=bool(d.get("reconciliationSupported", False)),
            retryable=bool(d.get("retryable", False)),
            reconciliation_message=d.get("reconciliationMessage", "") or "",
        )


@dataclass
class WorkflowActionAttempt:
    """Immutable history for one execution or reconciliation-visible attempt."""

    attempt_id: str
    action_id: str
    attempt: int
    status: ActionStatus = ActionStatus.PENDING
    started_at: str = ""
    finished_at: str = ""
    request_metadata: Dict[str, Any] = field(default_factory=dict)
    external_reference: str = ""
    result: Dict[str, Any] = field(default_factory=dict)
    error: str = ""
    last_reconciled_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "attemptId": self.attempt_id,
            "actionExecutionId": self.action_id,
            "attempt": self.attempt,
            "status": self.status.value,
            "startedAt": self.started_at or None,
            "finishedAt": self.finished_at or None,
            "requestMetadata": self.request_metadata,
            "externalReference": self.external_reference or None,
            "result": self.result,
            "error": self.error or None,
            "lastReconciledAt": self.last_reconciled_at or None,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 辅助函数
# ═══════════════════════════════════════════════════════════════════════════════


def _utc_now_iso() -> str:
    """返回 UTC 时间 ISO 格式字符串。"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_run_id() -> str:
    """生成 Workflow Run ID。"""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    short = uuid.uuid4().hex[:8]
    return f"wfrun_{ts}_{short}"


def generate_approval_id() -> str:
    """生成审批 ID。"""
    return f"wfappr_{uuid.uuid4().hex[:12]}"


def generate_action_id() -> str:
    """生成动作记录 ID。"""
    return f"wfact_{uuid.uuid4().hex[:12]}"


def compute_action_idempotency_key(
    run_id: str,
    node_id: str,
    action_type: str,
    semantic_action_version: str = "v1",
) -> str:
    """计算外部动作幂等键。

    格式: {runId}:{nodeId}:{actionType}:{semanticActionVersion} 的 SHA-256
    前 16 位。这确保同一语义版本只执行一次，同时允许显式的新语义
    版本形成新的副作用身份。
    """
    raw = f"{run_id}:{node_id}:{action_type}:{semantic_action_version or 'v1'}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def compute_legacy_action_idempotency_key(
    run_id: str,
    node_id: str,
    action_type: str,
) -> str:
    """Return the pre-21.3 three-part action identity.

    Existing databases can contain confirmed side effects keyed without a
    semantic version.  The runtime must continue to recognise those rows
    during an in-place upgrade so a crash/restart cannot dispatch them again.
    New records always use :func:`compute_action_idempotency_key`.
    """
    raw = f"{run_id}:{node_id}:{action_type}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def generate_event_id(run_id: str, seq: int) -> str:
    """生成审计事件 ID。"""
    return f"wfevent_{run_id}_{seq:06d}"


def generate_node_run_id(run_id: str, node_id: str, attempt: int) -> str:
    """生成节点执行记录 ID。"""
    return f"wfnr_{run_id}_{node_id}_{attempt}"
