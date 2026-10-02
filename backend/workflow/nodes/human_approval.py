"""
human_approval 节点 — 人工审批。

执行时暂停 Workflow，创建审批记录，等待人工决策。

审批操作：
  - approve: 批准，继续执行后续 action 节点
  - reject: 驳回，Workflow 终止
  - edit_and_approve: 编辑后批准

未经批准不得执行外部 action 节点。
"""

from datetime import datetime, timezone
from typing import Any, Dict

from backend.workflow.models import (
    ApprovalDecision,
    NodeConfig,
    generate_approval_id,
)
from backend.workflow.state import TrafficWorkflowState, WorkflowRunStatus


async def execute_human_approval(
    state: TrafficWorkflowState, config: NodeConfig
) -> Dict[str, Any]:
    """执行人工审批节点（第一阶段：创建审批，暂停 Workflow）。

    审批阶段：
      1. 本方法创建审批记录并暂停 Workflow
      2. 人工通过 API 调用 approve/reject/edit_and_approve
      3. executor.resume() 继续执行

    Args:
        state: 工作流状态
        config: 节点配置

    Returns:
        审批信息（含 approval_id，供 API 调用）
    """
    # ── 恢复场景：仅当本节点声明的 action 已全部批准时才跳过 ──
    # Phase17：不能因为「本 run 已有 approved_actions」就跳过第二个 high-risk
    # 门禁（approval 是 actionType-scoped，多个门禁需各自独立审批）。
    # Phase18 V2：按 actionStepId 精确判断，绝不因同 actionType 已批准而跳过。
    declared = config.config.get("action_types", []) or []
    approval_identity_version = config.config.get("approval_identity_version", 1)
    target_action_step_id = config.config.get("target_action_step_id", "") or ""
    if state.pending_approval is None and declared:
        if approval_identity_version >= 2 and target_action_step_id:
            # V2：仅当该 target_action_step_id 已批准才跳过（exact instance）
            approved_step_ids = {
                pa.get("actionStepId")
                for pa in (state.approved_actions or [])
                if isinstance(pa, dict) and pa.get("actionStepId")
            }
            if target_action_step_id in approved_step_ids:
                state.add_audit_event("approval_completed", config.node_id, {
                    "status": "already_approved",
                })
                return {"approval_required": False, "status": "already_approved"}
        else:
            # legacy V1：actionType-scoped skip
            approved_types = {
                pa.get("actionType") or pa.get("action_type")
                for pa in (state.approved_actions or [])
                if isinstance(pa, dict)
            }
            if all(at in approved_types for at in declared):
                state.add_audit_event("approval_completed", config.node_id, {
                    "status": "already_approved",
                })
                return {"approval_required": False, "status": "already_approved"}

    # 收集提议的动作
    approval_identity_version = config.config.get("approval_identity_version", 1)
    target_action_step_id = config.config.get("target_action_step_id", "") or ""
    if approval_identity_version >= 2:
        # V2：compiler-driven，不继承 agent-proposed actions（避免 actionType-scoped 泄漏）
        proposed_actions = []
    else:
        proposed_actions = list(state.proposed_actions or [])

    # Phase 13: 若有结构化 proposal (actionType)，优先保留，不覆盖为文本摘要
    has_structured = any(
        isinstance(pa, dict) and "actionType" in pa
        for pa in proposed_actions
    )

    # 如果没有结构化提议，从 Agent 输出和建议中构建文本摘要（仅 V1）
    if approval_identity_version < 2 and not has_structured:
        agent_outputs = state.agent_outputs or {}
        for name, output in agent_outputs.items():
            if isinstance(output, dict) and output.get("summary"):
                proposed_actions.append({
                    "source": name,
                    "action": output.get("summary", ""),
                    "urgency": output.get("urgency", "low"),
                    "evidenceRefs": output.get("evidenceRefs", []),
                })
        state.proposed_actions = proposed_actions

    # 模板声明的可执行动作类型 → 追加为结构化审批项。
    # 这确保 tool-level approval 绑定到具体 action，而非 run 级 bool：
    # 文本摘要审批只授权模板声明的动作，不会退化成「批准任意 high-risk tool」。
    # Phase18 V2：绑定 exact actionStepId（approvalIdentityVersion=2），按 actionStepId 去重。
    declared_action_types = config.config.get("action_types", []) or []
    for at in declared_action_types:
        if approval_identity_version >= 2 and target_action_step_id:
            already = any(
                isinstance(pa, dict) and pa.get("actionStepId") == target_action_step_id
                for pa in proposed_actions
            )
        else:
            already = any(
                isinstance(pa, dict) and (pa.get("actionType") or pa.get("action_type")) == at
                for pa in proposed_actions
            )
        if not already:
            entry: Dict[str, Any] = {"actionType": at, "source": "workflow_template"}
            if approval_identity_version >= 2 and target_action_step_id:
                entry["actionStepId"] = target_action_step_id
            proposed_actions.append(entry)

    # 从 rule_router 结果中获取审批原因
    risk = state.risk_assessment or {}
    event = state.current_event or {}

    # 创建审批记录
    approval_id = generate_approval_id()
    approval = {
        "approvalId": approval_id,
        "workflowRunId": state.workflow_run_id,
        "nodeId": config.node_id,
        "proposedActions": proposed_actions,
        "decision": ApprovalDecision.PENDING.value,
        "reviewer": "",
        "comment": "",
        "createdAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "context": {
            "riskLevel": risk.get("riskLevel", "未知"),
            "riskScore": risk.get("riskScore", 0),
            "eventType": event.get("eventTypeCn", event.get("eventType", "")),
            "roadName": event.get("roadName", ""),
            "agentCount": len(state.agent_outputs or {}),
        },
    }

    # 更新 state
    state.pending_approval = approval
    state.approval_ids.append(approval_id)
    state.transition(WorkflowRunStatus.AWAITING_APPROVAL)

    state.add_audit_event("approval_required", config.node_id, {
        "approvalId": approval_id,
        "actionCount": len(proposed_actions),
        "riskLevel": risk.get("riskLevel", ""),
    })

    return {
        "approval_required": True,
        "approval_id": approval_id,
        "proposed_actions": proposed_actions,
        "pause_reason": "需要人工审批",
    }


def process_approval_decision(
    state: TrafficWorkflowState,
    decision: ApprovalDecision,
    edited_actions: list = None,
    reviewer: str = "",
    comment: str = "",
) -> Dict[str, Any]:
    """处理审批决策（由 API 调用触发，非节点执行）。

    Args:
        state: 工作流状态
        decision: 审批决策
        edited_actions: 编辑后的动作（edit_and_approve 时）
        reviewer: 审批人
        comment: 审批意见

    Returns:
        处理结果
    """
    pending = state.pending_approval
    if not pending:
        return {"error": "没有待处理的审批"}

    approval_id = pending.get("approvalId", "")

    if decision == ApprovalDecision.APPROVED:
        state.approved_actions = pending.get("proposedActions", [])
        state.add_audit_event("approval_approved", pending.get("nodeId", ""), {
            "approvalId": approval_id,
            "reviewer": reviewer,
            "comment": comment,
        })
        result = {"decision": "approved", "approved_actions": state.approved_actions}

    elif decision == ApprovalDecision.REJECTED:
        state.approved_actions = []
        state.transition(WorkflowRunStatus.REJECTED)
        state.add_audit_event("approval_rejected", pending.get("nodeId", ""), {
            "approvalId": approval_id,
            "reviewer": reviewer,
            "comment": comment,
        })
        result = {"decision": "rejected", "reason": comment or "审批人驳回"}

    elif decision == ApprovalDecision.EDITED:
        edited = edited_actions or pending.get("proposedActions", [])
        # Phase18 V2：actionStepId 是 server-owned identity（来自 pending approval）。
        # 客户端 edit_and_approve payload 不得篡改 actionStepId —— 强制回填 server 值。
        server_owned_step_id = ""
        for pa in (pending.get("proposedActions", []) or []):
            if isinstance(pa, dict) and pa.get("actionStepId"):
                server_owned_step_id = pa.get("actionStepId")
                break
        if server_owned_step_id:
            sanitized = []
            for item in edited:
                if isinstance(item, dict):
                    item = dict(item)
                    item["actionStepId"] = server_owned_step_id  # 强制 server-owned
                    sanitized.append(item)
            edited = sanitized
        state.approved_actions = edited
        state.add_audit_event("approval_edited", pending.get("nodeId", ""), {
            "approvalId": approval_id,
            "reviewer": reviewer,
            "comment": comment,
            "editedActionCount": len(edited or []),
        })
        result = {"decision": "edited_and_approved", "approved_actions": state.approved_actions}

    else:
        return {"error": f"未知的审批决策: {decision}"}

    state.pending_approval = None
    return result
