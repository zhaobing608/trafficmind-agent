"""
action 节点 — 外部动作执行。

执行经批准的外部动作（通知、信号调整、派单等）。

幂等性保证：
  - 使用 idempotency identity = {runId}:{nodeId}:{actionType}:{semanticActionVersion}
  - 重复 resume 或 retry 不重复执行已成功动作
  - 通过 WorkflowActionRecord 表做幂等检查

未经 human_approval 批准不得执行 action 节点。
"""

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict

from backend.workflow.models import (
    ActionStatus,
    NodeConfig,
    WorkflowActionRecord,
    compute_action_idempotency_key,
    compute_legacy_action_idempotency_key,
    generate_action_id,
)
from backend.workflow.state import TrafficWorkflowState
from backend.agent.tool_policy import (
    ToolExecutionStatus,
    classify_tool_result,
    enforce_tool_request,
)
from backend.agent.tool_registry import ToolRisk, get_tool_registry
from backend.workflow.action_execution import (
    ActionExecutionContext,
    ActionExecutorResult,
    contains_sensitive_key,
    get_action_executor_registry,
    sanitize_public_value,
    sanitize_public_text,
)


# Agent 提案 actionType → workflow action_type 别名（二者命名不一致）
#   Agent 提案: traffic_diversion / signal_adjustment / ...
#   workflow action: simulation_traffic_diversion / simulation_signal_adjustment / ...
_ACTION_TYPE_ALIASES = {
    "traffic_diversion": "simulation_traffic_diversion",
    "signal_adjustment": "simulation_signal_adjustment",
    "signal_adjust": "simulation_signal_adjustment",
    "lane_control": "simulation_lane_control",
    "dispatch_coordination": "simulation_dispatch_coordination",
}


def _canonical_action_type(action_type: str) -> str:
    """将 action type 归一化到 workflow action_type 命名空间。"""
    normalized = str(action_type or "").strip().lower()
    return _ACTION_TYPE_ALIASES.get(normalized, normalized)


def is_current_action_approved(
    state: TrafficWorkflowState,
    action_type: str,
    config: NodeConfig = None,
) -> bool:
    """审批是否绑定到当前具体 action（而非 run 级 bool）。

    语义（fail-closed）：
      - V2（approvalIdentityVersion=2）：exact actionStepId == config.node_id 匹配。
        缺 actionStepId 的条目永不匹配 —— 绝不 fallback actionType。
      - legacy V1：approved_actions 中存在 actionType（归一化后）与当前 action_type
        一致 → 仅该 action 被授权。
      - 文本摘要（无 actionType）不授权任何未声明的 high-risk tool。
      - 空 approved_actions → 未批准。

    避免「批准 A 后，B 也被视为已批准」以及「run 有审批 → 任意 high-risk 放行」
    两类 scope escalation。
    """
    approved = state.approved_actions or []
    if not approved:
        return False

    identity_version = 1
    if config is not None:
        identity_version = config.config.get("approval_identity_version", 1)

    if identity_version >= 2:
        # V2：exact actionStepId == config.node_id（node_id 即 canonical stepId）
        target_step_id = config.node_id if config is not None else ""
        for item in approved:
            if not isinstance(item, dict):
                continue
            if item.get("actionStepId") == target_step_id:
                return True
        return False

    # legacy V1：actionType 匹配
    target = _canonical_action_type(action_type)
    for item in approved:
        if not isinstance(item, dict):
            continue
        at = item.get("actionType") or item.get("action_type")
        if at and _canonical_action_type(at) == target:
            return True

    # fail closed：无结构化 actionType 匹配 → 未批准
    return False


def _durable_approval_allows(repository, run_id: str, config: NodeConfig, action_type: str) -> bool:
    """Last-line high-risk approval check using persisted server-owned data."""
    if repository is None:
        return False
    identity_version = int(config.config.get("approval_identity_version", 1) or 1)
    for approval in repository.list_approvals(run_id):
        if approval.decision.value not in {"approved", "edited"}:
            continue
        actions = approval.edited_actions if approval.decision.value == "edited" else approval.proposed_actions
        for item in actions or []:
            if not isinstance(item, dict):
                continue
            if identity_version >= 2:
                if item.get("actionStepId") == config.node_id:
                    return True
            else:
                candidate = item.get("actionType") or item.get("action_type")
                if candidate and _canonical_action_type(str(candidate)) == _canonical_action_type(action_type):
                    return True
    return False


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


async def execute_action(
    state: TrafficWorkflowState, config: NodeConfig, repository=None,
    driver_owner: str = "", driver_generation: int = 0,
    defer_terminal: bool = False,
) -> Dict[str, Any]:
    """Validate, claim, execute and durably close one action attempt.

    Workflow callers set ``defer_terminal`` so the action terminal result and
    the node/run checkpoint commit in one SQLite transaction.  Direct callers
    still receive a self-contained, durably finalised execution.
    """
    action_type = _canonical_action_type(config.config.get("action_type", ""))
    if not action_type:
        return {"error": "action 节点缺少 action_type 配置"}
    semantic_version = str(
        config.config.get("semantic_action_version")
        or config.config.get("semanticActionVersion")
        or "v1"
    )
    action_id = generate_action_id()
    idempotency_key = compute_action_idempotency_key(
        state.workflow_run_id, config.node_id, action_type, semantic_version
    )
    legacy_idempotency_key = (
        compute_legacy_action_idempotency_key(
            state.workflow_run_id, config.node_id, action_type
        )
        if semantic_version == "v1"
        else ""
    )

    def _record_block(reason: str) -> None:
        """Persist a credential-free denial with canonical run/event scope."""
        payload = {
            "actionExecutionId": action_id,
            "workflowRunId": state.workflow_run_id,
            "nodeId": config.node_id,
            "eventId": str((state.current_event or {}).get("eventId") or "") or None,
            "actionType": action_type,
            "attempt": 0,
            "idempotencyKey": idempotency_key,
            "reason": str(reason)[:500],
        }
        state.add_audit_event("action_blocked", config.node_id, payload)
        if repository is not None and state.workflow_run_id:
            repository.append_event(
                state.workflow_run_id,
                "action_blocked",
                node_id=config.node_id,
                payload=payload,
            )

    if (state.current_event or {}).get("actionExecutionAllowed") is False:
        _record_block("replay run is analysis-only")
        return {
            "action_type": action_type,
            "status": "denied",
            "executed": False,
            "reason": "replay run is analysis-only",
            "error": "replay run is analysis-only",
        }

    def _driver_gate() -> str:
        """driver-managed execution gate：identity + lease 未过期 + 非 CANCELLED。

        返回 'ok' | 'cancelled' | 'lease_lost'。legacy（无 driver context）恒为 'ok'。
        """
        if repository:
            durable = repository.get_run(state.workflow_run_id)
            if durable is not None and durable.status.value == "cancelled":
                return "cancelled"
        if not repository or not driver_owner:
            return "ok"
        if repository.is_driver_execution_valid(state.workflow_run_id, driver_owner, driver_generation):
            return "ok"
        durable = repository.get_run(state.workflow_run_id)
        if durable is not None and durable.status.value == "cancelled":
            return "cancelled"
        return "lease_lost"

    def _lease_lost_result() -> Dict[str, Any]:
        return {
            "actionExecutionId": action_id,
            "action_type": action_type,
            "status": "lease_lost",
            "executed": False,
            "reason": "driver lease lost",
        }

    def _cancelled_result(reason: str) -> Dict[str, Any]:
        return {
            "actionExecutionId": action_id,
            "action_type": action_type,
            "status": "cancelled",
            "executed": False,
            "reason": reason,
        }

    reliable_executor = get_action_executor_registry().get(action_type)
    canonical_action_type = (
        get_action_executor_registry().canonical(action_type)
        if reliable_executor is not None else action_type
    )

    # ToolPolicy is the first gate; persisted approval below is the final gate.
    risk = state.risk_assessment or {}
    _policy = enforce_tool_request(
        action_type,
        caller=f"workflow:{state.workflow_run_id}:{config.node_id}",
        context={"riskLevel": risk.get("riskLevel", "")},
        is_approved=is_current_action_approved(state, action_type, config),
    )
    if not _policy["allowed"]:
        audit_payload = {
            **_policy["audit"],  # tool, caller, riskLevel, decision, reason, timestamp
            "actionExecutionId": action_id,
            "actionType": action_type,
            "workflowRunId": state.workflow_run_id,
            "nodeId": config.node_id,
            "attempt": 0,
            "executed": False,
        }
        event_type = (
            "tool_denied"
            if _policy["status"] == ToolExecutionStatus.DENIED.value
            else "tool_approval_required"
        )
        state.add_audit_event(event_type, config.node_id, audit_payload)
        _record_block(_policy["reason"])
        return {
            "actionExecutionId": action_id,
            "action_type": action_type,
            "status": _policy["status"],
            "executed": False,
            "reason": _policy["reason"],
            "approvalRequired": _policy["approvalRequired"],
        }

    # 检查是否有待审批但未批准的审批
    pending = state.pending_approval
    if pending:
        _record_block("存在未处理的审批，不能执行外部动作")
        return {
            "error": "存在未处理的审批，不能执行外部动作",
            "approval_id": pending.get("approvalId"),
        }

    # ── 优先使用审批后的 edited_actions，否则用节点配置 ──────────
    # Phase18 V2：business params 来自 compiler 归一化的 config.action_params，
    # 不由 approved_actions 覆盖（approved_actions 仅作审批绑定，不含 params）。
    action_params = config.config.get("action_params", {})
    if not isinstance(action_params, dict):
        action_params = {}
    identity_version = config.config.get("approval_identity_version", 1)
    if identity_version < 2 and state.approved_actions:
        # 查找匹配当前 action_type 的已批准动作
        for approved in state.approved_actions:
            if (
                isinstance(approved, dict)
                and _canonical_action_type(approved.get("actionType", "")) == action_type
            ):
                edited_params = approved.get("params", approved.get("action_params"))
                if isinstance(edited_params, dict):
                    action_params = edited_params
                break
            # Phase 13: 结构化 proposal 直接使用自身作为 params
            if (
                isinstance(approved, dict)
                and approved.get("actionType")
                and action_type.startswith("simulation_")
            ):
                # 映射 agent proposal 格式到 action 格式
                action_params = {
                    "targetIds": approved.get("sourceRoadId", approved.get("targetRoadIds", [])),
                    "parameters": {
                        "diversionRatio": approved.get("diversionRatio", 0.35),
                    },
                }
                if isinstance(action_params["targetIds"], str):
                    action_params["targetIds"] = [action_params["targetIds"]]
                # 添加 targetRoadIds
                tr = approved.get("targetRoadIds", [])
                if tr:
                    action_params["targetIds"] = [approved.get("sourceRoadId", tr[0])] + tr
                break

    # Credentials are server configuration, never plan/action parameters.
    if contains_sensitive_key(action_params):
        reason = "Action 参数包含禁止持久化的 credential/secret 字段"
        _record_block(reason)
        return {
            "action_type": canonical_action_type,
            "status": "blocked",
            "executed": False,
            "reason": reason,
            "error": reason,
        }
    safe_params = sanitize_public_value(action_params)
    if not isinstance(safe_params, dict):
        safe_params = {}

    # Immutable replay/canonical identity and durable high-risk approval fence.
    durable_run = repository.get_run(state.workflow_run_id) if repository and state.workflow_run_id else None
    definition = None
    if durable_run is not None:
        durable_state = durable_run.state if isinstance(durable_run.state, dict) else {}
        durable_event = durable_state.get("currentEvent") or {}
        durable_event_id = str(durable_event.get("eventId") or "") if isinstance(durable_event, dict) else ""
        state_event_id = str((state.current_event or {}).get("eventId") or "")
        durable_pending = (
            durable_state.get("pendingApproval")
            or durable_state.get("pending_approval")
        )
        if isinstance(durable_pending, dict):
            _record_block("持久化 Workflow 仍存在未处理审批")
            return {
                "action_type": canonical_action_type,
                "status": "blocked",
                "executed": False,
                "reason": "持久化 Workflow 仍存在未处理审批",
                "error": "持久化 Workflow 仍存在未处理审批",
            }
        durable_approved = durable_state.get("approvedActions")
        if durable_approved is None:
            durable_approved = durable_state.get("approved_actions") or []
        if reliable_executor is not None and list(state.approved_actions or []) != list(
            durable_approved or []
        ):
            _record_block("Action approval state 与 durable Workflow 不一致")
            return {
                "action_type": canonical_action_type,
                "status": "blocked",
                "executed": False,
                "reason": "Action approval state 与 durable Workflow 不一致",
                "error": "Action approval state 与 durable Workflow 不一致",
            }
        if durable_event.get("actionExecutionAllowed") is False:
            _record_block("replay run is analysis-only")
            return {
                "action_type": canonical_action_type,
                "status": "blocked",
                "executed": False,
                "reason": "replay run is analysis-only",
                "error": "replay run is analysis-only",
            }
        if reliable_executor is not None and (not durable_event_id or durable_event_id != state_event_id):
            _record_block("canonical event identity mismatch")
            return {
                "action_type": canonical_action_type,
                "status": "blocked",
                "executed": False,
                "reason": "canonical event identity mismatch",
                "error": "canonical event identity mismatch",
            }
        definition = None
        if durable_run.definition_id:
            # A Run is bound to an immutable Definition version.  The mutable
            # latest definition must never authorize parameters or action types
            # for an already-created Run.
            durable_version = repository.get_definition_version(
                durable_run.definition_id,
                durable_run.version,
            )
            if durable_version is not None:
                from backend.workflow.models import WorkflowDefinition
                definition = WorkflowDefinition.from_dict(
                    durable_version.definition_json
                )
            else:
                # Compatibility for old/direct records created before version
                # snapshots were mandatory.
                definition = repository.get_definition(durable_run.definition_id)
        if definition is not None:
            metadata = definition.metadata if isinstance(definition.metadata, dict) else {}
            plan_metadata = metadata.get("plan") if isinstance(metadata.get("plan"), dict) else {}
            nested_plan_metadata = (
                plan_metadata.get("metadata")
                if isinstance(plan_metadata.get("metadata"), dict)
                else {}
            )
            if (
                metadata.get("actionExecutionAllowed") is False
                or metadata.get("runKind") == "replay"
                or plan_metadata.get("actionExecutionAllowed") is False
                or plan_metadata.get("runKind") == "replay"
                or nested_plan_metadata.get("actionExecutionAllowed") is False
                or nested_plan_metadata.get("runKind") == "replay"
            ):
                _record_block("replay-derived definition cannot execute actions")
                return {
                    "action_type": canonical_action_type,
                    "status": "blocked",
                    "executed": False,
                    "reason": "replay-derived definition cannot execute actions",
                    "error": "replay-derived definition cannot execute actions",
                }

    if reliable_executor is not None:
        if durable_run is None or definition is None:
            if durable_run is not None:
                _record_block("reliable Action 必须绑定持久化 Workflow Definition/Run")
            return {
                "action_type": canonical_action_type,
                "status": "blocked",
                "executed": False,
                "reason": "reliable Action 必须绑定持久化 Workflow Definition/Run",
                "error": "reliable Action 必须绑定持久化 Workflow Definition/Run",
            }
        durable_node = definition.get_node(config.node_id)
        durable_action_type = (
            _canonical_action_type(durable_node.config.get("action_type", ""))
            if durable_node is not None else ""
        )
        durable_params = (
            durable_node.config.get("action_params", {})
            if durable_node is not None
            else {}
        )
        configured_params = config.config.get("action_params", {})
        durable_identity_version = int(
            durable_node.config.get("approval_identity_version", 1) or 1
        ) if durable_node is not None else 1
        configured_identity_version = int(
            config.config.get("approval_identity_version", 1) or 1
        )
        durable_semantic_version = str(
            durable_node.config.get("semantic_action_version")
            or durable_node.config.get("semanticActionVersion")
            or "v1"
        ) if durable_node is not None else "v1"
        configured_semantic_version = str(
            config.config.get("semantic_action_version")
            or config.config.get("semanticActionVersion")
            or "v1"
        )
        if (
            durable_node is None
            or durable_node.node_type.value != "action"
            or durable_action_type != action_type
            or durable_params != configured_params
            or durable_identity_version != configured_identity_version
            or durable_semantic_version != configured_semantic_version
        ):
            _record_block("Action 与不可变 Workflow Definition 不匹配")
            return {
                "action_type": canonical_action_type,
                "status": "blocked",
                "executed": False,
                "reason": "Action 与不可变 Workflow Definition 不匹配",
                "error": "Action 与不可变 Workflow Definition 不匹配",
            }

    tool_meta = get_tool_registry().get(action_type)
    if (
        tool_meta is not None
        and tool_meta.riskLevel == ToolRisk.HIGH_RISK
        and repository is not None
        and durable_run is not None
        and not _durable_approval_allows(repository, state.workflow_run_id, config, action_type)
    ):
        reason = "high-risk Action 缺少有效 durable approval"
        _record_block(reason)
        return {
            "action_type": canonical_action_type,
            "status": "blocked",
            "executed": False,
            "reason": reason,
            "error": reason,
        }

    if reliable_executor is not None and repository is None:
        return {
            "action_type": canonical_action_type,
            "status": "blocked",
            "executed": False,
            "reason": "reliable Action requires durable repository",
            "error": "reliable Action requires durable repository",
        }
    if reliable_executor is not None:
        from backend.tools.db_tools import get_event_by_id
        canonical_event_id = str((state.current_event or {}).get("eventId") or "")
        if not canonical_event_id or get_event_by_id(canonical_event_id) is None:
            reason = "reliable Action 必须绑定真实 canonical Event"
            _record_block(reason)
            return {
                "action_type": canonical_action_type,
                "status": "blocked",
                "executed": False,
                "reason": reason,
                "error": reason,
            }

    # ``canonical_action_type`` is registry-normalized and therefore must map
    # to the same stable key computed before the early safety gates.
    idempotency_key = compute_action_idempotency_key(
        state.workflow_run_id, config.node_id, canonical_action_type, semantic_version
    )
    event_id = str((state.current_event or {}).get("eventId") or "")
    request_metadata = {
        "executor": type(reliable_executor).__name__ if reliable_executor is not None else "legacy-dispatch",
        "idempotencyKey": idempotency_key,
        "semanticActionVersion": semantic_version,
    }
    record = WorkflowActionRecord(
        action_id=action_id,
        run_id=state.workflow_run_id,
        node_id=config.node_id,
        event_id=event_id,
        action_type=canonical_action_type,
        idempotency_key=idempotency_key,
        semantic_action_version=semantic_version,
        params=safe_params,
        request_metadata=request_metadata,
        reconciliation_supported=bool(
            getattr(reliable_executor, "reconciliation_supported", False)
        ),
        status=ActionStatus.PENDING,
    )

    # ── C1: budget/dispatch 前 gate（identity + lease 未过期 + 非 CANCELLED）──
    _gate = _driver_gate()
    if _gate == "cancelled":
        state.add_audit_event("action_cancelled_before_dispatch", config.node_id, {"actionType": action_type})
        _record_block("run cancelled before action")
        return _cancelled_result("run cancelled before action")
    if _gate == "lease_lost":
        state.add_audit_event("lease_lost", config.node_id, {"actionType": action_type})
        return _lease_lost_result()

    # A durable result is consulted before reserving another tool call.  This
    # is important after restart and for duplicate client delivery: a known
    # SUCCEEDED/UNKNOWN/RUNNING action must not consume retry budget or reach
    # the provider merely because the same command was submitted again.
    if repository:
        existing = repository.get_action_record_by_idempotency_key(idempotency_key)
        legacy_existing = (
            repository.get_action_record_by_idempotency_key(legacy_idempotency_key)
            if legacy_idempotency_key
            and legacy_idempotency_key != idempotency_key
            else None
        )
        if (
            existing is not None
            and legacy_existing is not None
            and existing.action_id != legacy_existing.action_id
        ):
            _record_block("检测到重复的 legacy/versioned Action identity，禁止再次执行")
            return {
                "actionExecutionId": existing.action_id,
                "action_type": canonical_action_type,
                "status": "blocked",
                "executed": False,
                "reason": "duplicate semantic Action identity",
                "error": "duplicate semantic Action identity",
            }
        if existing is None:
            existing = legacy_existing
        if existing is not None and existing.idempotency_key != idempotency_key:
            # Preserve the exact identity already sent to an external provider
            # by a pre-21.3 runtime.  Changing it during upgrade could create a
            # second side effect or make reconciliation query the wrong key.
            idempotency_key = existing.idempotency_key
            record.idempotency_key = idempotency_key
            record.request_metadata["idempotencyKey"] = idempotency_key
        if existing is not None and (
            existing.run_id != state.workflow_run_id
            or existing.node_id != config.node_id
            or (
                reliable_executor is not None
                and existing.event_id != event_id
            )
            or existing.action_type != canonical_action_type
            or existing.semantic_action_version != semantic_version
        ):
            action_id = existing.action_id
            _record_block("Action Execution durable identity mismatch")
            return {
                "action_id": existing.action_id,
                "actionExecutionId": existing.action_id,
                "action_type": canonical_action_type,
                "status": "blocked",
                "executed": False,
                "reason": "Action Execution durable identity mismatch",
                "error": "Action Execution durable identity mismatch",
            }
        if (
            existing is not None
            and existing.status != ActionStatus.PENDING
            # Pre-21.3 actions retain their configured in-node retry behavior;
            # reliable executors require the explicit FAILED → PENDING API.
            and not (
                reliable_executor is None
                and existing.status == ActionStatus.FAILED
            )
        ):
            if existing.status == ActionStatus.SUCCEEDED:
                return {
                    "action_id": existing.action_id,
                    "actionExecutionId": existing.action_id,
                    "action_type": canonical_action_type,
                    "status": "skipped",
                    "reason": "idempotent_skip",
                    "previous_result": sanitize_public_value(existing.result),
                }
            if existing.status in {
                ActionStatus.RUNNING,
                ActionStatus.EXECUTING,
                ActionStatus.UNKNOWN,
            }:
                return {
                    "action_id": existing.action_id,
                    "actionExecutionId": existing.action_id,
                    "action_type": canonical_action_type,
                    "status": (
                        "unknown"
                        if existing.status == ActionStatus.UNKNOWN
                        else "in_flight"
                    ),
                    "executed": False,
                    "reason": f"existing {existing.status.value} attempt",
                    "externalReference": existing.external_reference or None,
                }
            return {
                "action_id": existing.action_id,
                "actionExecutionId": existing.action_id,
                "action_type": canonical_action_type,
                "status": "blocked",
                "executed": False,
                "reason": "explicit retry required",
                "error": "explicit retry required",
            }
        if (
            reliable_executor is not None
            and durable_run is not None
            and durable_run.status.value not in {"pending", "running"}
        ):
            _record_block(
                f"Run 状态为 {durable_run.status.value}，禁止创建新的 Action 副作用"
            )
            return {
                "actionExecutionId": action_id,
                "action_type": canonical_action_type,
                "status": "blocked",
                "executed": False,
                "reason": "Workflow Run 当前状态不允许执行新 Action",
                "error": "Workflow Run 当前状态不允许执行新 Action",
            }

    # Budget is reserved before a durable claim; failure means no dispatch.
    if repository:
        from backend.planning.budget import reserve_tool_call_durable
        if not reserve_tool_call_durable(repository, state.workflow_run_id):
            state.add_audit_event("budget_exhausted", config.node_id, {
                "actionType": action_type, "reason": "tool budget exhausted",
            })
            _record_block("tool budget exhausted")
            return {
                "action_type": action_type,
                "status": "budget_exhausted",
                "executed": False,
                "reason": "tool budget exhausted",
            }

    # Durable compare-and-set claim is the only path to the side effect.
    if repository:
        try:
            if reliable_executor is None:
                # Compatibility boundary for pre-21.3 actions.  Their tests and
                # plugins hook save_action_record directly; the new reliable
                # executors below use the stricter transactional claim API.
                existing = repository.get_action_record_by_idempotency_key(idempotency_key)
                if existing is not None:
                    if existing.status == ActionStatus.SUCCEEDED:
                        return {
                            "action_id": existing.action_id,
                            "actionExecutionId": existing.action_id,
                            "action_type": canonical_action_type,
                            "status": "skipped",
                            "reason": "idempotent_skip",
                            "previous_result": sanitize_public_value(existing.result),
                        }
                    if existing.status in {ActionStatus.RUNNING, ActionStatus.EXECUTING, ActionStatus.UNKNOWN}:
                        return {
                            "action_id": existing.action_id,
                            "actionExecutionId": existing.action_id,
                            "action_type": canonical_action_type,
                            "status": "unknown" if existing.status == ActionStatus.UNKNOWN else "in_flight",
                            "executed": False,
                            "reason": f"existing {existing.status.value} attempt",
                        }
                record.status = ActionStatus.EXECUTING
                record.attempt = max(1, int(existing.attempt or 0) + 1) if existing else 1
                record.started_at = _utc_now_iso()
                repository.save_action_record(record)
                marker = repository.get_action_record_by_idempotency_key(idempotency_key)
                if marker is None or marker.action_id != action_id:
                    return {
                        "action_id": marker.action_id if marker else "",
                        "actionExecutionId": marker.action_id if marker else "",
                        "action_type": canonical_action_type,
                        "status": "in_flight",
                        "executed": False,
                        "reason": "idempotent_db_protect",
                        "error": "existing execution owns marker",
                    }
                record = marker
            else:
                claim = repository.claim_action_execution(record)
                marker = claim.get("record")
                if not claim.get("claimed"):
                    if claim.get("reason") == "run_cancelled":
                        return _cancelled_result("run cancelled before action")
                    if marker is None:
                        return {
                            "action_type": canonical_action_type,
                            "status": "blocked",
                            "executed": False,
                            "reason": str(claim.get("reason") or "claim rejected"),
                        }
                    if marker.status == ActionStatus.SUCCEEDED:
                        return {
                            "action_id": marker.action_id,
                            "actionExecutionId": marker.action_id,
                            "action_type": canonical_action_type,
                            "status": "skipped",
                            "reason": "idempotent_skip",
                            "previous_result": sanitize_public_value(marker.result),
                        }
                    if marker.status in {ActionStatus.RUNNING, ActionStatus.EXECUTING, ActionStatus.UNKNOWN}:
                        return {
                            "action_id": marker.action_id,
                            "actionExecutionId": marker.action_id,
                            "action_type": canonical_action_type,
                            "status": "unknown" if marker.status == ActionStatus.UNKNOWN else "in_flight",
                            "executed": False,
                            "reason": f"existing {marker.status.value} attempt",
                            "externalReference": marker.external_reference or None,
                        }
                    return {
                        "action_id": marker.action_id,
                        "actionExecutionId": marker.action_id,
                        "action_type": canonical_action_type,
                        "status": marker.status.value,
                        "executed": False,
                        "reason": "explicit retry required",
                        "error": "explicit retry required",
                    }
                record = marker
                action_id = record.action_id
                idempotency_key = record.idempotency_key
        except Exception as e:
            state.add_audit_event("dispatch_marker_persist_failed", config.node_id, {
                "actionType": action_type, "reason": str(e)[:200],
            })
            return {
                "action_type": action_type,
                "status": "marker_persist_failed",
                "executed": False,
                "reason": str(e)[:200],
                "error": str(e)[:200],  # 使 executor 判为 node 失败（可安全 retry，无外部 side effect）
            }

    # Re-check cancellation/fencing after the marker and immediately before dispatch.
    _gate = _driver_gate()
    if _gate == "cancelled":
        state.add_audit_event("action_cancelled_before_dispatch", config.node_id, {"actionType": action_type})
        if repository:
            if reliable_executor is None:
                record.status = ActionStatus.FAILED
                record.result = {"cancelled": True, "dispatched": False}
                record.error = "cancelled_before_dispatch"
                record.completed_at = _utc_now_iso()
                record.finished_at = record.completed_at
                repository.save_action_record(record)
            else:
                repository.finalize_action_execution({
                    "actionExecutionId": action_id,
                    "attempt": record.attempt,
                    "status": ActionStatus.CANCELLED.value,
                    "result": {"cancelled": True, "dispatched": False},
                    "error": "cancelled_before_dispatch",
                    "finishedAt": _utc_now_iso(),
                })
        return _cancelled_result("run cancelled before dispatch")
    if _gate == "lease_lost":
        state.add_audit_event("lease_lost", config.node_id, {"actionType": action_type})
        return _lease_lost_result()

    result_data: Dict[str, Any] = {}
    error = ""
    status = ActionStatus.SUCCEEDED
    external_reference = ""
    retryable = False
    reconciliation_supported = bool(record.reconciliation_supported)
    reconciliation_message = ""

    try:
        if reliable_executor is not None:
            context = ActionExecutionContext(
                action_execution_id=action_id,
                workflow_run_id=state.workflow_run_id,
                node_id=config.node_id,
                event_id=event_id,
                action_type=canonical_action_type,
                idempotency_key=idempotency_key,
                attempt=int(record.attempt or 1),
                params=safe_params,
                event=dict(state.current_event or {}),
                risk=dict(risk),
                repository=repository,
            )
            validation_error = reliable_executor.validate(context)
            if validation_error:
                execution_result = ActionExecutorResult(
                    status=ActionStatus.FAILED,
                    error=validation_error,
                    retryable=False,
                    reconciliation_supported=bool(reliable_executor.reconciliation_supported),
                )
            else:
                execution_result = await reliable_executor.execute(context)
            status = execution_result.status
            result_data = execution_result.safe_metadata()
            if execution_result.message:
                result_data["message"] = sanitize_public_text(
                    execution_result.message
                )
            error = sanitize_public_text(execution_result.error)[:500]
            external_reference = sanitize_public_text(
                execution_result.external_reference
            )[:500]
            retryable = bool(execution_result.retryable)
            reconciliation_supported = bool(execution_result.reconciliation_supported)
            reconciliation_message = sanitize_public_text(
                execution_result.reconciliation_message
            )[:500]
        else:
            result_data = await _dispatch_action(action_type, safe_params, state)
    except asyncio.CancelledError:
        status = ActionStatus.UNKNOWN
        error = "execution cancelled after dispatch marker; external outcome unknown"
        reconciliation_message = "reconciliation required before retry"
        raise_after_finalize = True
    except (asyncio.TimeoutError, TimeoutError):
        status = ActionStatus.UNKNOWN
        error = "provider timeout; external outcome unknown"
        reconciliation_message = "reconciliation required before retry"
        raise_after_finalize = False
    except Exception as e:
        error = sanitize_public_text(e)[:500]
        result_data = {}
        if reliable_executor is not None:
            # Once a durable dispatch marker exists, an executor/provider
            # exception cannot prove that the side effect did not happen.
            # Conservatively fence it as UNKNOWN; only reconciliation may
            # later establish FAILED/retryable or SUCCEEDED.
            status = ActionStatus.UNKNOWN
            retryable = False
            reconciliation_supported = bool(
                getattr(reliable_executor, "reconciliation_supported", False)
            )
            reconciliation_message = (
                "executor raised after dispatch marker; reconciliation required"
            )
            state.record_error(
                config.node_id,
                f"action 外部执行结果未知: {error}",
            )
        else:
            status = ActionStatus.FAILED
            state.record_error(config.node_id, f"action 执行失败: {error}")
            retryable = True
        raise_after_finalize = False
    else:
        raise_after_finalize = False

    # ── 工具失败语义（Section 17）：result 明确失败时不得标记 SUCCEEDED ──
    # _dispatch_action 对 notify/save 失败返回 {"sent": False}/{"saved": False}
    # 而非抛异常，因此必须在执行后重新判定，防止失败被记录成成功。
    if reliable_executor is None and status == ActionStatus.SUCCEEDED and classify_tool_result(result_data) == ToolExecutionStatus.FAILURE:
        status = ActionStatus.FAILED
        if isinstance(result_data, dict):
            error = str(result_data.get("error", "工具返回失败结果"))[:500]
        else:
            error = "工具返回失败结果"
        state.record_error(config.node_id, f"action 执行失败: {error}")
        retryable = True

    finalization = {
        "actionExecutionId": action_id,
        "attempt": int(record.attempt or 1),
        "status": status.value,
        "result": sanitize_public_value(result_data),
        "error": error,
        "externalReference": external_reference,
        "finishedAt": _utc_now_iso(),
        "retryable": retryable,
        "reconciliationSupported": reconciliation_supported,
        "reconciliationMessage": reconciliation_message,
    }
    if repository:
        if reliable_executor is None:
            try:
                record.status = status
                record.result = sanitize_public_value(result_data)
                record.error = error
                record.completed_at = finalization["finishedAt"]
                record.finished_at = finalization["finishedAt"]
                record.external_reference = external_reference
                record.retryable = retryable
                record.reconciliation_supported = reconciliation_supported
                record.reconciliation_message = reconciliation_message
                repository.save_action_record(record)
            except Exception as e:
                return {
                    "action_id": action_id,
                    "actionExecutionId": action_id,
                    "action_type": canonical_action_type,
                    "status": "result_persist_failed",
                    "error": f"action result 持久化失败: {str(e)[:200]}",
                }
        elif not defer_terminal:
            try:
                if not repository.finalize_action_execution(finalization):
                    raise RuntimeError("action terminal compare-and-set failed")
            except Exception as e:
                state.add_audit_event("action_result_persist_failed", config.node_id, {
                    "actionType": canonical_action_type, "reason": str(e)[:200],
                })
                return {
                    "action_id": action_id,
                    "actionExecutionId": action_id,
                    "action_type": canonical_action_type,
                    "status": "result_persist_failed",
                    "error": f"action result 持久化失败: {str(e)[:200]}",
                }

    if action_id not in state.action_record_ids:
        state.action_record_ids.append(action_id)
    if isinstance(state.action_results, dict):
        state.action_results[canonical_action_type] = {
            "actionExecutionId": action_id,
            "status": status.value,
            "result": sanitize_public_value(result_data),
            "error": error,
            "externalReference": external_reference or None,
        }

    state.add_audit_event(f"action_{status.value}", config.node_id, {
        "actionType": canonical_action_type,
        "actionExecutionId": action_id,
        "attempt": int(record.attempt or 1),
        "status": status.value,
        "idempotencyKey": idempotency_key,
    })

    response = {
        "action_id": action_id,
        "actionExecutionId": action_id,
        "action_type": canonical_action_type,
        "actionType": canonical_action_type,
        "attempt": int(record.attempt or 1),
        "status": status.value,
        "result": sanitize_public_value(result_data),
        "error": error,
        "externalReference": external_reference or None,
        "reconciliationSupported": reconciliation_supported,
        "retryable": retryable,
    }
    if defer_terminal and repository and reliable_executor is not None:
        response["_actionFinalization"] = finalization
    if raise_after_finalize:
        # asyncio.wait_for cancellation must not erase the UNKNOWN marker.
        if repository and defer_terminal:
            repository.finalize_action_execution(finalization)
            response.pop("_actionFinalization", None)
        raise asyncio.CancelledError
    return response


async def _dispatch_action(
    action_type: str,
    params: Dict[str, Any],
    state: TrafficWorkflowState,
) -> Dict[str, Any]:
    """调度具体的外部动作。

    Args:
        action_type: 动作类型
        params: 动作参数
        state: 工作流状态

    Returns:
        执行结果
    """
    event = state.current_event or {}
    risk = state.risk_assessment or {}

    if action_type == "notify_wechat":
        # 企业微信通知
        try:
            from backend.tools.notify_tools import send_wechat_work
            event_summary = (
                f"## TrafficMind 交通事件通知\n"
                f"事件类型：{event.get('eventTypeCn', '')}\n"
                f"路段：{event.get('roadName', '')}\n"
                f"风险等级：{risk.get('riskLevel', '未知')}（{risk.get('riskScore', 0)}分）\n"
            )
            ok = await asyncio.to_thread(send_wechat_work, event_summary)
            return {"sent": bool(ok), "channel": "wechat"}
        except Exception as e:
            return {"sent": False, "channel": "wechat", "error": str(e)[:200]}

    elif action_type == "notify_dingtalk":
        # 钉钉通知
        try:
            from backend.tools.notify_tools import send_dingtalk
            event_summary = (
                f"## TrafficMind 交通事件通知\n"
                f"事件：{event.get('eventTypeCn', '')} | {event.get('roadName', '')}\n"
                f"风险：{risk.get('riskLevel', '未知')}（{risk.get('riskScore', 0)}分）\n"
            )
            ok = await asyncio.to_thread(send_dingtalk, event_summary)
            return {"sent": bool(ok), "channel": "dingtalk"}
        except Exception as e:
            return {"sent": False, "channel": "dingtalk", "error": str(e)[:200]}

    elif action_type == "save_result":
        # 持久化分析结果
        try:
            from backend.tools.db_tools import save_event_analysis
            # ``save_event_analysis`` consumes the historical analyze-event
            # envelope, not a flat event DTO.  Passing flat fields here used
            # to replace an existing canonical row with empty event data on
            # Workflow completion.  Preserve the authoritative event snapshot
            # and let canonical lifecycle changes happen only through the
            # executor's compare-and-set transition.
            standard_event = dict(event)
            result_data = {
                "eventId": event.get("eventId", f"evt_{state.workflow_run_id}"),
                "standardEvent": standard_event,
                "riskScore": risk.get("riskScore", 0),
                "riskLevel": risk.get("riskLevel", "低风险"),
                "status": event.get("status", "待派单"),
                "report": getattr(state, "report", "") or "",
            }
            ok = save_event_analysis(result_data)
            return {"saved": bool(ok), "eventId": result_data.get("eventId", "")}
        except Exception as e:
            return {"saved": False, "error": str(e)[:200]}

    # ── Phase 13: Simulation Actions ──────────────────────────────────
    # 所有 simulation action 必须标记 simulation=true
    # Agent 不得直接调用，必须经过 Workflow Risk Gate → Human Approval

    elif action_type == "simulation_traffic_diversion":
        return await _execute_simulation_action(action_type, params, state,
            "分流动作：将指定道路流量分流到目标道路")

    elif action_type == "simulation_signal_adjustment":
        return await _execute_simulation_action(action_type, params, state,
            "信号调整：调整指定路口信号配时")

    elif action_type == "simulation_lane_control":
        return await _execute_simulation_action(action_type, params, state,
            "车道控制：调整指定路段车道使用")

    elif action_type == "simulation_dispatch_coordination":
        return await _execute_simulation_action(action_type, params, state,
            "调度协调：发送模拟调度指令")

    elif action_type == "simulation_monitor":
        return await _execute_simulation_action(action_type, params, state,
            "监控：检查交通状态改善情况")

    elif action_type == "simulation_close":
        return await _execute_simulation_action(action_type, params, state,
            "关闭：标记事件已处置完成")

    else:
        # 通用动作：记录日志
        return {
            "action_type": action_type,
            "params": params,
            "status": "executed",
            "note": "通用动作已记录",
        }


async def _execute_simulation_action(
    action_type: str,
    params: Dict[str, Any],
    state: TrafficWorkflowState,
    description: str,
) -> Dict[str, Any]:
    """执行模拟交通动作（Phase 13 Bridge）。

    约束：
      - simulation ALWAYS True
      - 必须通过 Workflow Risk Gate + Human Approval 后才能调用
      - 调用 DemoSimulationProvider.apply_action()
      - 记录 before/after snapshot 对比
    """
    from backend.simulation.demo_provider import get_demo_provider
    from backend.simulation.models import (
        TrafficSimulationAction as SimAction,
        ActionType,
        generate_action_id,
    )

    sim_refs = state.simulation_refs or {}
    simulation_run_id = sim_refs.get("simulationRunId", "") or sim_refs.get(
        "simulation_run_id", ""
    )
    decision_snapshot_id = sim_refs.get("decisionSnapshotId", "") or sim_refs.get(
        "decision_snapshot_id", ""
    )
    if not simulation_run_id:
        return {
            "error": "simulation_refs 缺少 simulationRunId，无法执行模拟动作",
            "simulation": True,
        }

    # 映射 workflow action_type → simulation ActionType
    action_type_map = {
        "simulation_traffic_diversion": ActionType.TRAFFIC_DIVERSION,
        "simulation_signal_adjustment": ActionType.SIGNAL_ADJUSTMENT,
        "simulation_lane_control": ActionType.LANE_CONTROL,
        "simulation_dispatch_coordination": ActionType.DISPATCH_COORDINATION,
        "simulation_monitor": ActionType.MONITOR,
        "simulation_close": ActionType.CLOSE,
    }
    sim_action_type = action_type_map.get(action_type, ActionType.MONITOR)

    provider = get_demo_provider()

    # 构建模拟动作
    sim_action = SimAction(
        action_id=generate_action_id(),
        action_type=sim_action_type,
        target_ids=params.get("targetIds", params.get("target_ids", [])),
        parameters=params.get("parameters", params.get("params", {})),
        source="workflow",
        workflow_run_id=state.workflow_run_id,
        simulation=True,
    )

    try:
        # 获取 before snapshot
        before_snap = provider.get_snapshot(simulation_run_id)
        sim_action.before_snapshot_id = before_snap.snapshot_id

        # 执行动作
        new_snap = provider.apply_action(simulation_run_id, sim_action)

        # 构建改善指标
        affected_roads = sim_action.target_ids
        improvements = {}
        for rid in affected_roads:
            before_rs = before_snap.road_states.get(rid)
            after_rs = new_snap.road_states.get(rid)
            if before_rs and after_rs:
                improvements[rid] = {
                    "speedBefore": before_rs.avg_speed,
                    "speedAfter": after_rs.avg_speed,
                    "speedDelta": round(after_rs.avg_speed - before_rs.avg_speed, 1),
                    "queueBefore": before_rs.queue_length,
                    "queueAfter": after_rs.queue_length,
                    "queueDelta": round(after_rs.queue_length - before_rs.queue_length, 0),
                    "congestionBefore": before_rs.congestion_level.value,
                    "congestionAfter": after_rs.congestion_level.value,
                }

        # 更新 simulation_refs: latestSnapshotId → after
        state.simulation_refs["latestSnapshotId"] = new_snap.snapshot_id

        return {
            "action_id": sim_action.action_id,
            "action_type": action_type,
            "simulation": True,
            "status": "succeeded",
            "description": description,
            "decisionSnapshotId": decision_snapshot_id,
            "beforeSnapshotId": before_snap.snapshot_id,
            "afterSnapshotId": new_snap.snapshot_id,
            "improvements": improvements,
        }

    except Exception as e:
        return {
            "action_id": sim_action.action_id,
            "action_type": action_type,
            "simulation": True,
            "status": "failed",
            "error": str(e)[:500],
            "description": description,
            "decisionSnapshotId": decision_snapshot_id,
        }
