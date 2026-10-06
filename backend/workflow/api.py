"""
Workflow V1 API — Phase 12 + Workflow Center V2 Round 1

FastAPI Router，提供 Workflow 相关的 REST + SSE 接口。

端点：
  GET    /workflow/definitions                        — 列出所有 Definition
  GET    /workflow/definitions/{definitionId}          — 获取单个 Definition
  GET    /workflow/runs                               — 列出 Run 历史（Workflow Center V2）
  POST   /workflow/runs                               — 创建并启动 Run（SSE 流式）
  GET    /workflow/runs/{runId}                        — 查询 Run 详情
  GET    /workflow/runs/{runId}/trace                  — 查询 Run Trace
  POST   /workflow/runs/{runId}/resume                 — 恢复暂停的 Run（SSE 流式）
  POST   /workflow/runs/{runId}/cancel                 — 取消 Run
  POST   /workflow/runs/{runId}/retry                  — 重试失败节点
  POST   /workflow/runs/{runId}/approvals/{approvalId} — 处理审批
  GET    /workflow/runs/{runId}/stream                 — SSE 状态流
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from backend.agent.streaming import sse_event, sse_error
from backend.workflow.models import (
    ApprovalDecision,
    ApprovalReasonCode,
    DefinitionStatus,
    WorkflowApproval,
    WorkflowRun,
    WorkflowRunStatus,
)
from backend.workflow.state import TrafficWorkflowState
from backend.workflow.definition import DefinitionManager
from backend.workflow.repository import SQLiteWorkflowRepository, init_workflow_tables
from backend.workflow.executor import definition_allows_action_execution, get_executor
from backend.workflow.action_execution import (
    contains_sensitive_key,
    reconcile_action_execution,
    request_action_execution_retry,
    sanitize_public_text,
    sanitize_public_value,
)

# ═══════════════════════════════════════════════════════════════════════════════
# Router
# ═══════════════════════════════════════════════════════════════════════════════

router = APIRouter(prefix="/workflow", tags=["Workflow V1"])

# 确保表已初始化
init_workflow_tables()

# 全局 Repository 和 Manager
_repo = SQLiteWorkflowRepository()
_def_manager = DefinitionManager(_repo)


# ═══════════════════════════════════════════════════════════════════════════════
# 请求模型
# ═══════════════════════════════════════════════════════════════════════════════


class StartRunRequest(BaseModel):
    """启动 Workflow Run 请求。"""
    definitionId: str
    sessionId: Optional[str] = ""
    eventThreadId: Optional[str] = ""
    event: Dict[str, Any] = {}
    triggeredBy: Optional[str] = "api"

    model_config = ConfigDict(extra="forbid")


class ResumeRunRequest(BaseModel):
    """恢复 Workflow Run 请求。"""
    pass  # 当前不需要额外参数


class CancelRunRequest(BaseModel):
    """取消 Workflow Run 请求。"""
    reason: Optional[str] = ""

    model_config = ConfigDict(extra="forbid")


class RetryNodeRequest(BaseModel):
    """重试失败节点请求。"""
    nodeId: str

    model_config = ConfigDict(extra="forbid")


class ApprovalRequest(BaseModel):
    """审批请求。"""
    action: str  # "approve", "reject", "edit_and_approve"
    reviewer: Optional[str] = Field(default="", max_length=200)
    comment: Optional[str] = Field(default="", max_length=1000)
    reasonCode: Optional[ApprovalReasonCode] = ApprovalReasonCode.NONE
    editedActions: Optional[List[Dict[str, Any]]] = None

    model_config = ConfigDict(extra="forbid")


# ═══════════════════════════════════════════════════════════════════════════════
# Workflow Center V2 Round 1 — RunSummary DTO Helpers
# ═══════════════════════════════════════════════════════════════════════════════


def _extract_event_summary(state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """从 state_json 中提取事件摘要信息。

    优先从 currentEvent 读取，回退到 originalInput。
    不存在的字段为 None，不编造数据。
    """
    event = state.get("currentEvent") or state.get("originalInput") or {} if isinstance(state, dict) else {}
    if not event or not isinstance(event, dict):
        return None

    road_name = event.get("roadName") or None
    event_type = event.get("eventType") or None
    event_type_cn = event.get("eventTypeCn") or None
    description = event.get("description") or None

    # 全部为 None 则返回 None
    if not any([road_name, event_type, event_type_cn, description]):
        return None

    return {
        "roadName": road_name,
        "eventType": event_type,
        "eventTypeCn": event_type_cn,
        "description": description,
    }


def _derive_approval_status(
    run_status: str,
    state: Dict[str, Any],
    approval_decisions: List[str],
) -> str:
    """从 Run 状态、state、审批记录综合推导审批状态。

    优先级：
      1. run status = awaiting_approval → "awaiting_approval"
      2. run status = rejected → "rejected"
      3. state.approvedActions 非空 → "approved"
      4. approval_decisions 中有 approved/edited → "approved"
      5. approval_decisions 中有 rejected → "rejected"
      6. state 中无 human_approval 相关数据 → "not_required"

    不单独依赖 pendingApproval（完成后为 null）。
    """
    if run_status == "awaiting_approval":
        return "awaiting_approval"

    if run_status == "rejected":
        return "rejected"

    # 检查 state 中的 approvedActions（持久化，完成后仍存在）
    approved_actions = state.get("approvedActions") or state.get("approved_actions") or []
    if isinstance(approved_actions, list) and len(approved_actions) > 0:
        return "approved"

    # 检查审批表中的决策
    if approval_decisions:
        if any(d in ("approved", "edited") for d in approval_decisions):
            return "approved"
        if any(d == "rejected" for d in approval_decisions):
            return "rejected"
        # pending 但 run 已终止 → 可能异常，仍按 pending 返回
        if any(d == "pending" for d in approval_decisions):
            return "awaiting_approval"

    # 检查 state 中是否有审批相关数据
    pending_approval = state.get("pendingApproval") or state.get("pending_approval")
    if pending_approval and isinstance(pending_approval, dict):
        return "awaiting_approval"

    return "not_required"


def _build_run_summary(
    run: "WorkflowRun",
    definition_name: Optional[str],
    definition_node_count: Optional[int],
    node_counts: Optional[Dict[str, int]],
    action_counts: Optional[Dict[str, int]],
    approval_decisions: Optional[List[str]],
) -> Dict[str, Any]:
    """构建单个 RunSummary DTO。

    所有字段基于真实现有数据，不编造不存在的字段。

    Args:
        run: WorkflowRun 对象
        definition_name: 模板名称（可能为 None）
        definition_node_count: 模板定义的节点总数（可能为 None）
        node_counts: workflow_node_runs 聚合统计
        action_counts: workflow_action_records 聚合统计
        approval_decisions: workflow_approvals 决策列表
    """
    state = run.state if isinstance(run.state, dict) else {}

    # ── 基础字段 ──
    summary: Dict[str, Any] = {
        "runId": run.run_id,
        "definitionId": run.definition_id,
        "definitionName": definition_name,
        "status": run.status.value,
        "version": run.version,
        "sessionId": run.session_id,
        "eventThreadId": run.event_thread_id,
        "currentNodeId": run.current_node_id,
        "triggeredBy": run.triggered_by,
        "startedAt": run.started_at or None,
        "updatedAt": run.updated_at or None,
        "completedAt": run.completed_at or None,
        "isTerminal": run.is_terminal(),
    }

    # ── 事件摘要 ──
    summary["eventSummary"] = _extract_event_summary(state)

    # ── 节点进度 ──
    # totalNodes     = Definition 定义的节点总数（来自 nodes_json 解析）
    # executedNodes  = workflow_node_runs 中已有记录的节点数
    # succeededNodes = 成功的节点数
    # failedNodes    = 失败的节点数（含 timed_out）
    progress: Dict[str, Any] = {
        "totalNodes": definition_node_count,     # None if definition missing
        "executedNodes": 0,
        "succeededNodes": 0,
        "failedNodes": 0,
        "currentNode": run.current_node_id or None,
    }
    if node_counts:
        progress["executedNodes"] = node_counts.get("total", 0)
        progress["succeededNodes"] = node_counts.get("succeeded", 0)
        progress["failedNodes"] = node_counts.get("failed", 0)
    summary["progress"] = progress

    # ── 审批摘要 ──
    summary["approvalSummary"] = {
        "status": _derive_approval_status(
            run.status.value, state, approval_decisions or []
        ),
    }

    # ── 动作摘要 ──
    act: Dict[str, Any] = {
        "total": 0,
        "succeeded": 0,
        "failed": 0,
        "unknown": 0,
    }
    if action_counts:
        act["total"] = action_counts.get("total", 0)
        act["succeeded"] = action_counts.get("succeeded", 0)
        act["failed"] = action_counts.get("failed", 0)
        act["unknown"] = action_counts.get("unknown", 0)
    summary["actionSummary"] = act

    return summary


# ═══════════════════════════════════════════════════════════════════════════════
# 端点
# ═══════════════════════════════════════════════════════════════════════════════


@router.get("/definitions", summary="列出 Workflow 定义")
async def list_definitions(
    status: Optional[str] = Query(None, description="按状态筛选: draft/active/deprecated"),
):
    """列出所有 WorkflowDefinition。可按状态筛选。"""
    definitions = _repo.list_definitions(status=status)
    return {
        "total": len(definitions),
        "definitions": [d.to_dict() for d in definitions],
    }


@router.get("/definitions/{definition_id}", summary="获取 Workflow 定义详情")
async def get_definition(definition_id: str):
    """获取单个 WorkflowDefinition 详情，包含所有版本。"""
    definition = _repo.get_definition(definition_id)
    if definition is None:
        raise HTTPException(status_code=404, detail=f"Definition '{definition_id}' 不存在")

    versions = _repo.list_definition_versions(definition_id)
    return {
        "definition": definition.to_dict(),
        "versions": [v.to_dict() for v in versions],
        "versionCount": len(versions),
    }


@router.get("/runs", summary="列出 Workflow Run 历史（Workflow Center V2）")
async def list_runs(
    status: Optional[str] = Query(None, description="按状态筛选: pending/running/paused/awaiting_approval/completed/failed/cancelled/rejected"),
    definition_id: Optional[str] = Query(None, description="按 Definition ID 筛选"),
    session_id: Optional[str] = Query(None, description="按 Session ID 筛选"),
    event_id: Optional[str] = Query(None, description="按事件 ID 精确匹配（state_json $.currentEvent.eventId，只读）"),
    limit: int = Query(50, ge=1, le=200, description="每页条数（1-200）"),
    offset: int = Query(0, ge=0, description="偏移量"),
):
    """列出 Workflow Run 历史记录（只读）。

    支持按状态、Definition、Session、事件 ID 筛选；支持分页（limit/offset）。
    event_id 为 Phase20 R2 薄只读扩展：对 state_json 的
    $.currentEvent.eventId 做精确匹配（JSON1），不写库、不改 schema、
    不触发任何 Agent；仅启动方在初始事件中携带了 eventId 的 Run 会被命中。
    排序：updated_at DESC, run_id DESC（稳定排序）。

    返回 RunSummary DTO，包含：
      - 基础字段（runId, definitionId, definitionName, status, ...）
      - eventSummary（roadName, eventType, eventTypeCn, description）
      - progress（totalNodes, succeededNodes, failedNodes, currentNode）
      - approvalSummary（status: not_required/awaiting_approval/approved/rejected）
      - actionSummary（total, succeeded, failed）
    """
    # ── 验证 status 参数 ──
    if status is not None:
        try:
            WorkflowRunStatus(status)
        except ValueError:
            valid = [s.value for s in WorkflowRunStatus]
            raise HTTPException(
                status_code=400,
                detail=f"无效的状态值 '{status}'。有效值: {valid}",
            )

    # ── 查询 Run 列表 + 总数 ──
    runs = _repo.list_runs(
        session_id=session_id or "",
        definition_id=definition_id or "",
        status=status,
        event_id=event_id or "",
        limit=limit,
        offset=offset,
    )
    total = _repo.count_runs(
        session_id=session_id or "",
        definition_id=definition_id or "",
        status=status,
        event_id=event_id or "",
    )

    # ── 批量加载关联数据（避免 N+1）──
    run_ids = [r.run_id for r in runs]
    definition_ids = list({r.definition_id for r in runs if r.definition_id})

    # 批量: Definition Summaries（name + nodeCount）
    def_summaries = _repo.batch_get_definition_summaries(definition_ids)

    # 批量: Node 统计
    node_counts = _repo.batch_get_node_counts(run_ids)

    # 批量: Action 统计
    action_counts = _repo.batch_get_action_counts(run_ids)

    # 批量: Approval 决策
    approval_decisions_map = _repo.batch_get_approval_decisions(run_ids)

    # ── 构建 DTO ──
    items = []
    for run in runs:
        def_summ = def_summaries.get(run.definition_id)
        summary = _build_run_summary(
            run=run,
            definition_name=def_summ["name"] if def_summ else None,
            definition_node_count=def_summ["nodeCount"] if def_summ else None,
            node_counts=node_counts.get(run.run_id),
            action_counts=action_counts.get(run.run_id),
            approval_decisions=approval_decisions_map.get(run.run_id),
        )
        items.append(summary)

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "runs": items,
    }


@router.post("/runs", summary="创建并启动 Workflow Run（SSE 流式）")
async def start_run(body: StartRunRequest):
    """创建 Workflow Run 并以 SSE 流式执行。

    返回 SSE 事件流：
      workflow_started → node_started* → node_completed* →
      [approval_required] → [workflow_paused] →
      workflow_completed → done
    """
    executor = get_executor()

    if contains_sensitive_key(body.event):
        raise HTTPException(
            status_code=422,
            detail={
                "code": "credential_in_payload",
                "message": "Workflow event 不允许包含 credential/secret 字段",
            },
        )

    # 获取 definition 名称（用于 SSE）
    definition = _repo.get_definition(body.definitionId)
    if definition is None:
        raise HTTPException(status_code=404, detail=f"Definition '{body.definitionId}' 不存在")
    if not definition_allows_action_execution(definition):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "replay_execution_blocked",
                "message": "该 Workflow 来自安全 replay run，默认禁止启动 Action 执行",
            },
        )

    async def _stream():
        try:
            async for sse_str in executor.start(
                definition_id=body.definitionId,
                session_id=body.sessionId or "",
                event_thread_id=body.eventThreadId or "",
                initial_event=body.event,
                triggered_by=body.triggeredBy or "api",
            ):
                yield sse_str
        except Exception as e:
            import traceback
            traceback.print_exc()
            yield sse_event("error", {
                "message": str(e).split("\n")[0][:200],
                "details": "An internal error occurred during workflow execution."
            })
            yield sse_event("done", {"error": True})

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _decision_provenance_or_empty(run: WorkflowRun) -> List[Dict[str, Any]]:
    """Phase19 R4：decision provenance 只读投影（0 provider / 0 写；异常 → []）。

    Phase20 Workflow Detail 可直接消费，无需再解析 state_json。
    """
    try:
        from backend.planning.decision_provenance import build_decision_provenance
        return build_decision_provenance(run, _repo)
    except Exception:
        return []


def _runtime_projection(
    run: WorkflowRun,
    state: Dict[str, Any],
    node_runs: List[Any],
    approvals: List[WorkflowApproval],
    action_records: Optional[List[Any]] = None,
) -> Dict[str, Any]:
    """Project persisted runtime truth and valid user operations."""
    current_event = state.get("currentEvent") if isinstance(state, dict) else {}
    if not isinstance(current_event, dict):
        current_event = {}

    definition = _repo.get_definition(run.definition_id) if run.definition_id else None
    metadata = definition.metadata if definition and isinstance(definition.metadata, dict) else {}
    is_plan = bool(metadata.get("plan") or metadata.get("planFingerprint"))

    completed_steps = state.get("completedSteps") or []
    if not isinstance(completed_steps, list):
        completed_steps = []
    if not completed_steps:
        for node_run in node_runs:
            if node_run.status.value == "succeeded" and node_run.node_id not in completed_steps:
                completed_steps.append(node_run.node_id)

    failures = [
        node_run for node_run in node_runs
        if node_run.status.value in ("failed", "timed_out")
    ]
    failures.sort(key=lambda item: (item.started_at, item.attempt, item.node_run_id))
    latest_failed = failures[-1] if failures else None
    state_errors = state.get("errors") or []
    state_failure = state_errors[-1] if state_errors and isinstance(state_errors[-1], dict) else {}
    failure = None
    if run.status == WorkflowRunStatus.FAILED:
        failure = {
            "nodeId": (
                latest_failed.node_id if latest_failed is not None
                else state_failure.get("nodeId") or run.current_node_id
            ),
            "message": (
                latest_failed.error if latest_failed is not None and latest_failed.error
                else state_failure.get("error") or "Workflow 执行失败"
            ),
            "attempt": (
                latest_failed.attempt if latest_failed is not None
                else int(state_failure.get("attempt", 0) or 0)
            ),
            "timestamp": state_failure.get("timestamp") or run.updated_at or None,
        }

    pending = state.get("pendingApproval")
    approval_waiting = None
    if run.status == WorkflowRunStatus.AWAITING_APPROVAL and isinstance(pending, dict):
        approval_waiting = {
            "approvalId": pending.get("approvalId"),
            "nodeId": pending.get("nodeId"),
            "createdAt": pending.get("createdAt"),
            "proposedActions": sanitize_public_value(
                pending.get("proposedActions") or []
            ),
        }

    can_retry = run.status == WorkflowRunStatus.FAILED and latest_failed is not None
    if can_retry:
        failed_node_actions = [
            action for action in (action_records or [])
            if action.node_id == latest_failed.node_id
        ]
        failed_node_actions.sort(
            key=lambda action: (
                int(action.attempt or 0),
                action.created_at,
                action.action_id,
            )
        )
        if failed_node_actions:
            latest_action = failed_node_actions[-1]
            can_retry = bool(
                latest_action.status.value == "failed"
                and latest_action.retryable
            )
    unknown_actions = [
        action for action in (action_records or [])
        if action.status.value == "unknown"
    ]
    can_resume = (
        (run.status == WorkflowRunStatus.PAUSED and not unknown_actions)
        or (
            run.status == WorkflowRunStatus.AWAITING_APPROVAL
            and not isinstance(pending, dict)
        )
    )
    can_cancel = run.status in {
        WorkflowRunStatus.PENDING,
        WorkflowRunStatus.RUNNING,
        WorkflowRunStatus.PAUSED,
        WorkflowRunStatus.AWAITING_APPROVAL,
    }

    return {
        "runtime": {
            "eventId": current_event.get("eventId") or None,
            "planId": run.definition_id if is_plan else None,
            "currentStep": run.current_node_id or state.get("currentNode") or None,
            "completedSteps": completed_steps,
            "retryCount": int(state.get("retryCount", 0) or 0),
            "failure": failure,
            "approvalWaiting": approval_waiting,
            "actionWaiting": (
                {
                    "actionExecutionId": unknown_actions[-1].action_id,
                    "nodeId": unknown_actions[-1].node_id,
                    "actionType": unknown_actions[-1].action_type,
                    "status": "unknown",
                    "message": "Action 执行结果待确认；请先 reconciliation",
                }
                if unknown_actions else None
            ),
            "cancelledAt": state.get("cancelledAt") or None,
            "cancelReason": state.get("cancelReason") or None,
            "startedAt": run.started_at or state.get("startedAt") or None,
            "updatedAt": run.updated_at or state.get("updatedAt") or None,
            "finishedAt": run.completed_at or state.get("finishedAt") or None,
        },
        "operations": {
            "canRetry": can_retry,
            "canResume": can_resume,
            "canCancel": can_cancel,
            "retryNodeId": latest_failed.node_id if can_retry else None,
        },
        "approvals": [_approval_public_projection(approval) for approval in approvals],
    }


def _approval_public_projection(approval: WorkflowApproval) -> Dict[str, Any]:
    """Strip credential-shaped data from current and legacy approval rows."""
    value = sanitize_public_value(approval.to_dict())
    return value if isinstance(value, dict) else {}


def _state_public_projection(state: Dict[str, Any]) -> Dict[str, Any]:
    """Sanitize the state subtrees that can carry untrusted action data."""
    public = dict(state or {})
    for key in (
        "pendingApproval",
        "pending_approval",
        "proposedActions",
        "proposed_actions",
        "approvedActions",
        "approved_actions",
        "actionResults",
        "action_results",
        "nodeOutputs",
        "node_outputs",
        "auditTrail",
        "audit_trail",
    ):
        if key in public:
            public[key] = sanitize_public_value(public[key])
    return public


def _raise_mutation_error(result: Dict[str, Any]) -> None:
    if "error" not in result:
        return
    code = str(result.get("errorCode") or "invalid_state")
    status_code = 404 if code == "not_found" else 409
    if code in {"invalid_parameters", "invalid_request"}:
        status_code = 422
    raise HTTPException(
        status_code=status_code,
        detail={"code": code, "message": str(result["error"])},
    )


def _action_public_projection(action: Any, run: WorkflowRun) -> Dict[str, Any]:
    """Explicit allow-list DTO: no params, credentials, or raw provider body."""
    attempts = _repo.list_action_attempts(action.action_id)
    public_result = sanitize_public_value(action.result or {})
    can_retry = bool(
        action.status.value == "failed"
        and action.retryable
        and run.status in {WorkflowRunStatus.FAILED, WorkflowRunStatus.PAUSED}
    )
    can_reconcile = bool(
        action.status.value == "unknown"
        and action.reconciliation_supported
        and run.status in {
            WorkflowRunStatus.PAUSED,
            WorkflowRunStatus.RUNNING,
            WorkflowRunStatus.FAILED,
        }
    )
    return {
        "actionExecutionId": action.action_id,
        "actionId": action.action_id,
        "workflowRunId": action.run_id,
        "nodeId": action.node_id,
        "eventId": action.event_id or None,
        "actionType": sanitize_public_text(action.action_type),
        "idempotencyKey": action.idempotency_key,
        "semanticActionVersion": action.semantic_action_version,
        "attempt": int(action.attempt or 0),
        "status": action.status.value,
        "startedAt": (
            action.started_at
            or (
                action.created_at
                if action.status.value != "pending"
                else None
            )
        ),
        "finishedAt": action.finished_at or action.completed_at or None,
        "externalReference": sanitize_public_text(action.external_reference) or None,
        "message": (
            str(public_result.get("message"))
            if isinstance(public_result, dict) and public_result.get("message")
            else None
        ),
        "result": public_result,
        "error": sanitize_public_text(action.error) or None,
        "lastReconciledAt": action.last_reconciled_at or None,
        "unknownSince": action.unknown_since or None,
        "reconciliationAttempts": int(action.reconciliation_attempts or 0),
        "reconciliationSupported": bool(action.reconciliation_supported),
        "reconciliationMessage": sanitize_public_text(
            action.reconciliation_message
        ) or None,
        "retryable": bool(action.retryable),
        "operations": {
            "canRetry": can_retry,
            "canReconcile": can_reconcile,
        },
        "attempts": [
            {
                "attemptId": attempt.attempt_id,
                "attempt": attempt.attempt,
                "status": attempt.status.value,
                "startedAt": attempt.started_at or None,
                "finishedAt": attempt.finished_at or None,
                "externalReference": sanitize_public_text(
                    attempt.external_reference
                ) or None,
                "result": sanitize_public_value(attempt.result or {}),
                "error": sanitize_public_text(attempt.error) or None,
                "lastReconciledAt": attempt.last_reconciled_at or None,
            }
            for attempt in attempts
        ],
    }


@router.get("/runs/{run_id}", summary="查询 Workflow Run 详情")
async def get_run(run_id: str):
    """查询单个 Workflow Run 的完整详情，包含状态、节点执行记录和 Trace。"""
    run = _repo.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' 不存在")

    node_runs = _repo.get_node_runs(run_id)
    events = _repo.list_events(run_id)
    action_records = _repo.list_action_records(run_id)
    approvals = _repo.list_approvals(run_id)

    # 解析 state
    state = run.state
    if isinstance(state, str):
        try:
            state = json.loads(state)
        except Exception:
            state = {}

    projection = _runtime_projection(run, state, node_runs, approvals, action_records)
    public_state = _state_public_projection(state)
    public_run = run.to_dict()
    public_run["state"] = public_state
    return {
        "run": public_run,
        "state": public_state,
        "nodeRuns": [nr.to_dict() for nr in node_runs],
        "events": [sanitize_public_value(e.to_dict()) for e in events],
        "actionRecords": [_action_public_projection(a, run) for a in action_records],
        "nodeCount": len(node_runs),
        "eventCount": len(events),
        "decisionProvenance": _decision_provenance_or_empty(run),
        **projection,
    }


@router.get("/runs/{run_id}/trace", summary="查询 Workflow Run Trace")
async def get_run_trace(run_id: str):
    """查询 Workflow Run 的完整 Trace：节点、事件、审批、动作记录。"""
    run = _repo.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' 不存在")

    node_runs = _repo.get_node_runs(run_id)
    events = _repo.list_events(run_id)
    action_records = _repo.list_action_records(run_id)
    approvals = _repo.list_approvals(run_id)
    state = run.state if isinstance(run.state, dict) else {}

    # 构建时间线
    timeline = []
    for e in sorted(events, key=lambda x: x.sequence):
        timeline.append({
            "sequence": e.sequence,
            "eventType": e.event_type,
            "nodeId": e.node_id,
            "payload": sanitize_public_value(e.payload),
            "createdAt": e.created_at,
        })

    return {
        "runId": run_id,
        "definitionId": run.definition_id,
        "version": run.version,
        "status": run.status.value,
        "currentNodeId": run.current_node_id,
        "timeline": timeline,
        "nodeRuns": [
            {
                "nodeId": nr.node_id,
                "nodeType": nr.node_type.value,
                "status": nr.status.value,
                "attempt": nr.attempt,
                "error": nr.error,
                "startedAt": nr.started_at,
                "completedAt": nr.completed_at,
            }
            for nr in node_runs
        ],
        "actionRecords": [_action_public_projection(a, run) for a in action_records],
        "ragTraceIds": list(state.get("ragTraceIds") or []),
        "agentRunIds": list(state.get("agentRunIds") or []),
        "approvalIds": [approval.approval_id for approval in approvals],
        "actionRecordIds": [a.action_id for a in action_records],
        "approvals": [_approval_public_projection(approval) for approval in approvals],
    }


@router.get(
    "/runs/{run_id}/actions/{action_execution_id}",
    summary="查询 Action Execution 详情",
)
async def get_action_execution(run_id: str, action_execution_id: str):
    run = _repo.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Workflow Run 不存在")
    action = _repo.get_action_record(action_execution_id)
    if action is None or action.run_id != run_id:
        raise HTTPException(status_code=404, detail="Action Execution 不存在")
    return _action_public_projection(action, run)


@router.post(
    "/runs/{run_id}/actions/{action_execution_id}/retry",
    summary="重试已确认失败的 Action",
)
async def retry_action_execution(run_id: str, action_execution_id: str):
    result = request_action_execution_retry(_repo, run_id, action_execution_id)
    _raise_mutation_error(result)
    return result


@router.post(
    "/runs/{run_id}/actions/{action_execution_id}/reconcile",
    summary="对账 UNKNOWN Action",
)
async def reconcile_action(run_id: str, action_execution_id: str):
    result = await reconcile_action_execution(_repo, run_id, action_execution_id)
    _raise_mutation_error(result)
    return result


@router.post("/runs/{run_id}/resume", summary="恢复 Workflow Run（SSE 流式）")
async def resume_run(run_id: str, body: ResumeRunRequest = None):
    """恢复暂停的 Workflow Run 执行（SSE 流式）。

    仅在 Run 状态为 paused 或 awaiting_approval 时可恢复。
    """
    run = _repo.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' 不存在")
    state = run.state if isinstance(run.state, dict) else {}
    pending = state.get("pendingApproval")
    if run.status not in {
        WorkflowRunStatus.PAUSED,
        WorkflowRunStatus.AWAITING_APPROVAL,
    }:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "invalid_status",
                "message": f"Run '{run_id}' 状态为 {run.status.value}，无法恢复",
            },
        )
    if any(
        action.status.value == "unknown"
        for action in _repo.list_action_records(run_id)
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "reconcile_required",
                "message": "Run 存在 UNKNOWN Action，必须先 reconciliation，不能直接 resume",
            },
        )
    if run.status == WorkflowRunStatus.AWAITING_APPROVAL and isinstance(pending, dict):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "approval_pending",
                "message": "Run 仍有待处理审批，审批完成前不能 resume",
            },
        )

    executor = get_executor()

    async def _stream():
        try:
            async for sse_str in executor.resume(run_id=run_id):
                yield sse_str
        except Exception as e:
            import traceback
            traceback.print_exc()
            yield sse_event("error", {
                "message": str(e).split("\n")[0][:200],
            })
            yield sse_event("done", {"error": True})

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/runs/{run_id}/cancel", summary="取消 Workflow Run")
async def cancel_run(run_id: str, body: CancelRunRequest = None):
    """取消正在执行的 Workflow Run。"""
    executor = get_executor()
    result = await executor.cancel(run_id, reason=(body.reason if body else "") or "")
    _raise_mutation_error(result)
    return result


@router.post("/runs/{run_id}/retry", summary="重试失败节点")
async def retry_node(run_id: str, body: RetryNodeRequest):
    """重试 Workflow Run 中失败的节点。"""
    executor = get_executor()
    result = await executor.retry_node(run_id, body.nodeId)
    _raise_mutation_error(result)
    return result


@router.post("/runs/{run_id}/approvals/{approval_id}", summary="处理审批")
async def process_approval(run_id: str, approval_id: str, body: ApprovalRequest):
    """处理人工审批。

    action 可选值:
      - approve: 批准，继续执行后续节点
      - reject: 驳回，Workflow 终止
      - edit_and_approve: 编辑后批准
    """
    executor = get_executor()

    action = body.action
    reviewer = sanitize_public_text(body.reviewer or "")[:200]
    comment = sanitize_public_text(body.comment or "")[:1000]
    reason_code = (body.reasonCode or ApprovalReasonCode.NONE).value
    if action == "edit_and_approve" and contains_sensitive_key(
        body.editedActions or []
    ):
        raise HTTPException(
            status_code=422,
            detail={
                "code": "invalid_parameters",
                "message": "审批编辑内容包含禁止持久化的 credential/secret 字段",
            },
        )
    if action == "approve":
        result = await executor.approve(
            run_id,
            reviewer=reviewer,
            comment=comment,
            approval_id=approval_id,
        )
    elif action == "reject":
        result = await executor.reject(
            run_id,
            reviewer=reviewer,
            comment=comment,
            reason_code=reason_code,
            approval_id=approval_id,
        )
    elif action == "edit_and_approve":
        result = await executor.edit_and_approve(
            run_id,
            edited_actions=body.editedActions or [],
            reviewer=reviewer,
            comment=comment,
            approval_id=approval_id,
        )
    else:
        raise HTTPException(
            status_code=400,
            detail=f"无效的审批动作 '{action}'。有效值: approve, reject, edit_and_approve",
        )

    _raise_mutation_error(result)
    return result


@router.get("/runs/{run_id}/stream", summary="Workflow Run SSE 状态流")
@router.post("/runs/{run_id}/stream", summary="Workflow Run SSE 状态流 (POST)")
async def get_run_stream(run_id: str):
    """获取 Workflow Run 的当前状态 SSE 流。

    用于前端获取 Run 的实时状态更新。
    如果 Run 已完成，返回一个 done 事件后关闭。
    """
    run = _repo.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' 不存在")

    async def _stream():
        # 发送当前状态
        yield sse_event("run_status", {
            "runId": run_id,
            "status": run.status.value,
            "currentNodeId": run.current_node_id,
            "definitionId": run.definition_id,
            "version": run.version,
        })

        # 发送节点执行记录
        node_runs = _repo.get_node_runs(run_id)
        for nr in node_runs:
            yield sse_event("node_status", nr.to_dict())

        # 发送 action 记录
        action_records = _repo.list_action_records(run_id)
        for ar in action_records:
            yield sse_event("action_status", _action_public_projection(ar, run))

        yield sse_event("done", {"runId": run_id, "status": run.status.value})

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
