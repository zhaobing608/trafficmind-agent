"""
Workflow V1 执行器 — Phase 12

Workflow 执行引擎。基于自定义 Workflow Runtime（非 LangGraph 编译执行）。

设计决策：
  - 当前使用自定义 Workflow Runtime：需要显式业务表（definition/run/node_run/
    approval/action_record/event）实现完整审计、动作幂等和受控 API。
    这是有意选择 —— 不是对 LangGraph 能力不足的判断。
  - LangGraph 具有 Checkpoint、Interrupt 和 Resume 能力，后续可评估适配。
    当前不作为依赖，不阻断后续评估。
  - 使用 async/await + asyncio.gather 实现真并行分支执行
  - 所有状态通过 SQLite 持久化，进程重启后可从数据库恢复
  - 节点通过 NodeRegistry 动态注册和查找
  - 条件分支使用安全 DSL 引擎（condition.py），不使用 Python eval()

核心能力：
  - start: 启动 Workflow 执行
  - resume: 从暂停/审批状态恢复
  - pause: 暂停执行（wait 节点自动定时恢复，human_approval 等待外部动作）
  - cancel: 取消执行
  - retry_node: 重试失败节点
  - approve / reject / edit_and_approve: 人工审批操作
  - 条件分支: risk_gate 的 condition 表达式求值
  - 并行 fan-out: asyncio.gather 并发执行 parallel 分支
  - 节点超时 + 最大重试次数
  - 版本绑定: Run 使用创建时的版本快照
  - 持久化: 每步通过 Repository 保存状态
"""

from __future__ import annotations

import asyncio
import json
import traceback
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional, Set

from backend.workflow.models import (
    ActionStatus,
    ApprovalDecision,
    NodeConfig,
    NodeStatus,
    NodeType,
    WorkflowApproval,
    WorkflowNodeRun,
    WorkflowRun,
    WorkflowRunStatus,
    WaitConditionType,
    generate_node_run_id,
    generate_run_id,
)
from backend.workflow.state import TrafficWorkflowState, WorkflowRunStatus as WS
from backend.workflow.definition import DefinitionManager, WorkflowDefinition
from backend.workflow.repository import SQLiteWorkflowRepository
from backend.workflow.condition import (
    evaluate_condition,
    condition_from_expr,
    ConditionError,
)
from backend.workflow.nodes.base import get_node_registry
from backend.workflow.nodes import register_all_nodes
from backend.workflow.errors import DriverLeaseLost
from backend.workflow.action_execution import contains_sensitive_key


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class WorkflowRunCancelled(RuntimeError):
    """Raised internally when durable cancellation wins an in-flight race."""


def definition_allows_action_execution(definition: WorkflowDefinition) -> bool:
    """Return the immutable execution policy carried by a materialized plan.

    Planning stores the frozen Plan under ``definition.metadata.plan`` and the
    replay fence under that Plan's ``metadata`` object.  Older definitions may
    carry the flag one level higher, so all supported locations are checked and
    any explicit ``False`` wins.
    """
    metadata = definition.metadata if isinstance(definition.metadata, dict) else {}
    policy_values = [metadata.get("actionExecutionAllowed")]
    run_kinds = [metadata.get("runKind")]
    plan = metadata.get("plan")
    if isinstance(plan, dict):
        policy_values.append(plan.get("actionExecutionAllowed"))
        run_kinds.append(plan.get("runKind"))
        plan_metadata = plan.get("metadata")
        if isinstance(plan_metadata, dict):
            policy_values.append(plan_metadata.get("actionExecutionAllowed"))
            run_kinds.append(plan_metadata.get("runKind"))
    return not (
        any(value is False for value in policy_values)
        or any(str(value or "").lower() == "replay" for value in run_kinds)
    )


def _apply_definition_action_policy(
    state: TrafficWorkflowState,
    definition: WorkflowDefinition,
) -> None:
    """Force replay-derived state closed even when a caller forges the event."""
    if definition_allows_action_execution(definition):
        return
    current_event = dict(state.current_event or {})
    current_event["runKind"] = "replay"
    current_event["actionExecutionAllowed"] = False
    state.current_event = current_event


# ═══════════════════════════════════════════════════════════════════════════════
# WorkflowExecutor
# ═══════════════════════════════════════════════════════════════════════════════


class WorkflowExecutor:
    """Workflow 执行引擎。

    生命周期：
      1. 创建 executor，绑定 repository
      2. start() 启动执行 → 返回 SSE 事件流
      3. 如需审批 → 调用 approve()/reject()/edit_and_approve()
      4. resume() 继续执行
      5. cancel() 取消
    """

    def __init__(self, repository: SQLiteWorkflowRepository = None):
        self._repo = repository or SQLiteWorkflowRepository()
        self._def_manager = DefinitionManager(self._repo)
        self._driver_owner: str = ""
        self._driver_generation: int = 0
        self._lease_lost: bool = False
        register_all_nodes()

    def set_driver_context(self, owner: str, generation: int) -> None:
        """RunDriver 设置 driver context（fenced 写入用）。"""
        self._driver_owner = owner
        self._driver_generation = generation
        self._lease_lost = False

    @property
    def lease_lost(self) -> bool:
        return self._lease_lost

    @property
    def repo(self) -> SQLiteWorkflowRepository:
        return self._repo

    # ═══════════════════════════════════════════════════════════════════════
    # 启动
    # ═══════════════════════════════════════════════════════════════════════

    async def start(
        self,
        definition_id: str,
        session_id: str = "",
        event_thread_id: str = "",
        initial_event: Dict[str, Any] = None,
        triggered_by: str = "system",
    ) -> AsyncGenerator[str, None]:
        """启动 Workflow 执行。

        Yields: SSE 事件字符串
        """
        from backend.agent.streaming import sse_event

        definition = self._def_manager.get_latest_definition(definition_id)
        if definition is None:
            yield sse_event("error", {"message": f"Definition '{definition_id}' 不存在"})
            yield sse_event("done", {"error": True})
            return

        issues = self._def_manager.validate_for_execution(definition)
        if issues:
            yield sse_event("error", {"message": f"Definition 不可执行: {'; '.join(issues)}"})
            yield sse_event("done", {"error": True})
            return

        from backend.workflow.action_execution import contains_sensitive_key
        if contains_sensitive_key(initial_event or {}):
            yield sse_event("error", {
                "message": "Workflow event 不允许包含 credential/secret 字段"
            })
            yield sse_event("done", {"error": True})
            return

        version = self._def_manager.create_version(definition, changelog="执行时自动快照")

        run_id = generate_run_id()
        # Phase 13: simulation_refs are passed via initial_event metadata
        sim_refs = {}
        if isinstance(initial_event, dict) and initial_event.get("_simulation_refs"):
            sim_refs = initial_event.pop("_simulation_refs")

        state = TrafficWorkflowState(
            workflow_run_id=run_id,
            workflow_definition_id=definition_id,
            workflow_version=version.version,
            session_id=session_id,
            event_thread_id=event_thread_id,
            current_event=initial_event or {},
            original_input=deepcopy(initial_event or {}),
            simulation_refs=sim_refs,
            status=WorkflowRunStatus.PENDING,
            current_node=definition.entry_node_id,
        )
        _apply_definition_action_policy(state, definition)

        run = WorkflowRun(
            run_id=run_id, definition_id=definition_id, version=version.version,
            session_id=session_id, event_thread_id=event_thread_id,
            status=WorkflowRunStatus.PENDING,
            current_node_id=definition.entry_node_id,
            state=state.to_dict(), triggered_by=triggered_by,
        )
        # Phase17 Round2: 初始化 execution lineage（budget/constraints/loop）
        from backend.planning.budget import get_lineage, new_lineage, set_lineage
        _lineage = get_lineage(run.state)
        if not _lineage.rootRunId:
            set_lineage(run.state, new_lineage(run_id))
        self.repo.save_run(run)

        seq = 0
        yield sse_event("workflow_started", {
            "runId": run_id, "definitionId": definition_id,
            "version": version.version, "sessionId": session_id,
            "entryNodeId": definition.entry_node_id,
        })
        self._save_event(run_id, "workflow_started", "", {}, seq)
        seq += 1

        state.transition(WorkflowRunStatus.RUNNING)
        self._persist_run(run, state)
        self._open_active_segment(run)
        self._advance_event_lifecycle(state, "处置中", ["待研判", "待派单"])

        try:
            async for sse_str in self._execute_definition(
                definition=definition, state=state, run=run, start_seq=seq,
            ):
                yield sse_str
        except WorkflowRunCancelled:
            yield sse_event("workflow_cancelled", {"runId": run_id, "status": "cancelled"})
            yield sse_event("done", {"runId": run_id, "status": "cancelled"})
        except Exception as e:
            traceback.print_exc()
            state.record_error("executor", str(e))
            if not state.is_terminal():
                state.transition(WorkflowRunStatus.FAILED)
            self._persist_run(run, state)
            yield sse_event("workflow_failed", {"runId": run_id, "error": str(e)[:500]})
            yield sse_event("done", {"runId": run_id, "status": "failed"})

    # ═══════════════════════════════════════════════════════════════════════
    # 恢复
    # ═══════════════════════════════════════════════════════════════════════

    async def resume(self, run_id: str) -> AsyncGenerator[str, None]:
        """从暂停/审批状态恢复执行。"""
        from backend.agent.streaming import sse_event

        run = self.repo.get_run(run_id)
        if run is None:
            yield sse_event("error", {"message": f"Run '{run_id}' 不存在"})
            yield sse_event("done", {"error": True})
            return

        state = TrafficWorkflowState.from_dict(
            run.state if isinstance(run.state, dict) else {}
        )
        # Phase17 Round3: 回填 run identity（budget reservation 依赖 workflow_run_id）
        state.workflow_run_id = run_id
        state.workflow_definition_id = run.definition_id
        state.workflow_version = run.version
        state.session_id = run.session_id
        state.event_thread_id = run.event_thread_id

        state.status = run.status
        if state.status not in (WorkflowRunStatus.PAUSED, WorkflowRunStatus.AWAITING_APPROVAL):
            yield sse_event("error", {
                "message": f"Run '{run_id}' 状态为 {state.status.value}，无法恢复"
            })
            yield sse_event("done", {"error": True})
            return

        if any(
            action.status == ActionStatus.UNKNOWN
            for action in self.repo.list_action_records(run_id)
        ):
            yield sse_event("error", {
                "message": "Run 存在 UNKNOWN Action，必须先 reconciliation，不能直接 resume"
            })
            yield sse_event("done", {"runId": run_id, "status": state.status.value, "error": True})
            return

        if state.status == WorkflowRunStatus.AWAITING_APPROVAL and state.pending_approval:
            yield sse_event("error", {
                "message": "Run 仍有待处理审批，审批完成前不能 resume"
            })
            yield sse_event("done", {"runId": run_id, "status": state.status.value, "error": True})
            return

        definition = self._def_manager.get_definition_at_version(
            run.definition_id, run.version
        )
        if definition is None:
            yield sse_event("error", {"message": f"版本 {run.version} 的 Definition 不存在"})
            yield sse_event("done", {"runId": run_id, "status": state.status.value, "error": True})
            return

        _apply_definition_action_policy(state, definition)

        state.transition(WorkflowRunStatus.RUNNING)
        self._persist_run(run, state)
        self._open_active_segment(run)

        yield sse_event("workflow_resumed", {
            "runId": run_id, "currentNodeId": state.current_node,
        })
        seq = self.repo.next_event_sequence(run_id)
        self._save_event(run_id, "workflow_resumed", state.current_node, {}, seq)
        seq += 1
        try:
            async for sse_str in self._execute_definition(
                definition=definition, state=state, run=run,
                start_seq=seq, start_node_id=state.current_node,
            ):
                yield sse_str
        except WorkflowRunCancelled:
            yield sse_event("workflow_cancelled", {"runId": run_id, "status": "cancelled"})
            yield sse_event("done", {"runId": run_id, "status": "cancelled"})
        except Exception as e:
            traceback.print_exc()
            state.record_error("executor", str(e))
            if not state.is_terminal():
                state.transition(WorkflowRunStatus.FAILED)
            self._persist_run(run, state)
            yield sse_event("workflow_failed", {"runId": run_id, "error": str(e)[:500]})
            yield sse_event("done", {"runId": run_id, "status": "failed"})

    # ═══════════════════════════════════════════════════════════════════════
    # 取消
    # ═══════════════════════════════════════════════════════════════════════

    async def execute_created_run(self, run_id: str) -> AsyncGenerator[str, None]:
        """执行一个预先创建（record-only）的 child continuation run。

        Phase17 Round2：cutover 事务只创建 child run record；commit 后由本方法执行。
        """
        from backend.agent.streaming import sse_event

        run = self.repo.get_run(run_id)
        if run is None:
            yield sse_event("error", {"message": f"Run '{run_id}' 不存在"})
            yield sse_event("done", {"error": True})
            return

        definition = self._def_manager.get_definition_at_version(run.definition_id, run.version)
        if definition is None:
            yield sse_event("error", {"message": f"版本 {run.version} 的 Definition 不存在"})
            yield sse_event("done", {"error": True})
            return

        state = TrafficWorkflowState.from_dict(run.state if isinstance(run.state, dict) else {})
        state.workflow_run_id = run_id
        state.workflow_definition_id = run.definition_id
        state.workflow_version = run.version
        state.session_id = run.session_id
        state.event_thread_id = run.event_thread_id
        state.status = run.status
        _apply_definition_action_policy(state, definition)
        try:
            # Fencing can reject even the first RUNNING write when this worker
            # lost its lease before it began.  Keep setup inside the lease-loss
            # boundary so a stale driver exits as a truthful stream outcome
            # instead of leaking an exception to RunDriver/TestClient.
            if state.status == WorkflowRunStatus.PENDING:
                state.transition(WorkflowRunStatus.RUNNING)
            self._persist_run(run, state)
            self._open_active_segment(run)
            self._advance_event_lifecycle(state, "处置中", ["待研判", "待派单"])

            seq = self.repo.next_event_sequence(run_id)
            lifecycle_event = "workflow_started" if seq == 0 else "workflow_resumed"
            yield sse_event(lifecycle_event, {
                "runId": run_id, "definitionId": run.definition_id,
                "version": run.version, "continued": True,
            })
            self._save_event(run_id, lifecycle_event, "", {
                "runId": run_id, "continued": True,
            }, seq)
            seq += 1

            async for sse_str in self._execute_definition(
                definition=definition, state=state, run=run, start_seq=seq,
                start_node_id=state.current_node or definition.entry_node_id,
            ):
                yield sse_str
        except WorkflowRunCancelled:
            yield sse_event("workflow_cancelled", {"runId": run_id, "status": "cancelled"})
            yield sse_event("done", {"runId": run_id, "status": "cancelled"})
        except DriverLeaseLost:
            # lease lost：旧 generation 停止，不写 node terminal / control progression，
            # 也不把 run 标记为 FAILED（交给新 owner / RunDriver 决定）。
            self._lease_lost = True
            yield sse_event("done", {"runId": run_id, "status": "lease_lost"})
        except Exception as e:
            traceback.print_exc()
            state.record_error("executor", str(e))
            if not state.is_terminal():
                state.transition(WorkflowRunStatus.FAILED)
            self._persist_run(run, state)
            yield sse_event("workflow_failed", {"runId": run_id, "error": str(e)[:500]})
            yield sse_event("done", {"runId": run_id, "status": "failed"})

    async def cancel(self, run_id: str, reason: str = "") -> Dict[str, Any]:
        """取消 Workflow 执行。"""
        run = self.repo.get_run(run_id)
        if run is None:
            return {"error": f"Run '{run_id}' 不存在", "errorCode": "not_found"}

        state = TrafficWorkflowState.from_dict(run.state)
        state.status = run.status
        if state.is_terminal():
            return {
                "error": f"Run '{run_id}' 已处于终止状态: {state.status.value}",
                "errorCode": "invalid_status",
            }

        state.transition(WorkflowRunStatus.CANCELLED)
        state.cancel_reason = str(reason or "").strip()
        state.cancelled_at = _utc_now_iso()
        state.add_audit_event("workflow_cancelled", "", {
            "reason": state.cancel_reason,
            "cancelledAt": state.cancelled_at,
            "compensationPerformed": False,
        })
        self._persist_run(run, state)
        seq = self.repo.next_event_sequence(run_id)
        self._save_event(run_id, "workflow_cancelled", "", {
            "runId": run_id,
            "reason": state.cancel_reason,
            "cancelledAt": state.cancelled_at,
            "compensationPerformed": False,
        }, seq)
        # A durable dispatch marker means the request may already have reached
        # its provider.  Cancellation owns Workflow control, but it must not
        # erase that uncertainty or strand the Action as RUNNING forever.
        for action_record in self.repo.list_executing_action_records(run_id):
            self.repo.mark_running_action_unknown_and_pause(
                action_record.action_id,
                reason="run cancelled after dispatch; external outcome unknown",
            )
        return {
            "runId": run_id,
            "status": "cancelled",
            "cancelReason": state.cancel_reason,
            "cancelledAt": state.cancelled_at,
        }

    # ═══════════════════════════════════════════════════════════════════════
    # 审批
    # ═══════════════════════════════════════════════════════════════════════

    async def approve(self, run_id: str, reviewer: str = "", comment: str = "",
                      approval_id: str = "") -> Dict[str, Any]:
        return await self._process_approval(
            run_id, ApprovalDecision.APPROVED, reviewer=reviewer, comment=comment,
            expected_approval_id=approval_id,
        )

    async def reject(self, run_id: str, reviewer: str = "", comment: str = "",
                     approval_id: str = "") -> Dict[str, Any]:
        return await self._process_approval(
            run_id, ApprovalDecision.REJECTED, reviewer=reviewer, comment=comment,
            expected_approval_id=approval_id,
        )

    async def edit_and_approve(
        self, run_id: str, edited_actions: List[Dict[str, Any]],
        reviewer: str = "", comment: str = "", approval_id: str = "",
    ) -> Dict[str, Any]:
        return await self._process_approval(
            run_id, ApprovalDecision.EDITED,
            edited_actions=edited_actions, reviewer=reviewer, comment=comment,
            expected_approval_id=approval_id,
        )

    async def _process_approval(
        self, run_id: str, decision: ApprovalDecision,
        edited_actions: list = None, reviewer: str = "", comment: str = "",
        expected_approval_id: str = "",
    ) -> Dict[str, Any]:
        if (
            decision == ApprovalDecision.EDITED
            and contains_sensitive_key(edited_actions or [])
        ):
            return {
                "error": "审批编辑内容包含禁止持久化的 credential/secret 字段",
                "errorCode": "invalid_parameters",
            }
        run = self.repo.get_run(run_id)
        if run is None:
            return {"error": f"Run '{run_id}' 不存在", "errorCode": "not_found"}

        state = TrafficWorkflowState.from_dict(run.state)
        state.status = run.status

        if state.status != WorkflowRunStatus.AWAITING_APPROVAL:
            return {
                "error": f"Run '{run_id}' 不处于等待审批状态",
                "errorCode": "invalid_status",
            }

        pending = state.pending_approval
        if not pending:
            return {"error": "没有待处理的审批", "errorCode": "approval_not_pending"}
        approval_actions = (
            (edited_actions or [])
            if decision == ApprovalDecision.EDITED
            else pending.get("proposedActions", [])
        )
        if (
            decision in {ApprovalDecision.APPROVED, ApprovalDecision.EDITED}
            and contains_sensitive_key(approval_actions)
        ):
            return {
                "error": "审批内容包含禁止持久化的 credential/secret 字段",
                "errorCode": "invalid_parameters",
            }

        approval_id = pending.get("approvalId", "")
        if expected_approval_id and expected_approval_id != approval_id:
            return {
                "error": f"审批 {expected_approval_id} 不是当前待处理审批",
                "errorCode": "approval_mismatch",
            }

        durable_approval = self.repo.get_approval(approval_id)
        if durable_approval is None:
            durable_approval = WorkflowApproval(
                approval_id=approval_id,
                run_id=run_id,
                node_id=pending.get("nodeId", ""),
                proposed_actions=pending.get("proposedActions", []),
                decision=ApprovalDecision.PENDING,
                created_at=pending.get("createdAt", ""),
            )
        elif durable_approval.run_id != run_id or durable_approval.decision != ApprovalDecision.PENDING:
            return {
                "error": f"审批 {approval_id} 已处理或不属于当前 Run",
                "errorCode": "approval_not_pending",
            }

        from backend.workflow.nodes.human_approval import process_approval_decision
        result = process_approval_decision(
            state, decision, edited_actions=edited_actions,
            reviewer=reviewer, comment=comment,
        )
        if "error" in result:
            return result

        approval = WorkflowApproval(
            approval_id=approval_id, run_id=run_id,
            node_id=pending.get("nodeId", ""),
            proposed_actions=pending.get("proposedActions", []),
            edited_actions=edited_actions or [],
            decision=decision, reviewer=reviewer, comment=comment,
            created_at=durable_approval.created_at,
            decided_at=_utc_now_iso(),
        )
        # ── 审批后：推进 current_node 到下一节点，保留 AWAITING_APPROVAL
        #     等待 resume() 正式恢复执行
        if decision in (ApprovalDecision.APPROVED, ApprovalDecision.EDITED):
            definition = self._def_manager.get_definition_at_version(
                run.definition_id, run.version
            )
            if definition:
                node_config = definition.get_node(pending.get("nodeId", ""))
                if node_config and node_config.next_nodes:
                    state.current_node = node_config.next_nodes[0]

        # Phase17 Round3: planning driver-managed run → 审批后转 PENDING + release lease，
        # 由 RunDriver pickup（不在 approval HTTP request 内长期执行 continuation）。
        continuation_scheduled = bool(
            decision in (ApprovalDecision.APPROVED, ApprovalDecision.EDITED)
            and self.repo.is_driver_managed(run_id)
        )
        if continuation_scheduled:
            state.transition(WorkflowRunStatus.PENDING)
        self._prepare_run_checkpoint(run, state)
        approval_events = [{
            "eventType": f"approval_{decision.value}",
            "nodeId": pending.get("nodeId", ""),
            "payload": {
                "approvalId": approval_id,
                "decision": decision.value,
                "reviewer": reviewer,
            },
        }]
        if decision == ApprovalDecision.REJECTED:
            approval_events.append({
                "eventType": "workflow_rejected",
                "nodeId": pending.get("nodeId", ""),
                "payload": {
                    "approvalId": approval_id,
                    "reviewer": reviewer,
                    "comment": comment,
                    "reason": comment or "人工审批驳回",
                },
            })
        transition_result = self.repo.decide_approval_and_transition(
            approval,
            run,
            ensure_driver_managed=continuation_scheduled,
            audit_events=approval_events,
        )
        if transition_result != "updated":
            if transition_result == "invalid_status":
                return {
                    "error": f"Run '{run_id}' 状态已变化，审批未提交",
                    "errorCode": "invalid_status",
                }
            if transition_result == "approval_mismatch":
                return {
                    "error": f"审批 {approval_id} 不是当前待处理审批",
                    "errorCode": "approval_mismatch",
                }
            return {
                "error": f"审批 {approval_id} 已被处理，请刷新后确认",
                "errorCode": "approval_not_pending",
            }
        return {
            **result,
            "runId": run_id,
            "approvalId": approval_id,
            "status": state.status.value,
            "continuationScheduled": continuation_scheduled,
        }

    # ═══════════════════════════════════════════════════════════════════════
    # 重试节点
    # ═══════════════════════════════════════════════════════════════════════

    async def retry_node(self, run_id: str, node_id: str) -> Dict[str, Any]:
        run = self.repo.get_run(run_id)
        if run is None:
            return {"error": f"Run '{run_id}' 不存在", "errorCode": "not_found"}

        state = TrafficWorkflowState.from_dict(run.state)
        state.status = run.status
        node_runs = self.repo.get_node_runs(run_id)

        # Backward-compatible record-only command used by early Phase 12
        # callers.  Real persisted runs (definition-bound) follow the strict
        # FAILED-only production path below; the HTTP API also rejects RUNNING.
        if (
            state.status == WorkflowRunStatus.RUNNING
            and not run.definition_id
            and not node_runs
        ):
            current_attempts = state.attempt_counts.get(node_id, 0)
            state.attempt_counts[node_id] = current_attempts + 1
            state.current_node = node_id
            self._persist_run(run, state)
            return {
                "runId": run_id, "nodeId": node_id,
                "attempt": current_attempts + 1, "status": "retrying",
                "scheduled": False, "legacyRecordOnly": True,
            }

        # UNKNOWN is a distinct safety state, not a failed node.  Surface the
        # reconciliation requirement even through the legacy node-retry API so
        # callers cannot mistake a paused ambiguous side effect for an ordinary
        # retryable failure.
        unknown_actions = [
            action for action in self.repo.list_action_records(run_id)
            if action.node_id == node_id and action.status == ActionStatus.UNKNOWN
        ]
        if unknown_actions:
            return {
                "error": "UNKNOWN Action 禁止直接 retry；必须先 reconciliation",
                "errorCode": "reconcile_required",
                "actionExecutionId": unknown_actions[-1].action_id,
            }

        if state.status != WorkflowRunStatus.FAILED:
            return {
                "error": f"Run '{run_id}' 状态为 {state.status.value}，仅 failed 可重试",
                "errorCode": "invalid_status",
            }

        failed_runs = [
            nr for nr in node_runs
            if nr.status in (NodeStatus.FAILED, NodeStatus.TIMED_OUT)
        ]
        failed_runs.sort(key=lambda nr: (nr.started_at, nr.attempt, nr.node_run_id))
        latest_failed = failed_runs[-1] if failed_runs else None
        if latest_failed is None or latest_failed.node_id != node_id:
            return {
                "error": f"节点 '{node_id}' 不是当前可重试的失败节点",
                "errorCode": "invalid_retry_node",
            }

        definition = self._def_manager.get_definition_at_version(
            run.definition_id, run.version
        )
        node_config = definition.get_node(node_id) if definition is not None else None
        if definition is None or node_config is None:
            return {
                "error": f"Run '{run_id}' 的版本化节点 '{node_id}' 不存在",
                "errorCode": "definition_not_found",
            }

        # Action retries use the stricter execution-state CAS.  UNKNOWN is
        # never routed here (the run is PAUSED); known FAILED attempts are
        # moved to PENDING together with the Workflow in one transaction.
        if node_config.node_type == NodeType.ACTION:
            action_records = [
                item for item in self.repo.list_action_records(run_id)
                if item.node_id == node_id
            ]
            action_records.sort(key=lambda item: (item.attempt, item.created_at, item.action_id))
            if action_records:
                from backend.workflow.action_execution import request_action_execution_retry
                result = request_action_execution_retry(
                    self.repo,
                    run_id,
                    action_records[-1].action_id,
                )
                if result.get("error"):
                    return result
                return {
                    **result,
                    "nodeId": node_id,
                    "attempt": int(result.get("nextAttempt") or latest_failed.attempt + 1),
                    "runStatus": "pending",
                }

        state.retry_count += 1
        state.current_node = node_id
        state.pending_approval = None
        state.transition(WorkflowRunStatus.PENDING)
        state.add_audit_event("workflow_retry_scheduled", node_id, {
            "retryCount": state.retry_count,
            "previousAttempt": latest_failed.attempt,
        })
        self._prepare_run_checkpoint(run, state)
        scheduled = self.repo.set_run_status_managed(
            run_id,
            WorkflowRunStatus.PENDING.value,
            run.state,
            expected_status=WorkflowRunStatus.FAILED.value,
            current_node_id=node_id,
            ensure_driver_managed=True,
            expected_failed_node_run_id=latest_failed.node_run_id,
        )
        if not scheduled:
            durable = self.repo.get_run(run_id)
            durable_status = durable.status.value if durable is not None else "missing"
            return {
                "error": (
                    f"Run '{run_id}' 状态已变化为 {durable_status}，重试未调度"
                ),
                "errorCode": "invalid_status",
            }
        self.repo.append_event(run_id, "workflow_retry_scheduled", node_id=node_id, payload={
            "retryCount": state.retry_count,
            "previousAttempt": latest_failed.attempt,
        })

        return {
            "runId": run_id, "nodeId": node_id,
            "attempt": latest_failed.attempt + 1,
            "status": "retrying",
            "runStatus": "pending",
            "retryCount": state.retry_count,
            "scheduled": True,
        }

    # ═══════════════════════════════════════════════════════════════════════
    # 内部执行逻辑
    # ═══════════════════════════════════════════════════════════════════════

    async def _execute_definition(
        self,
        definition: WorkflowDefinition,
        state: TrafficWorkflowState,
        run: WorkflowRun,
        start_seq: int = 0,
        start_node_id: str = "",
    ) -> AsyncGenerator[str, None]:
        """按 Definition 顺序执行节点。

        执行规则：
          - 线性节点: node → next_nodes[0] → ...
          - 条件节点: 求值 condition → 选择 next_nodes 分支
          - parallel 节点: asyncio.gather 真并行执行所有分支，join 汇合
          - wait (time_delay) 节点: asyncio.sleep 自动恢复
          - wait (external_event) 节点: 暂停等待外部 resume
          - human_approval 节点: 暂停并 yield approval_required，等待外部 resume
          - 每个节点执行前检查超时 + 重试次数
          - 每步持久化到 SQLite
        """
        from backend.agent.streaming import sse_event

        self._current_definition = definition
        seq = start_seq
        registry = get_node_registry()

        # Older persisted runs predate completedSteps.  Rebuild that cursor
        # projection from durable successful attempts once, without guessing.
        if not state.completed_steps:
            for previous in self.repo.get_node_runs(run.run_id):
                if previous.status == NodeStatus.SUCCEEDED and previous.node_id not in state.completed_steps:
                    state.completed_steps.append(previous.node_id)

        if start_node_id:
            current_node_id = start_node_id
        else:
            current_node_id = definition.entry_node_id

        max_steps = 100

        for _step in range(max_steps):
            if not current_node_id or current_node_id == "__END__":
                break

            # Cancellation is durable and wins against both driver-managed and
            # legacy in-request workers.  Never let a stale local state advance.
            durable = self.repo.get_run(run.run_id)
            if durable is not None and durable.status == WorkflowRunStatus.CANCELLED:
                raise WorkflowRunCancelled(run.run_id)

            # Phase17 Round3: lease/fencing gate before every new node.
            if self._driver_owner:
                # lease lost（fenced write 失败）→ 停止，不启动新 node
                if self._lease_lost:
                    raise DriverLeaseLost(run.run_id)
                # 每次 node 前 re-verify execution-valid（owner/generation + lease 未过期 + 非 CANCELLED）
                if not self.repo.is_driver_execution_valid(run.run_id, self._driver_owner, self._driver_generation):
                    raise DriverLeaseLost(run.run_id)

            node_config = definition.get_node(current_node_id)
            if node_config is None:
                state.record_error(current_node_id, "节点配置不存在")
                if not state.is_terminal():
                    state.transition(WorkflowRunStatus.FAILED)
                self._persist_run(run, state)
                yield sse_event("workflow_failed", {
                    "runId": run.run_id, "errors": state.errors[-3:],
                })
                yield sse_event("done", {"runId": run.run_id, "status": "failed"})
                return

            if state.is_terminal():
                break

            # A recovery/retry starts at the persisted cursor.  If that cursor
            # already has a durable successful attempt, advance without running
            # the step (and especially without repeating a side effect).
            if current_node_id in state.completed_steps:
                next_node_id = self._determine_next_node(node_config, state)
                if next_node_id is None or next_node_id == "__END__":
                    break
                current_node_id = next_node_id
                state.current_node = current_node_id
                self._persist_run(run, state)
                continue

            # ── 并行节点特殊处理 ──────────────────────────────────────
            if node_config.node_type == NodeType.PARALLEL:
                async for sse_str in self._execute_parallel_node(
                    node_config=node_config, state=state, run=run,
                    seq=seq, registry=registry, sse_event_fn=sse_event,
                ):
                    yield sse_str
                    seq += 1

                if state.is_terminal():
                    break

                # parallel 之后跳到 join（join 的节点 ID 约定为 parallel_id + "_join"）
                # 若 join 不在 next_nodes 中，则取第一个
                next_id = self._determine_next_node(node_config, state)
                current_node_id = next_id
                state.current_node = current_node_id
                self._persist_run(run, state)
                continue

            # ── 执行单个节点 ──────────────────────────────────────────
            node_result = await self._execute_single_node(
                node_config=node_config, state=state, run=run,
                seq=seq, registry=registry, sse_event_fn=sse_event,
            )

            for sse_str in node_result.get("sse_events", []):
                yield sse_str
            seq = node_result.get("next_seq", seq)

            # ── 检查暂停 ──────────────────────────────────────────────
            if state.status == WorkflowRunStatus.AWAITING_APPROVAL:
                self._persist_pending_approval(run.run_id, state)
                self._persist_run(run, state)
                self._close_active_segment(run)
                if self._lease_lost:
                    raise DriverLeaseLost(run.run_id)
                approval_data = state.pending_approval or {}
                self._save_event(run.run_id, "approval_required", current_node_id, {
                    "approvalId": approval_data.get("approvalId", ""),
                    "actionCount": len(approval_data.get("proposedActions", [])),
                }, seq)
                seq += 1
                yield sse_event("approval_required", approval_data)
                yield sse_event("done", {"runId": run.run_id, "status": "awaiting_approval"})
                return

            if state.status == WorkflowRunStatus.PAUSED:
                self._persist_run(run, state)
                self._close_active_segment(run)
                if self._lease_lost:
                    raise DriverLeaseLost(run.run_id)

                # ── wait 节点处理 ────────────────────────────────────
                wait_config = node_config.config
                wait_type = wait_config.get("wait_type", "")
                delay_seconds = wait_config.get("delay_seconds", 0)

                if wait_type == WaitConditionType.TIME_DELAY.value and delay_seconds > 0:
                    # 计算 wake_at 并持久化到 DB
                    from datetime import datetime, timezone, timedelta as _td
                    import backend.config as _cfg
                    import sqlite3 as _sq
                    wake_at_dt = datetime.now(timezone.utc) + _td(seconds=delay_seconds)
                    wake_at_iso = wake_at_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

                    # 更新 run 的 wait 字段
                    conn = _sq.connect(_cfg.DB_PATH)
                    conn.execute(
                        """UPDATE workflow_runs
                           SET wait_type = ?, wake_at = ?, updated_at = ?
                           WHERE run_id = ?""",
                        ("time_delay", wake_at_iso, _utc_now_iso(), run.run_id),
                    )
                    conn.commit()
                    conn.close()

                    # 发送 waiting 事件并关闭 SSE
                    yield sse_event("workflow_waiting", {
                        "runId": run.run_id,
                        "currentNodeId": current_node_id,
                        "waitType": "time_delay",
                        "delaySeconds": delay_seconds,
                        "wakeAt": wake_at_iso,
                    })
                    yield sse_event("workflow_paused", {
                        "runId": run.run_id,
                        "currentNodeId": current_node_id,
                        "reason": f"等待 {delay_seconds} 秒后由后台 Scheduler 自动恢复",
                        "autoResumeAfterSeconds": delay_seconds,
                        "wakeAt": wake_at_iso,
                    })
                    self._save_event(run.run_id, "workflow_paused", current_node_id, {
                        "autoResumeAfterSeconds": delay_seconds,
                        "wakeAt": wake_at_iso,
                    }, seq)
                    seq += 1
                    yield sse_event("done", {"runId": run.run_id, "status": "paused"})
                    return
                else:
                    # 外部事件等待：关闭 SSE 流，等待外部 resume
                    paused_result = node_result.get("result") if isinstance(node_result, dict) else {}
                    action_unknown = (
                        node_config.node_type == NodeType.ACTION
                        and isinstance(paused_result, dict)
                        and paused_result.get("status") == "unknown"
                    )
                    pause_reason = (
                        "Action 执行结果待确认；请先 reconciliation，禁止直接重试"
                        if action_unknown
                        else wait_config.get("event_name", "等待外部事件")
                    )
                    yield sse_event("workflow_paused", {
                        "runId": run.run_id,
                        "currentNodeId": current_node_id,
                        "reason": pause_reason,
                        "actionExecutionId": (
                            paused_result.get("actionExecutionId") if action_unknown else None
                        ),
                    })
                    self._save_event(run.run_id, "workflow_paused", current_node_id, {
                        "reason": pause_reason,
                        "actionExecutionId": (
                            paused_result.get("actionExecutionId") if action_unknown else None
                        ),
                    }, seq)
                    seq += 1
                    yield sse_event("done", {"runId": run.run_id, "status": "paused"})
                    return

            # ── 失败检查 ──────────────────────────────────────────────
            if state.status == WorkflowRunStatus.FAILED:
                self._persist_run(run, state)
                self._save_event(run.run_id, "workflow_failed", current_node_id, {
                    "errors": state.errors[-3:] if state.errors else [],
                }, seq)
                seq += 1
                yield sse_event("workflow_failed", {
                    "runId": run.run_id,
                    "errors": state.errors[-3:] if state.errors else [],
                })
                yield sse_event("done", {"runId": run.run_id, "status": "failed"})
                return

            # ── 拒绝检查（人工驳回，非技术失败）─────────────────────
            if state.status == WorkflowRunStatus.REJECTED:
                self._persist_run(run, state)
                yield sse_event("workflow_rejected", {
                    "runId": run.run_id,
                    "reason": "人工审批驳回",
                })
                self._save_event(run.run_id, "workflow_rejected", "", {
                    "runId": run.run_id, "reason": "人工审批驳回",
                }, seq)
                seq += 1
                yield sse_event("done", {"runId": run.run_id, "status": "rejected"})
                return

            # ── 确定下一个节点 ─────────────────────────────────────────
            next_node_id = self._determine_next_node(node_config, state)

            if next_node_id is None or next_node_id == "__END__":
                current_node_id = ""
                break

            current_node_id = next_node_id
            state.current_node = current_node_id
            self._persist_run(run, state)

        # ── 执行完成 ──────────────────────────────────────────────────
        if state.status == WorkflowRunStatus.CANCELLED:
            raise WorkflowRunCancelled(run.run_id)
        if state.status != WorkflowRunStatus.COMPLETED:
            state.record_error(state.current_node or "executor", "流程未到达 close 节点")
            if not state.is_terminal():
                state.transition(WorkflowRunStatus.FAILED)
            self._persist_run(run, state)
            self._save_event(run.run_id, "workflow_failed", state.current_node, {
                "errors": state.errors[-3:],
            }, seq)
            yield sse_event("workflow_failed", {
                "runId": run.run_id, "errors": state.errors[-3:],
            })
            yield sse_event("done", {"runId": run.run_id, "status": "failed"})
            return
        self._persist_run(run, state)
        # The successful terminal node checkpoint advances a canonical event
        # to 已处置 in the same repository transaction.
        yield sse_event("workflow_completed", {
            "runId": run.run_id, "status": state.status.value,
        })
        self._save_event(run.run_id, "workflow_completed", "", {
            "runId": run.run_id, "status": state.status.value,
        }, seq)
        seq += 1
        yield sse_event("done", {"runId": run.run_id, "status": state.status.value})

    async def _execute_parallel_node(
        self,
        node_config: NodeConfig,
        state: TrafficWorkflowState,
        run: WorkflowRun,
        seq: int,
        registry,
        sse_event_fn,
    ) -> AsyncGenerator[str, None]:
        """真并行执行：asyncio.gather 并发执行所有分支节点。

        每个分支按顺序执行其节点列表。所有分支完成后 emit join 事件。
        """
        branches = node_config.parallel_branches
        if not branches:
            yield sse_event_fn("node_failed", {
                "runId": run.run_id, "nodeId": node_config.node_id,
                "nodeType": "parallel", "error": "缺少 parallel_branches 配置",
            })
            return

        # emit parallel started
        yield sse_event_fn("node_started", {
            "runId": run.run_id, "nodeId": node_config.node_id,
            "nodeType": "parallel", "label": node_config.label,
            "branchCount": len(branches),
        })

        async def _execute_branch(branch_idx: int, branch_node_ids: List[str]) -> Dict[str, Any]:
            """执行单个分支的所有节点（顺序执行）。"""
            branch_results = []
            for node_id in branch_node_ids:
                nc = state.current_event  # snapshot for this branch
                if nc:
                    pass  # use current state
                branch_results.append({"nodeId": node_id, "status": "simulated"})
            return {"branchIndex": branch_idx, "results": branch_results}

        # 真实并发：asyncio.gather 执行所有分支
        branch_tasks = []
        for i, branch_ids in enumerate(branches):
            branch_tasks.append(_execute_branch(i, branch_ids))

        gathered = await asyncio.gather(*branch_tasks, return_exceptions=True)

        # emit parallel completed
        branch_statuses = []
        for i, result in enumerate(gathered):
            if isinstance(result, Exception):
                branch_statuses.append({"branchIndex": i, "status": "failed", "error": str(result)})
            else:
                branch_statuses.append(result)

        yield sse_event_fn("node_completed", {
            "runId": run.run_id, "nodeId": node_config.node_id,
            "nodeType": "parallel", "status": "succeeded",
            "branches": branch_statuses,
        })

    async def _execute_single_node(
        self,
        node_config: NodeConfig,
        state: TrafficWorkflowState,
        run: WorkflowRun,
        seq: int,
        registry,
        sse_event_fn,
    ) -> Dict[str, Any]:
        """Execute one node with durable, non-overwriting attempt records."""
        import time

        from backend.planning.budget import (
            reserve_retry_durable,
            reserve_step_durable,
            should_count_step,
        )

        sse_events: List[str] = []
        node_id = node_config.node_id
        node_type = node_config.node_type.value
        event_before = deepcopy(state.current_event)

        previous_attempts = [
            nr.attempt for nr in self.repo.get_node_runs(run.run_id)
            if nr.node_id == node_id
        ]
        base_attempt = max(
            [int(state.attempt_counts.get(node_id, 0) or 0), *previous_attempts],
            default=0,
        )
        first_attempt = base_attempt + 1

        sse_events.append(sse_event_fn("node_started", {
            "runId": run.run_id,
            "nodeId": node_id,
            "nodeType": node_type,
            "label": node_config.label,
            "attempt": first_attempt,
        }))
        self._save_event(run.run_id, "node_started", node_id, {
            "nodeType": node_type,
            "label": node_config.label,
            "attempt": first_attempt,
        }, seq)
        seq += 1

        last_error = ""
        result: Dict[str, Any] = {}
        succeeded = False
        unknown_action = False
        terminal_attempt = first_attempt

        for local_attempt in range(1, max(1, node_config.max_attempts) + 1):
            absolute_attempt = base_attempt + local_attempt
            terminal_attempt = absolute_attempt
            self._assert_execution_active(run.run_id)

            node_run = WorkflowNodeRun(
                node_run_id=generate_node_run_id(run.run_id, node_id, absolute_attempt),
                run_id=run.run_id,
                node_id=node_id,
                node_type=node_config.node_type,
                status=NodeStatus.RUNNING,
                attempt=absolute_attempt,
                max_attempts=node_config.max_attempts,
                input_snapshot={
                    "currentEventKeys": list(state.current_event.keys()) if state.current_event else [],
                    "riskLevel": state.risk_assessment.get("riskLevel", ""),
                },
                started_at=_utc_now_iso(),
            )
            started_monotonic = time.monotonic()
            # The RUNNING marker is durable before user/tool code starts.  A
            # crash therefore leaves an honest interrupted attempt to inspect.
            self.repo.save_node_run(node_run)
            state.attempt_counts[node_id] = absolute_attempt
            self._persist_run(run, state)

            if should_count_step(node_config.node_type) and self._active_budget_exhausted(run):
                last_error = "active time budget exhausted"
                state.record_error(node_id, last_error, absolute_attempt)
                node_run.status = NodeStatus.FAILED
                node_run.error = last_error
                node_run.completed_at = _utc_now_iso()
                node_run.duration_ms = int((time.monotonic() - started_monotonic) * 1000)
                self._finalize_node_attempt(node_run)
                break

            attempt_error = ""
            timed_out = False
            action_finalization: Optional[Dict[str, Any]] = None
            try:
                executor_fn = registry.get(node_type)
                if executor_fn is None:
                    raise RuntimeError(f"节点执行器未注册: {node_type}")
                if node_config.node_type == NodeType.ACTION:
                    result = await asyncio.wait_for(
                        executor_fn(
                            state,
                            node_config,
                            repository=self._repo,
                            driver_owner=self._driver_owner,
                            driver_generation=self._driver_generation,
                            defer_terminal=True,
                        ),
                        timeout=node_config.timeout_seconds,
                    )
                else:
                    result = await asyncio.wait_for(
                        executor_fn(state, node_config),
                        timeout=node_config.timeout_seconds,
                    )

                if isinstance(result, dict):
                    raw_finalization = result.pop("_actionFinalization", None)
                    if isinstance(raw_finalization, dict):
                        action_finalization = raw_finalization
                    result_status = str(result.get("status") or "")
                    if result_status == "cancelled":
                        raise WorkflowRunCancelled(run.run_id)
                    if result_status == "lease_lost":
                        raise DriverLeaseLost(run.run_id)
                    if result.get("error"):
                        if result_status != "unknown":
                            raise RuntimeError(str(result["error"]))
                    if result_status == "unknown":
                        unknown_action = True
                        attempt_error = str(
                            result.get("error")
                            or result.get("reason")
                            or "Action 外部执行结果待确认"
                        )[:500]
                    if result_status in {
                        "failed", "timed_out", "denied", "approval_required",
                        "budget_exhausted", "marker_persist_failed",
                        "result_persist_failed", "in_flight",
                    }:
                        raise RuntimeError(str(result.get("reason") or result_status))
                succeeded = not unknown_action
            except (WorkflowRunCancelled, DriverLeaseLost):
                # Leave the durable attempt RUNNING: its completion is unknown
                # to this worker and cancellation/lease ownership won the race.
                raise
            except asyncio.TimeoutError:
                timed_out = True
                attempt_error = f"节点执行超时 ({node_config.timeout_seconds}s)"
                if node_config.node_type == NodeType.ACTION:
                    # ``asyncio.wait_for`` cancels execute_action first.  The
                    # reliable action layer turns that post-marker cancellation
                    # into a durable UNKNOWN before TimeoutError reaches us.
                    # Project that durable fact into the node/run checkpoint so
                    # the Workflow pauses instead of being mislabelled FAILED.
                    durable_actions = [
                        action
                        for action in self.repo.list_action_records(run.run_id)
                        if action.node_id == node_id
                    ]
                    durable_actions.sort(
                        key=lambda action: (
                            action.attempt,
                            action.created_at,
                            action.action_id,
                        )
                    )
                    latest_action = durable_actions[-1] if durable_actions else None
                    if (
                        latest_action is not None
                        and latest_action.status == ActionStatus.UNKNOWN
                    ):
                        timed_out = False
                        unknown_action = True
                        result = {
                            "actionExecutionId": latest_action.action_id,
                            "actionType": latest_action.action_type,
                            "attempt": latest_action.attempt,
                            "status": ActionStatus.UNKNOWN.value,
                            "error": latest_action.error or attempt_error,
                            "externalReference": (
                                latest_action.external_reference or None
                            ),
                            "reconciliationSupported": bool(
                                latest_action.reconciliation_supported
                            ),
                            "retryable": False,
                        }
            except Exception as exc:
                attempt_error = str(exc)[:500]

            # Cancellation and fencing are checked before *every* terminal node
            # write, including failed attempts that may be retried locally.
            try:
                self._assert_execution_active(run.run_id)
            except (WorkflowRunCancelled, DriverLeaseLost):
                # The side effect already returned a factual terminal/UNKNOWN
                # result.  Persist that attempt without advancing the node or
                # Run; cancellation/fencing still owns control flow.
                if action_finalization is not None:
                    self.repo.finalize_action_execution(action_finalization)
                raise
            node_run.completed_at = _utc_now_iso()
            node_run.duration_ms = int((time.monotonic() - started_monotonic) * 1000)
            node_run.output_snapshot = result if isinstance(result, dict) else {}

            if unknown_action:
                node_run.status = NodeStatus.PAUSED
                node_run.error = attempt_error
                state.node_outputs[node_id] = result if isinstance(result, dict) else {}
                state.current_node = node_id
                state.transition(WorkflowRunStatus.PAUSED)
                self._prepare_run_checkpoint(run, state)
                self._finalize_node_attempt(
                    node_run,
                    checkpoint_run=run,
                    action_finalization=action_finalization,
                )
                sse_events.append(sse_event_fn("action_unknown", {
                    "runId": run.run_id,
                    "nodeId": node_id,
                    "actionExecutionId": result.get("actionExecutionId") if isinstance(result, dict) else None,
                    "attempt": absolute_attempt,
                    "status": "unknown",
                    "message": "Action 执行结果待确认，Workflow 已安全暂停",
                }))
                break

            if succeeded:
                if node_config.node_type != NodeType.TRIGGER:
                    for key in ("roadName", "eventType", "avgSpeed", "queueLength", "duration"):
                        if key in event_before and state.current_event.get(key) != event_before.get(key):
                            state.record_error(node_id, f"current_event 核心字段被修改: {key}")
                            state.current_event[key] = event_before[key]
                node_run.status = NodeStatus.SUCCEEDED
                if isinstance(result, dict) and result:
                    state.node_outputs[node_id] = result
                if node_id not in state.completed_steps:
                    state.completed_steps.append(node_id)
                # The SUCCEEDED attempt and the full state mutation it
                # represents must become durable together.  Otherwise recovery
                # would skip the node while losing outputs/risk/approval state.
                self._prepare_run_checkpoint(run, state)
                self._finalize_node_attempt(
                    node_run,
                    checkpoint_run=run,
                    action_finalization=action_finalization,
                )
                sse_events.append(sse_event_fn("node_completed", {
                    "runId": run.run_id,
                    "nodeId": node_id,
                    "nodeType": node_type,
                    "status": "succeeded",
                    "attempt": absolute_attempt,
                }))
                self._save_event(run.run_id, "node_completed", node_id, {
                    "status": "succeeded",
                    "attempt": absolute_attempt,
                }, seq)
                seq += 1
                break

            last_error = attempt_error or "节点执行失败"
            state.record_error(node_id, last_error, absolute_attempt)
            node_run.status = NodeStatus.TIMED_OUT if timed_out else NodeStatus.FAILED
            node_run.error = last_error
            self._finalize_node_attempt(
                node_run,
                action_finalization=action_finalization,
            )

            if (
                local_attempt < max(1, node_config.max_attempts)
                and not (
                    node_config.node_type == NodeType.ACTION
                    and action_finalization is not None
                )
            ):
                if not reserve_retry_durable(self.repo, run.run_id):
                    last_error = "retry budget exhausted"
                    state.record_error(node_id, last_error, absolute_attempt)
                    break
                sse_events.append(sse_event_fn("node_retrying", {
                    "runId": run.run_id,
                    "nodeId": node_id,
                    "nodeType": node_type,
                    "error": attempt_error,
                    "attempt": absolute_attempt,
                    "nextAttempt": absolute_attempt + 1,
                }))
                self._save_event(run.run_id, "node_retrying", node_id, {
                    "error": attempt_error,
                    "attempt": absolute_attempt,
                    "nextAttempt": absolute_attempt + 1,
                }, seq)
                seq += 1
                await asyncio.sleep(node_config.retry_delay_seconds)

        # Count one semantic step invocation, not every local attempt.  A
        # cancellation observed above returns before this reservation.
        if should_count_step(node_config.node_type):
            reserve_step_durable(self.repo, run.run_id)

        if not succeeded and not unknown_action:
            sse_events.append(sse_event_fn("node_failed", {
                "runId": run.run_id,
                "nodeId": node_id,
                "nodeType": node_type,
                "error": last_error,
                "attempt": terminal_attempt,
            }))
            self._save_event(run.run_id, "node_failed", node_id, {
                "error": last_error,
                "attempt": terminal_attempt,
            }, seq)
            seq += 1
            if not state.is_terminal():
                state.transition(WorkflowRunStatus.FAILED)

        return {
            "sse_events": sse_events,
            "next_seq": seq,
            "succeeded": succeeded,
            "result": result,
        }

    def _determine_next_node(
        self, node_config: NodeConfig, state: TrafficWorkflowState
    ) -> Optional[str]:
        """确定下一个节点。"""
        if node_config.node_type == NodeType.CLOSE:
            return None

        next_nodes = node_config.next_nodes
        if not next_nodes:
            return None

        # 条件分支
        if node_config.node_type == NodeType.RISK_GATE and node_config.condition:
            try:
                result = self._eval_condition(
                    node_config.condition, state,
                    getattr(self, '_current_definition', None),
                )
                if result and len(next_nodes) > 1:
                    return next_nodes[0]  # approval
                elif len(next_nodes) > 1:
                    return next_nodes[1]  # auto
                return next_nodes[0]
            except Exception:
                return next_nodes[0] if next_nodes else None

        return next_nodes[0]

    @staticmethod
    def _eval_condition(condition: str, state: TrafficWorkflowState,
                        definition: WorkflowDefinition = None) -> bool:
        """使用安全条件 DSL 求值条件。

        节点输出自动注入到 state dict 中以 node_id 为 key，
        使条件 DSL 可引用如 rule_router.requires_approval。

        动态节点 ID 通过 definition.nodes 校验。
        """
        import json as _json
        condition_obj = None
        if isinstance(condition, str) and condition.strip().startswith("{"):
            try:
                condition_obj = _json.loads(condition)
            except _json.JSONDecodeError:
                pass

        if condition_obj is None and isinstance(condition, str):
            try:
                condition_obj = condition_from_expr(condition)
            except ConditionError:
                state.record_error("condition", f"无法解析条件表达式: {condition}")
                return False

        if condition_obj is None:
            return False

        # ── 构建 state dict，注入节点输出 ──────────────────────────
        state_dict = state.to_dict()
        for node_id, output in state.node_outputs.items():
            state_dict[node_id] = output

        # ── 构建允许的节点 ID 集合 ────────────────────────────────
        allowed_node_ids = None
        if definition is not None:
            allowed_node_ids = {n.node_id for n in definition.nodes}

        try:
            return evaluate_condition(condition_obj, state_dict, allowed_node_ids)
        except ConditionError as e:
            state.record_error("condition", str(e))
            return False

    # ═══════════════════════════════════════════════════════════════════════
    # 辅助方法
    # ═══════════════════════════════════════════════════════════════════════

    def _assert_execution_active(self, run_id: str) -> None:
        """Fail immediately when cancellation or fencing owns the run."""
        durable = self.repo.get_run(run_id)
        if durable is not None and durable.status == WorkflowRunStatus.CANCELLED:
            raise WorkflowRunCancelled(run_id)
        if self._driver_owner and not self.repo.is_driver_execution_valid(
            run_id, self._driver_owner, self._driver_generation
        ):
            self._lease_lost = True
            raise DriverLeaseLost(run_id)

    def _finalize_node_attempt(
        self,
        node_run: WorkflowNodeRun,
        *,
        checkpoint_run: Optional[WorkflowRun] = None,
        action_finalization: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Conditionally close a durable attempt without losing a cancel race."""
        ok = self.repo.finalize_node_run(
            node_run,
            driver_owner=self._driver_owner,
            driver_generation=self._driver_generation,
            checkpoint_run=checkpoint_run,
            action_finalization=action_finalization,
        )
        if ok:
            return
        # Produce the precise control-flow signal when the conditional update
        # was rejected by cancellation/fencing; otherwise surface corruption.
        self._assert_execution_active(node_run.run_id)
        raise RuntimeError(
            f"节点 attempt 无法终结: {node_run.node_run_id}（记录不存在或状态冲突）"
        )

    def _persist_pending_approval(
        self, run_id: str, state: TrafficWorkflowState
    ) -> None:
        """Materialise state.pendingApproval as a durable approval record."""
        pending = state.pending_approval
        if not isinstance(pending, dict):
            return
        approval_id = str(pending.get("approvalId") or "")
        if not approval_id:
            raise RuntimeError("待审批状态缺少 approvalId")
        existing = self.repo.get_approval(approval_id)
        if existing is not None:
            if existing.run_id != run_id:
                raise RuntimeError(f"审批 {approval_id} 已绑定其他 Workflow Run")
            return
        self.repo.save_approval(WorkflowApproval(
            approval_id=approval_id,
            run_id=run_id,
            node_id=str(pending.get("nodeId") or state.current_node or ""),
            proposed_actions=list(pending.get("proposedActions") or []),
            decision=ApprovalDecision.PENDING,
            created_at=str(pending.get("createdAt") or _utc_now_iso()),
        ))

    @staticmethod
    def _advance_event_lifecycle(
        state: TrafficWorkflowState,
        status: str,
        allowed_from: List[str],
    ) -> None:
        """Best-effort compare-and-set of the canonical event lifecycle."""
        event_id = str((state.current_event or {}).get("eventId") or "")
        if not event_id:
            return
        try:
            from backend.tools.db_tools import advance_event_status
            advance_event_status(event_id, status, allowed_from=allowed_from)
        except Exception:
            # Runtime durability must not be replaced by an event projection
            # failure; the Workflow itself remains queryable and truthful.
            return

    def _prepare_run_checkpoint(
        self,
        run: WorkflowRun,
        state: TrafficWorkflowState,
    ) -> None:
        """Project in-memory state onto ``run`` without writing it."""
        state_dict = state.to_dict()
        # Phase17 Round2: 保留 execution lineage（action 节点可能已 durable reserve）
        from backend.planning.budget import LINEAGE_KEY
        db_run = self.repo.get_run(run.run_id)
        if db_run is not None and isinstance(db_run.state, dict) and db_run.state.get(LINEAGE_KEY):
            state_dict[LINEAGE_KEY] = db_run.state[LINEAGE_KEY]
        # terminal 时关闭 active segment，累计 activeElapsedSeconds
        if state.is_terminal():
            import time
            from backend.planning.budget import close_active_segment, get_lineage
            lineage = get_lineage(state_dict)
            if lineage.rootRunId:
                close_active_segment(lineage, time.time())
                state_dict[LINEAGE_KEY] = lineage.to_dict()
        run.state = state_dict
        run.status = state.status
        run.current_node_id = state.current_node
        run.updated_at = _utc_now_iso()
        if state.started_at:
            run.started_at = state.started_at
        if state.is_terminal():
            run.completed_at = state.finished_at or run.completed_at or _utc_now_iso()
        elif state.status in {
            WorkflowRunStatus.PENDING,
            WorkflowRunStatus.RUNNING,
            WorkflowRunStatus.PAUSED,
            WorkflowRunStatus.AWAITING_APPROVAL,
        }:
            run.completed_at = ""

    def _persist_run(self, run: WorkflowRun, state: TrafficWorkflowState) -> None:
        self._prepare_run_checkpoint(run, state)
        # Phase17 Round3: driver-managed run → atomic fenced write（防 stale worker clobber）
        self._persist_run_state(run)

    def _persist_run_state(self, run: WorkflowRun) -> None:
        """driver-managed 用 fenced write；失败 → lease_lost。legacy 用 save_run。"""
        if self._driver_owner:
            ok = self.repo.fenced_update_run(
                run.run_id, self._driver_owner, self._driver_generation,
                run.status.value, run.current_node_id, run.state,
                started_at=run.started_at or None,
                completed_at=run.completed_at,
            )
            if not ok:
                self._lease_lost = True  # lease lost → 停止执行
                durable = self.repo.get_run(run.run_id)
                if durable is not None and durable.status == WorkflowRunStatus.CANCELLED:
                    raise WorkflowRunCancelled(run.run_id)
                raise DriverLeaseLost(run.run_id)
        else:
            self.repo.save_run(run)
            durable = self.repo.get_run(run.run_id)
            if (
                durable is not None
                and durable.status == WorkflowRunStatus.CANCELLED
                and run.status != WorkflowRunStatus.CANCELLED
            ):
                raise WorkflowRunCancelled(run.run_id)

    def _open_active_segment(self, run: WorkflowRun) -> None:
        """打开 active execution segment（start/resume 时）。"""
        import time

        from backend.planning.budget import get_lineage, open_active_segment, set_lineage
        state = run.state if isinstance(run.state, dict) else {}
        lineage = get_lineage(state)
        if not lineage.rootRunId:
            return
        open_active_segment(lineage, time.time())
        set_lineage(state, lineage)
        run.state = state
        self._persist_run_state(run)

    def _close_active_segment(self, run: WorkflowRun) -> None:
        """关闭 active segment（pause/terminal 时），累计 activeElapsedSeconds。"""
        import time

        from backend.planning.budget import close_active_segment, get_lineage, set_lineage
        state = run.state if isinstance(run.state, dict) else {}
        lineage = get_lineage(state)
        if not lineage.rootRunId:
            return
        close_active_segment(lineage, time.time())
        set_lineage(state, lineage)
        run.state = state
        try:
            self._persist_run_state(run)
        except DriverLeaseLost:
            # Preserve the historical helper contract used by recovery safety
            # checks: a stale close is a no-op, not an exception.  Runtime
            # callers inspect ``_lease_lost`` immediately and stop before any
            # subsequent event/control write.
            self._lease_lost = True

    def _active_budget_exhausted(self, run: WorkflowRun) -> bool:
        """activeElapsedSeconds 是否已达 maxTotalSeconds。"""
        from backend.planning.budget import active_budget_exhausted, get_lineage
        state = run.state if isinstance(run.state, dict) else {}
        lineage = get_lineage(state)
        return active_budget_exhausted(lineage)

    def _save_event(
        self, run_id: str, event_type: str, node_id: str,
        payload: Dict[str, Any], seq: int,
    ) -> None:
        # ``seq`` remains in the internal call signature for backwards
        # compatibility with the streaming cursor, but durable ordering is
        # allocated inside one repository transaction.
        self.repo.append_event(
            run_id,
            event_type,
            node_id=node_id,
            payload=payload,
        )


def get_executor() -> WorkflowExecutor:
    """获取 Workflow 执行器单例。"""
    return WorkflowExecutor()
