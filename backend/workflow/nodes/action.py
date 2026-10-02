"""
action 节点 — 外部动作执行。

执行经批准的外部动作（通知、信号调整、派单等）。

幂等性保证：
  - 使用 idempotency_key = {runId}:{nodeId}:{actionType}
  - 重复 resume 或 retry 不重复执行已成功动作
  - 通过 WorkflowActionRecord 表做幂等检查

未经 human_approval 批准不得执行 action 节点。
"""

import asyncio
from typing import Any, Dict

from backend.workflow.models import (
    ActionStatus,
    NodeConfig,
    WorkflowActionRecord,
    compute_action_idempotency_key,
    generate_action_id,
)
from backend.workflow.state import TrafficWorkflowState
from backend.agent.tool_policy import (
    ToolExecutionStatus,
    classify_tool_result,
    enforce_tool_request,
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
    return _ACTION_TYPE_ALIASES.get(action_type, action_type)


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


async def execute_action(
    state: TrafficWorkflowState, config: NodeConfig, repository=None,
    driver_owner: str = "", driver_generation: int = 0,
) -> Dict[str, Any]:
    """执行外部动作。

    执行前检查：
      1. 是否已经过审批（如有 approval 节点）
      2. 幂等键是否已存在成功记录
      3. driver lease ownership（fencing，driver-managed 时）

    Args:
        state: 工作流状态
        config: 节点配置
          - config.action_type: 动作类型（"notify_wechat", "adjust_signal" 等）
          - config.action_params: 动作参数
        repository: Workflow 持久化仓库（用于幂等检查）
        driver_owner / driver_generation: driver context（fencing 校验）

    Returns:
        执行结果
    """
    action_type = config.config.get("action_type", "")
    if not action_type:
        return {"error": "action 节点缺少 action_type 配置"}
    if (state.current_event or {}).get("actionExecutionAllowed") is False:
        state.add_audit_event("action_replay_blocked", config.node_id, {
            "actionType": action_type,
            "reason": "replay run is analysis-only",
        })
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
            "action_type": action_type,
            "status": "lease_lost",
            "executed": False,
            "reason": "driver lease lost",
        }

    def _cancelled_result(reason: str) -> Dict[str, Any]:
        return {
            "action_type": action_type,
            "status": "cancelled",
            "executed": False,
            "reason": reason,
        }

    def _finalize_marker_cancelled() -> None:
        """EXECUTING marker 已 persist，但 cancel 发生在真正 dispatch 前 → 终结为 known-not-dispatched。

        优先写 FAILED / cancelled_before_dispatch；terminal marker 更新失败则保守保留 EXECUTING
        （→ recovery human review / UNKNOWN），但绝不 dispatch。
        """
        if not repository:
            return
        record.status = ActionStatus.FAILED
        record.error = "cancelled_before_dispatch"
        record.result = {"cancelled": True, "dispatched": False}
        record.completed_at = record.created_at
        try:
            repository.save_action_record(record)
        except Exception:
            pass

    # ── ToolPolicy 门禁（Section 11/12/13）：外部动作必须先通过 ToolPolicy ──
    # 未知工具 fail-closed；高风险工具未批准则阻止执行并返回 approval_required。
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
            "actionType": action_type,
            "workflowRunId": state.workflow_run_id,
            "nodeId": config.node_id,
            "executed": False,
        }
        event_type = (
            "tool_denied"
            if _policy["status"] == ToolExecutionStatus.DENIED.value
            else "tool_approval_required"
        )
        state.add_audit_event(event_type, config.node_id, audit_payload)
        return {
            "action_type": action_type,
            "status": _policy["status"],
            "executed": False,
            "reason": _policy["reason"],
            "approvalRequired": _policy["approvalRequired"],
        }

    # 检查是否有待审批但未批准的审批
    pending = state.pending_approval
    if pending:
        return {
            "error": "存在未处理的审批，不能执行外部动作",
            "approval_id": pending.get("approvalId"),
        }

    # ── 优先使用审批后的 edited_actions，否则用节点配置 ──────────
    # Phase18 V2：business params 来自 compiler 归一化的 config.action_params，
    # 不由 approved_actions 覆盖（approved_actions 仅作审批绑定，不含 params）。
    action_params = config.config.get("action_params", {})
    identity_version = config.config.get("approval_identity_version", 1)
    if identity_version < 2 and state.approved_actions:
        # 查找匹配当前 action_type 的已批准动作
        for approved in state.approved_actions:
            if isinstance(approved, dict) and approved.get("actionType") == action_type:
                action_params = approved.get("params", approved.get("action_params", approved))
                break
            # Phase 13: 结构化 proposal 直接使用自身作为 params
            if isinstance(approved, dict) and approved.get("actionType"):
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

    # 幂等键
    idempotency_key = compute_action_idempotency_key(
        state.workflow_run_id, config.node_id, action_type
    )

    # 幂等检查（如果提供了 repository）
    if repository:
        try:
            existing = repository.get_action_record_by_idempotency_key(idempotency_key)
        except Exception as e:
            # 幂等检查失败 → fail-closed，不 dispatch
            state.add_audit_event("action_idempotency_check_failed", config.node_id, {
                "actionType": action_type, "idempotencyKey": idempotency_key,
                "reason": f"idempotency check failed: {str(e)[:200]}",
            })
            return {
                "action_type": action_type,
                "status": "failed",
                "executed": False,
                "reason": f"idempotency check failed: {str(e)[:200]}",
            }
        if existing is not None:
            if existing.status == ActionStatus.SUCCEEDED:
                state.add_audit_event("action_idempotent_skip", config.node_id, {
                    "actionType": action_type,
                    "idempotencyKey": idempotency_key,
                    "reason": "已成功执行，幂等跳过",
                })
                return {
                    "action_type": action_type,
                    "status": "skipped",
                    "reason": "idempotent_skip",
                    "previous_result": existing.result,
                }
            if existing.status == ActionStatus.EXECUTING:
                # 已有 in-flight/unknown attempt → no second dispatch（fail closed）
                state.add_audit_event("action_inflight_conflict", config.node_id, {
                    "actionType": action_type, "idempotencyKey": idempotency_key,
                    "reason": "已有 EXECUTING attempt，禁止二次 dispatch",
                })
                return {
                    "action_type": action_type,
                    "status": "in_flight",
                    "executed": False,
                    "reason": "existing EXECUTING attempt",
                    "error": "existing EXECUTING attempt",  # 使 executor 判为 node 失败（不 SUCCEEDED）
                }

    # 创建动作记录
    action_id = generate_action_id()
    record = WorkflowActionRecord(
        action_id=action_id,
        run_id=state.workflow_run_id,
        node_id=config.node_id,
        action_type=action_type,
        idempotency_key=idempotency_key,
        params=action_params,
        status=ActionStatus.EXECUTING,
    )

    # ── C1: budget/dispatch 前 gate（identity + lease 未过期 + 非 CANCELLED）──
    _gate = _driver_gate()
    if _gate == "cancelled":
        state.add_audit_event("action_cancelled_before_dispatch", config.node_id, {"actionType": action_type})
        return _cancelled_result("run cancelled before action")
    if _gate == "lease_lost":
        state.add_audit_event("lease_lost", config.node_id, {"actionType": action_type})
        return _lease_lost_result()

    # ── Phase17 Round2: budget durable reservation BEFORE dispatch ──
    # ToolPolicy ALLOW 后、真正 dispatch 前：check → increment → persist → dispatch。
    # 若 persist 失败或 budget 耗尽 → fail-closed，不 dispatch。
    if repository:
        from backend.planning.budget import reserve_tool_call_durable
        if not reserve_tool_call_durable(repository, state.workflow_run_id):
            state.add_audit_event("budget_exhausted", config.node_id, {
                "actionType": action_type, "reason": "tool budget exhausted",
            })
            return {
                "action_type": action_type,
                "status": "budget_exhausted",
                "executed": False,
                "reason": "tool budget exhausted",
            }

    # ── Phase17 Round3: persist EXECUTING record BEFORE dispatch ──
    # action_id 即 dispatchAttemptId；EXECUTING = dispatch_started marker。
    # marker 必须 durable persist；失败 → 不 dispatch（fail-closed）。
    if repository:
        try:
            repository.save_action_record(record)
            marker = repository.get_action_record_by_idempotency_key(idempotency_key)
            if marker is None:
                raise RuntimeError("dispatch marker read-after-write failed")
            if marker.action_id != action_id:
                # A concurrent execution already claimed this idempotency key.
                # The loser must not dispatch.
                state.add_audit_event("action_idempotent_db_protect", config.node_id, {
                    "actionType": action_type,
                    "idempotencyKey": idempotency_key,
                    "reason": f"existing {marker.status.value} attempt owns marker",
                })
                return {
                    "action_id": marker.action_id,
                    "action_type": action_type,
                    "status": "skipped" if marker.status == ActionStatus.SUCCEEDED else "in_flight",
                    "executed": False,
                    "reason": "idempotent_db_protect",
                    **({"error": "existing EXECUTING attempt"}
                       if marker.status != ActionStatus.SUCCEEDED else {}),
                }
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

    # ── C2: external dispatch 前 re-check（durable != CANCELLED 且 lease 未过期）──
    _gate = _driver_gate()
    if _gate == "cancelled":
        # EXECUTING marker 已 persist，但 cancel 发生在真正 dispatch 前 → 终结为 known-not-dispatched
        state.add_audit_event("action_cancelled_before_dispatch", config.node_id, {"actionType": action_type})
        _finalize_marker_cancelled()
        return _cancelled_result("run cancelled before dispatch")
    if _gate == "lease_lost":
        state.add_audit_event("lease_lost", config.node_id, {"actionType": action_type})
        return _lease_lost_result()

    # 执行具体动作
    result_data: Dict[str, Any] = {}
    error = ""
    status = ActionStatus.SUCCEEDED

    try:
        result_data = await _dispatch_action(action_type, action_params, state)
    except Exception as e:
        error = str(e)[:500]
        status = ActionStatus.FAILED
        result_data = {}
        state.record_error(config.node_id, f"action 执行失败: {error}")

    # ── 工具失败语义（Section 17）：result 明确失败时不得标记 SUCCEEDED ──
    # _dispatch_action 对 notify/save 失败返回 {"sent": False}/{"saved": False}
    # 而非抛异常，因此必须在执行后重新判定，防止失败被记录成成功。
    if status == ActionStatus.SUCCEEDED and classify_tool_result(result_data) == ToolExecutionStatus.FAILURE:
        status = ActionStatus.FAILED
        if isinstance(result_data, dict):
            error = str(result_data.get("error", "工具返回失败结果"))[:500]
        else:
            error = "工具返回失败结果"
        state.record_error(config.node_id, f"action 执行失败: {error}")

    # 更新动作记录
    record.status = status
    record.result = result_data
    record.error = error
    record.completed_at = record.created_at  # 简化时间戳

    # 持久化（如果提供了 repository）
    # DB 层有 UNIQUE(idempotency_key) 约束，提供数据库级别的幂等保护
    if repository:
        try:
            repository.save_action_record(record)
        except Exception as e:
            # 检查是否为 IntegrityError（幂等键冲突）
            err_str = str(e).lower()
            if "unique" in err_str or "integrity" in err_str:
                # 数据库级别幂等保护：重新读取已有记录
                try:
                    existing = repository.get_action_record_by_idempotency_key(
                        idempotency_key
                    )
                    if existing:
                        state.add_audit_event("action_idempotent_db_protect", config.node_id, {
                            "actionType": action_type,
                            "idempotencyKey": idempotency_key,
                            "reason": "DB UNIQUE 约束触发，幂等跳过",
                        })
                        return {
                            "action_id": existing.action_id,
                            "action_type": action_type,
                            "status": "skipped",
                            "reason": "idempotent_db_protect",
                            "previous_result": existing.result,
                        }
                except Exception:
                    pass
                # UNIQUE 冲突但读取失败 → 继续（幂等键仍存在，不重复执行）
            else:
                # 其他持久化错误 → fail-safe：external 已发生但 durable terminal result 丢失。
                # EXECUTING marker 仍在 DB，HIGH_RISK 重启/恢复会识别 UNKNOWN_OUTCOME。
                state.add_audit_event("action_result_persist_failed", config.node_id, {
                    "actionType": action_type, "reason": str(e)[:200],
                })
                state.record_error(config.node_id, f"action result 持久化失败: {str(e)[:200]}")
                return {
                    "action_id": action_id,
                    "action_type": action_type,
                    "status": "result_persist_failed",
                    "error": f"action result 持久化失败: {str(e)[:200]}",
                }

    # 跟踪
    state.action_record_ids.append(action_id)
    if isinstance(state.action_results, dict):
        state.action_results[action_type] = {
            "actionId": action_id,
            "status": status.value,
            "result": result_data,
            "error": error,
        }

    state.add_audit_event("action_executed", config.node_id, {
        "actionType": action_type,
        "actionId": action_id,
        "status": status.value,
        "idempotencyKey": idempotency_key,
    })

    return {
        "action_id": action_id,
        "action_type": action_type,
        "status": status.value,
        "result": result_data,
        "error": error,
    }


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
