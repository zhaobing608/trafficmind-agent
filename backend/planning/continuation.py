"""
Planning Continuation Coordinator — Phase 17 Round 2

thin control-plane：
  safe-boundary result → Observation → Decision → existing retry OR revision transaction
  → after commit → WorkflowExecutor execute child

禁止：执行 node / 调用 Tool / 实现 approval / 实现 retry runtime。不是第二 runtime。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from backend.planning.budget import (
    ExecutionLineage,
    get_lineage,
    inherit_lineage,
    new_lineage,
    reserve_replan,
    set_lineage,
)
from backend.planning.models import Plan
from backend.planning.observation import (
    Observation,
    ObservationScope,
    ObservationSource,
    ObservationStatus,
    ObservationType,
    generate_observation_id,
)
from backend.planning.critic import (
    CriticContext,
    CriticRecommendation,
    build_critic_invocation_key,
    invoke_critic_sync,
)
from backend.planning.replan_decision import (
    DecisionResult,
    ReplanDecision,
    ReplanDecisionEngine,
    classify_observation,
)
from backend.planning.replanner import build_revision, build_semantic_revision
from backend.planning.revision import (
    build_child_state,
    compute_continuation_key,
    deterministic_child_run_id,
    plan_to_child_definition,
    validate_carried_refs,
)
from backend.planning.validator import has_errors, validate_plan
from backend.workflow.models import WorkflowRun, WorkflowRunStatus
from backend.workflow.repository import SQLiteWorkflowRepository


class PlanningContinuationCoordinator:
    """control plane：串 Observation → Decision → Replanner → child。"""

    def __init__(self, repository: Optional[SQLiteWorkflowRepository] = None, critic_client: Any = None,
                 semantic_replan_enabled: Optional[bool] = None,
                 grounded_decision_context_enabled: Optional[bool] = None):
        self._repo = repository or SQLiteWorkflowRepository()
        self._engine = ReplanDecisionEngine()
        self._critic_client = critic_client  # None → critic disabled（Phase17 语义）
        # None → follow durable Plan.semanticReplanEnabled；False → explicit kill-switch（测试）
        self._semantic_replan_enabled = semantic_replan_enabled
        # Phase19 R2：grounded 开关。None/True → follow Plan.groundedDecisionContextEnabled；
        # False → process kill-switch（force off）。Plan flag=false 恒为 off。
        self._grounded_decision_context_enabled = grounded_decision_context_enabled

    def _grounded_enabled(self, plan: Plan) -> bool:
        """grounded DecisionContext 是否启用（kill-switch 语义同 semantic_replan_enabled）。

        Plan flag false → off；process kill-switch false → force off；None/true → follow Plan flag。
        """
        if self._grounded_decision_context_enabled is False:
            return False
        return bool(getattr(plan, "groundedDecisionContextEnabled", False))

    @property
    def repo(self) -> SQLiteWorkflowRepository:
        return self._repo

    # ── observation 持久化 ───────────────────────────────────────────

    def persist_observation(self, observation: Observation) -> None:
        """写 observation_recorded workflow_event（durable audit log）。"""
        self._repo.append_event(
            observation.runId,
            "observation_recorded",
            payload=observation.to_dict(),
        )

    # ── 幂等 / lineage 辅助 ─────────────────────────────────────────

    def _load_plan_from_run(self, run: WorkflowRun) -> Optional[Plan]:
        """加载 run 绑定的 plan。

        - 优先 exact version snapshot（child run 绑定到版本化 plan）。
        - versioned child（version>1）snapshot 缺失/malformed → fail-closed（禁止 fallback v1）。
        - legacy base v1 run（无 snapshot）→ fallback base definition metadata.plan。
        """
        ver = self._repo.get_definition_version(run.definition_id, run.version)
        if ver is not None:
            dj = ver.definition_json if isinstance(ver.definition_json, dict) else {}
            metadata = dj.get("metadata", {})
            if not metadata:
                metadata = dj
            plan_raw = metadata.get("plan")
            if not plan_raw:
                # versioned snapshot 存在但 plan 缺失 → fail-closed
                return None
            return self._parse_plan_raw(plan_raw)

        # 无 version snapshot：仅 legacy base v1 允许 fallback
        if run.version > 1:
            # versioned child 但 snapshot 缺失 → fail-closed，禁止用旧 v1 plan 重规划
            return None
        definition = self._repo.get_definition(run.definition_id)
        if definition is None:
            return None
        return self._parse_plan_raw(definition.metadata.get("plan"))

    def _parse_plan_raw(self, plan_raw: Any) -> Optional[Plan]:
        if not plan_raw:
            return None
        if isinstance(plan_raw, str):
            import json
            try:
                plan_raw = json.loads(plan_raw)
            except Exception:
                return None
        try:
            return Plan.from_dict(plan_raw)
        except Exception:
            return None

    def _completed_result_refs(self, run_id: str) -> Dict[str, str]:
        """从 node_runs 提取 completed step 的 {stepId: resultRef}。"""
        refs: Dict[str, str] = {}
        for nr in self._repo.get_node_runs(run_id):
            if nr.status.value == "succeeded":
                refs[nr.node_id] = f"{run_id}:{nr.node_id}"
        return refs

    def _get_or_init_lineage(self, run: WorkflowRun) -> ExecutionLineage:
        state = run.state if isinstance(run.state, dict) else {}
        lineage = get_lineage(state)
        if not lineage.rootRunId:
            lineage = new_lineage(run.run_id)
            set_lineage(state, lineage)
            run.state = state
            self._repo.save_run(run)
        return lineage

    # ── Phase18 Round2: Critic（bounded semantic review）────────────────

    def _critic_for(self, observation: Observation, lineage: ExecutionLineage,
                    plan: Plan, run: WorkflowRun) -> tuple:
        """若 semantic_review 且 critic_client/budget 可用，调用 critic。

        Returns: (CriticRecommendation | None, fallback_reason | None)。
        - critic disabled / ineligible / unavailable / timeout / invalid / interrupted /
          budget 不可用 → (None, reason)，最终 decision 用 Phase17 deterministic（I2）。
        - Critic 只返回 recommendation，绝不 build revision / child / execute tool。
        """
        client = self._critic_client
        if client is None:
            # production wiring：从现有 config 解析 planning LLM client（无 key → None）
            from backend.planning.llm_client import get_planning_llm_client_optional
            client = get_planning_llm_client_optional()
        if client is None:
            return None, None
        if classify_observation(observation, lineage) != "semantic_review":
            return None, None

        # Phase19 R2：grounded assembly（flag + kill-switch 双门，claim 之前）。
        # assembler 本身 0 provider / 0 claim / 0 持久化；失败 → grounded_ctx=None，
        # 后续走 Phase18-equivalent legacy input（§9：绝不 fail workflow）。
        grounded_ctx = None
        if self._grounded_enabled(plan):
            try:
                from backend.planning.context_assembler import assemble_or_empty
                from backend.planning.decision_context import DecisionType
                dctx = assemble_or_empty(self._repo, run, plan, observation,
                                         DecisionType.CRITIC, lineage=lineage)
                if not dctx.isEmpty:
                    grounded_ctx = dctx
            except Exception:
                grounded_ctx = None

        state = run.state if isinstance(run.state, dict) else {}
        root_run_id = lineage.rootRunId or run.run_id
        # Final Identity Rule：decision identity 只由 durable Plan flag 决定
        # （kill-switch 不参与）：flag=off 恒为 ""（legacy 命名空间不变）；
        # flag=on → 真实 stepId（enriched grounded 命名空间）
        failed_step_id = self._observation_prompt_view(observation, plan)["stepId"]
        key = build_critic_invocation_key(
            root_run_id, run.run_id, plan.version, observation.type.value, failed_step_id
        )

        claim = self._repo.claim_critic_invocation_tx(run.run_id, key)
        result = claim.get("result")
        if result == "already_completed":
            rec = claim.get("recommendation", {}) or {}
            return CriticRecommendation(
                recommendation=rec.get("recommendation", "replan"),
                confidence=float(rec.get("confidence", 0.0) or 0.0),
                reasonSummary=rec.get("reasonSummary", ""),
                semanticFailureType=rec.get("semanticFailureType", ""),
                evidenceGaps=list(rec.get("evidenceGaps", [])),
                unresolvedRisks=list(rec.get("unresolvedRisks", [])),
            ), None
        if result == "already_started":
            return None, "interrupted"
        if result == "budget_exhausted":
            return None, "budget_exhausted"
        if result != "claimed":
            return None, None

        # claimed → invoke provider（sync），失败 → deterministic fallback。
        # 每个 Critic decision 最多 1 次 provider：grounded 调用失败后
        # 禁止再做第二次 legacy provider call（§9）。
        try:
            if grounded_ctx is not None:
                from backend.planning.critic_prompts import build_grounded_critic_messages
                system, user = build_grounded_critic_messages(grounded_ctx)
                data, _usage, _attempts = client.call_structured_json_sync(system, user)
                rec = CriticRecommendation.from_dict_strict(data)
            else:
                ctx = self._build_critic_context(observation, plan, run, lineage)
                rec = invoke_critic_sync(client, ctx)
            self._repo.complete_critic_invocation_tx(run.run_id, key, rec.to_dict())
            return rec, None
        except Exception as e:
            return None, str(e)[:200]

    def _build_critic_context(self, observation: Observation, plan: Plan,
                              run: WorkflowRun, lineage: ExecutionLineage) -> CriticContext:
        state = run.state if isinstance(run.state, dict) else {}
        # legacy builder 恒消费 Phase18 冻结投影：flag=true 时本 builder 仅作为
        # grounded assembly 失败的 degrade 输入，必须与 Phase18 字节等价（§9）。
        view = observation.to_phase18_prompt_view()
        return CriticContext(
            goal=plan.goal,
            goalType=plan.goalType.value,
            planSummary=[
                {"stepId": s.stepId, "stepType": s.stepType.value, "objective": s.objective}
                for s in plan.steps
            ],
            planVersion=plan.version,
            completedStepIds=[nr.node_id for nr in self._repo.get_node_runs(run.run_id)
                               if nr.status.value == "succeeded"],
            currentStep={"stepId": view["stepId"]},
            # key 顺序即序列化顺序（json.dumps 无 sort_keys）——不得调整
            observation={"type": view["type"], "status": view["status"],
                         "failureReason": view["failureReason"], "failureCode": view["failureCode"]},
            budgetSummary={"usage": lineage.budgetUsage.to_dict(), "limits": lineage.budgetLimits.to_dict()},
            loopGuardSummary=dict(lineage.loopGuard),
            rejectionConstraints=list(lineage.rejectionConstraints),
            policyDenyConstraints=list(lineage.policyDenyConstraints),
            evidenceRefs=list(view["evidenceRefs"]),
            trajectorySummary={},
        )

    # ── explicit replan ─────────────────────────────────────────────

    def explicit_replan(self, run_id: str, user_goal: str = "") -> Dict[str, Any]:
        """显式 deterministic replan。幂等（parent.replannedToRunId 为第一道防线）。"""
        parent = self._repo.get_run(run_id)
        if parent is None:
            return {"error": f"Run '{run_id}' 不存在"}
        parent_state = parent.state if isinstance(parent.state, dict) else {}

        # 幂等第一道防线：已 replan → 返回既有 child
        if parent_state.get("replannedToRunId"):
            return {"childRunId": parent_state["replannedToRunId"], "alreadyReplanned": True}

        plan = self._load_plan_from_run(parent)
        if plan is None:
            return {"error": "无法从 definition 加载 Plan"}

        lineage = self._get_or_init_lineage(parent)

        # 构建 observation（从 parent 状态）
        observation = self._build_observation(parent, plan, lineage)

        # Phase18 Round2：critic（semantic_review 且 critic_client/budget 可用时）
        critic, _critic_fallback = self._critic_for(observation, lineage, plan, parent)

        # decision
        decision = self._engine.decide(observation, lineage, critic)
        if decision.decision != ReplanDecision.REPLAN:
            self.persist_observation(observation)
            return {"error": f"decision={decision.decision.value}, 不 replan"}

        # Phase18 Extension：semantic replan（失败 fallback deterministic）
        suffix = self._try_semantic_replan(parent, plan, lineage, observation)
        return self._perform_replan(parent, plan, lineage, observation, suffix_steps=suffix)

    def auto_continue(self, run_id: str) -> Dict[str, Any]:
        """自动 continuation（machine failure 在 safe terminal boundary）。"""
        run = self._repo.get_run(run_id)
        if run is None:
            return {"decision": "no_replan", "reason": "run not found"}
        # 审批拒绝 → 不自动（显式 /replan）
        if run.status == WorkflowRunStatus.REJECTED:
            return {"decision": "no_replan", "reason": "approval_rejected"}
        # 仅 machine failure（FAILED）触发
        if run.status != WorkflowRunStatus.FAILED:
            return {"decision": "no_replan", "reason": f"status={run.status.value}"}

        plan = self._load_plan_from_run(run)
        if plan is None:
            return {"decision": "no_replan", "reason": "no plan"}

        lineage = self._get_or_init_lineage(run)
        observation = self._build_observation(run, plan, lineage)
        critic, _critic_fallback = self._critic_for(observation, lineage, plan, run)
        decision = self._engine.decide(observation, lineage, critic)

        if decision.decision == ReplanDecision.REPLAN:
            suffix = self._try_semantic_replan(run, plan, lineage, observation)
            return self._perform_replan(run, plan, lineage, observation, suffix_steps=suffix)

        # 非 REPLAN 决策（DENY/ABORT/ESCALATE/NO_REPLAN）→ 只记录，不自动 replan
        self.persist_observation(observation)
        return {"decision": decision.decision.value, "reason": decision.reason}

    def _build_observation(self, parent: WorkflowRun, plan: Plan, lineage: ExecutionLineage) -> Observation:
        """根据 parent 状态构造 observation。

        Phase19 R1：填充 stepId / failureCode / failureReason / output /
        evidenceRefs / metadata.nodeId —— 这些证据本就在 durable 层，
        Phase18 只是在本函数里丢弃了它们（失败 node_run 已在局部作用域内）。

        富字段**不改变**任何既有行为：所有会泄漏到 Phase18 prompt 或
        idempotency key 的读取点，都改为消费 `_observation_prompt_view()`，
        flag 关闭时返回冻结的 legacy 字面值。
        """
        parent_state = parent.state if isinstance(parent.state, dict) else {}
        failed_nr = None
        if parent.status == WorkflowRunStatus.REJECTED:
            typ = ObservationType.APPROVAL_REJECTED
            status = ObservationStatus.APPROVAL_REJECTED
        else:
            # 找失败 node
            typ, status = ObservationType.NODE_FAILED, ObservationStatus.FAILURE
            for nr in self._repo.get_node_runs(parent.run_id):
                if nr.status.value in ("failed", "timed_out"):
                    typ = ObservationType.TOOL_FAILED if nr.node_type.value == "action" else ObservationType.NODE_FAILED
                    status = ObservationStatus.FAILURE
                    failed_nr = nr
                    break
        return Observation(
            observationId=generate_observation_id(parent.run_id),
            planId=plan.planId,
            planVersion=plan.version,
            runId=parent.run_id,
            type=typ,
            status=status,
            scope=ObservationScope.RUN,
            source=ObservationSource.SYSTEM,
            **self._observation_evidence(parent, parent_state, typ, failed_nr),
        )

    def _observation_evidence(self, parent: WorkflowRun, parent_state: Dict[str, Any],
                              typ: ObservationType, failed_nr) -> Dict[str, Any]:
        """从 durable 层派生 observation 富字段（确定性，无 LLM）。"""
        from backend.planning import evidence_refs as ev_refs

        if failed_nr is None:
            # approval_rejected 或无失败 node：只挂 run 级 refs
            refs = [{"ref": ev_refs.node_output_ref(parent.run_id, nid)}
                    for nid in sorted((parent_state.get("nodeOutputs") or {}).keys())][:5]
            return {"stepId": None, "failureCode": None, "failureReason": None,
                    "output": None, "evidenceRefs": refs, "metadata": {}}

        node_id = failed_nr.node_id
        if failed_nr.status.value == "timed_out":
            failure_code = "timeout"
        elif typ == ObservationType.TOOL_FAILED:
            failure_code = "tool_error"
        else:
            failure_code = "node_error"

        refs = [{"ref": ev_refs.node_ref(parent.run_id, node_id)}]
        outputs = parent_state.get("nodeOutputs") or {}
        output = None
        if node_id in outputs:
            # 走 allowlist 投影，绝不把原始 node 输出挂到 observation ——
            # observation 会被 persist 进 workflow_events，原始输出可能内嵌
            # RAG 正文 / memory 原文 / 回填的 action params。
            from backend.planning.context_assembler import project_node_output
            projected, _trust = project_node_output(node_id, outputs[node_id])
            output = {"nodeOutput": projected}
            refs.append({"ref": ev_refs.node_output_ref(parent.run_id, node_id)})
        for err in (parent_state.get("errors") or []):
            if isinstance(err, dict) and err.get("nodeId") == node_id:
                refs.append({"ref": ev_refs.error_ref(parent.run_id, node_id,
                                                      int(err.get("attempt", 1) or 1))})
                break

        return {
            "stepId": node_id,
            "failureCode": failure_code,
            "failureReason": failed_nr.error or None,
            "output": output,
            "evidenceRefs": refs,
            "metadata": {"nodeId": node_id, "attempt": failed_nr.attempt},
        }

    def _observation_prompt_view(self, observation: Observation, plan: Plan) -> Dict[str, Any]:
        """critic invocation **boundary identity** 的读取口（R2 后仅此一处使用）。

        Final Identity Rule（Round2 closure）—— decision identity 与 prompt
        mode 职责分离：
          - 本函数 = decision identity：只由 durable Plan flag 决定，与
            process kill-switch 完全无关。
              flag=false/absent → Phase18 冻结字面值（legacy key 命名空间不变）
              flag=true        → R1 填充的真实证据（enriched grounded 命名空间）
            kill-switch None/True/False 三种取值下 key 必须相同。
          - prompt mode = runtime operational control，由 _grounded_enabled
            （kill-switch AND Plan flag）决定。kill=false 时 prompt legacy、
            key 仍 grounded —— 刻意允许 prompt mode ≠ key mode。

        R3 起 semantic replan claim key 同样只经本读取口（§5）：flag=true →
        grounded identity（真实 stepId，key 格式不变）；flag=false → 冻结
        Phase18 字面值。legacy prompt（_build_replan_context）仍恒消费
        to_phase18_prompt_view()（§12）。
        """
        if getattr(plan, "groundedDecisionContextEnabled", False):
            return observation.to_grounded_prompt_view()
        return observation.to_phase18_prompt_view()

    # ── Phase18 Extension: semantic replan（LLM 重新设计 unresolved suffix）──

    def _requires_approval(self, run: WorkflowRun) -> bool:
        state = run.state if isinstance(run.state, dict) else {}
        risk = state.get("riskAssessment", {}) or {}
        return risk.get("riskLevel", "") in ("高风险", "重大风险")

    def _build_replan_context(self, plan: Plan, parent: WorkflowRun,
                              lineage: ExecutionLineage, observation: Observation) -> Any:
        from backend.planning.capability_snapshot import build_planner_capability_snapshot
        from backend.planning.replan_context import SemanticReplanContext
        snapshot = build_planner_capability_snapshot()
        completed_refs = self._completed_result_refs(parent.run_id)
        # R3 legacy 路径（flag=false / kill=false / grounded assembly 失败）：
        # 恒消费 Phase18 冻结投影 + criticRecommendation={}（§16，Phase18 行为
        # 字节冻结）。flag=true + kill 允许时本 builder 不再被调用（走
        # build_grounded_semantic_replan_messages）。
        view = observation.to_phase18_prompt_view()
        return SemanticReplanContext(
            goal=plan.goal,
            goalType=plan.goalType.value,
            parentPlanVersion=plan.version,
            originalPlanSummary=[
                {"stepId": s.stepId, "stepType": s.stepType.value, "objective": s.objective}
                for s in plan.steps
            ],
            completedPrefixSummary=list(completed_refs.keys()),
            failedStep={"stepId": view["stepId"]},
            # key 顺序即序列化顺序（json.dumps 无 sort_keys）——不得调整
            observation={"type": view["type"], "status": view["status"],
                         "failureReason": view["failureReason"], "failureCode": view["failureCode"]},
            criticRecommendation={},
            capabilitySnapshot=snapshot.to_prompt_dict(),
            rejectionConstraints=list(lineage.rejectionConstraints),
            policyDenyConstraints=list(lineage.policyDenyConstraints),
            remainingBudget={"usage": lineage.budgetUsage.to_dict(), "limits": lineage.budgetLimits.to_dict()},
            evidenceRefs=list(view["evidenceRefs"]),
        )

    def _compile_suffix_from_raw(self, raw: Dict[str, Any], plan: Plan, parent: WorkflowRun) -> Any:
        """从持久化 raw proposal 重建 semantic suffix（already_completed 复用）。失败 → None。"""
        try:
            from backend.planning.capability_snapshot import build_planner_capability_snapshot
            from backend.planning.proposal_compiler import compile_replan_suffix
            from backend.planning.replan_context import SemanticReplanProposal
            proposal = SemanticReplanProposal.from_dict_strict(raw)
            snapshot = build_planner_capability_snapshot()
            carried_ids = set(self._completed_result_refs(parent.run_id).keys())
            return compile_replan_suffix(
                proposal.suffixSteps, snapshot, self._requires_approval(parent), carried_ids
            )
        except Exception:
            return None

    def _try_semantic_replan(self, parent: WorkflowRun, plan: Plan,
                             lineage: ExecutionLineage, observation: Observation) -> Any:
        """LLM 重新设计 unresolved suffix。失败/不可用 → None（fallback deterministic）。"""
        # kill-switch：False 显式禁用（测试隔离）；None/True 跟随 durable plan flag
        if self._semantic_replan_enabled is False:
            return None
        # EA01：durable enablement gate（absent=False，仅 LLM 计划 True）
        if not getattr(plan, "semanticReplanEnabled", False):
            return None
        # EA04：maxReplans 必须在 semantic LLM spend 之前检查
        if lineage.budgetUsage.replansUsed >= lineage.budgetLimits.maxReplans:
            return None
        client = self._critic_client
        if client is None:
            from backend.planning.llm_client import get_planning_llm_client_optional
            client = get_planning_llm_client_optional()
        if client is None:
            return None
        # 只有 semantic_review 分类才允许 semantic replan（hard safety 永不触发）
        if classify_observation(observation, lineage) != "semantic_review":
            return None

        root_run_id = lineage.rootRunId or parent.run_id
        # R3 §5 Final Identity Rule：semantic replan claim key 只由 durable
        # Plan flag + run/root/version + observation boundary 决定 ——
        # kill-switch 不参与。flag=false → Phase18 冻结投影（stepId=""）；
        # flag=true → grounded identity（真实 stepId），kill=None/True/False
        # 均命中同一 claim identity（禁止 kill 切换后在 legacy 命名空间
        # 二次 claim）。key 格式（字段顺序）保持 Phase18，不与 critic key 统一。
        failed_step_id = self._observation_prompt_view(observation, plan)["stepId"]
        key = f"{root_run_id}:{parent.run_id}:{plan.version}:{failed_step_id or 'unknown'}:{observation.type.value}"

        # R3 §6/§17：grounded assembly 在 claim 之前（assembler 本身
        # 0 provider / 0 claim / 0 持久化；失败 → dctx=None → legacy prompt，
        # 绝不 fail workflow）。Critic recommendation 严格绑定：criticBoundaryKey
        # 字节级复现 Critic claim key，registry 只接受 COMPLETED，否则 {}。
        dctx = None
        if self._grounded_enabled(plan):
            try:
                from backend.planning.context_assembler import assemble_or_empty
                from backend.planning.critic import (
                    derive_critic_boundary_key,
                    lookup_bound_critic_recommendation,
                )
                from backend.planning.decision_context import DecisionType
                bound_key = derive_critic_boundary_key(
                    root_run_id, parent.run_id, plan.version,
                    observation.type.value, failed_step_id,
                )
                # EA12：claim 前读最新 parent state（critic claim/complete 各自事务写入）
                fresh_parent = self._repo.get_run(parent.run_id)
                parent_state = fresh_parent.state if fresh_parent is not None \
                    and isinstance(fresh_parent.state, dict) \
                    else (parent.state if isinstance(parent.state, dict) else {})
                bound_rec = lookup_bound_critic_recommendation(parent_state, bound_key)
                assembled = assemble_or_empty(
                    self._repo, parent, plan, observation, DecisionType.SEMANTIC_REPLAN,
                    lineage=lineage,
                    critic_recommendation=bound_rec or None,
                    critic_boundary_key=bound_key,
                )
                if not assembled.isEmpty:
                    dctx = assembled
            except Exception:
                dctx = None

        claim = self._repo.claim_semantic_replan_tx(parent.run_id, key)
        result = claim.get("result")
        if result == "already_completed":
            return self._compile_suffix_from_raw(claim.get("proposal", {}).get("raw", {}), plan, parent)
        if result != "claimed":
            return None  # already_started / budget_exhausted / not_eligible → fallback

        try:
            from backend.planning.capability_snapshot import build_planner_capability_snapshot
            from backend.planning.proposal_compiler import compile_replan_suffix
            from backend.planning.replan_context import (
                SemanticReplanProposal,
                build_grounded_semantic_replan_messages,
                build_semantic_replan_messages,
            )
            snapshot = build_planner_capability_snapshot()
            if dctx is not None:
                # R3 grounded prompt：唯一来源 = split_trusted_projection +
                # capability snapshot（authority）；输出 schema 与 legacy 一致。
                # 调用失败 → 直接 fallback（§17：每 decision ≤1 provider call，
                # 禁止 grounded 失败后再补一次 legacy provider call）。
                system, user = build_grounded_semantic_replan_messages(
                    dctx, snapshot.to_prompt_dict()
                )
            else:
                # legacy 路径（flag=false / kill=false / assembly 失败）：
                # criticRecommendation 恒 {}（§16，Phase18 行为冻结）
                ctx = self._build_replan_context(plan, parent, lineage, observation)
                system, user = build_semantic_replan_messages(ctx)
            data, _usage, _attempts = client.call_structured_json_sync(system, user)
            proposal = SemanticReplanProposal.from_dict_strict(data)
            carried_ids = set(self._completed_result_refs(parent.run_id).keys())
            suffix = compile_replan_suffix(
                proposal.suffixSteps, snapshot, self._requires_approval(parent), carried_ids
            )
            self._repo.complete_semantic_replan_tx(parent.run_id, key, {"raw": data})
            return suffix
        except Exception:
            return None

    def _perform_replan(
        self,
        parent: WorkflowRun,
        plan: Plan,
        lineage: ExecutionLineage,
        observation: Observation,
        suffix_steps: Any = None,
    ) -> Dict[str, Any]:
        """执行 revision transaction（build → validate → child cutover → execute）。

        suffix_steps 提供时走 semantic revision（carried prefix + LLM suffix），
        否则走 deterministic build_revision（carried prefix + 原 suffix re-attempt）。
        """
        # R3 §17：critic / semantic replan claim 已在 DB 事务内递增 llmCallsUsed /
        # criticCallsUsed；此处 in-memory lineage 是 claim 前的 stale 快照，
        # 直接用 reserve_replan + inherit_lineage 会把 claim 计数清零
        # （parent 终态与 child 继承的 budget 都少记 → maxLlmCalls 跨代不生效）。
        # 从最新 parent state 重建 lineage 后再 reserve/inherit。
        fresh_parent = self._repo.get_run(parent.run_id)
        if fresh_parent is not None and isinstance(fresh_parent.state, dict):
            fresh_lineage = get_lineage(fresh_parent.state)
            if fresh_lineage.rootRunId:
                lineage = fresh_lineage

        completed_refs = self._completed_result_refs(parent.run_id)

        # 构建 v2（carried prefix + suffix）
        if suffix_steps is not None:
            v2 = build_semantic_revision(plan, completed_refs, parent.run_id, suffix_steps)
        else:
            v2 = build_revision(plan, completed_refs, parent.run_id)

        # validate
        issues = validate_plan(v2)
        if has_errors(issues):
            self.persist_observation(observation)
            return {"error": "v2 validation failed", "validationIssues": [i.to_dict() for i in issues]}

        # carried result 校验（fail-closed）
        carried_issues = validate_carried_refs(v2, self._repo)
        if carried_issues:
            return {"error": "carried validation failed", "carriedIssues": carried_issues}

        # maxReplans enforcement（execution lineage）
        if not reserve_replan(lineage):
            budget_obs = Observation(
                observationId=generate_observation_id(parent.run_id),
                planId=plan.planId, planVersion=plan.version, runId=parent.run_id,
                type=ObservationType.BUDGET_EXHAUSTED, status=ObservationStatus.BLOCKED,
                scope=ObservationScope.RUN, source=ObservationSource.BUDGET,
            )
            self.persist_observation(budget_obs)
            return {"error": "maxReplans exhausted", "decision": "abort"}

        # 持久化 observation
        self.persist_observation(observation)

        # child lineage 继承（含 replansUsed+1，与 cutover 事务原子提交）
        child_lineage = inherit_lineage(lineage)
        return self._create_and_execute_child(parent, v2, child_lineage, observation)

    def _create_and_execute_child(
        self,
        parent: WorkflowRun,
        v2: Plan,
        child_lineage: ExecutionLineage,
        observation: Observation,
    ) -> Dict[str, Any]:
        """child cutover + execute。"""
        child_run_id = deterministic_child_run_id(child_lineage.rootRunId, observation.observationId)
        child_definition = plan_to_child_definition(v2)

        # reload latest parent state：claim 写入的 registry（semanticReplanInvocations / budget）
        # 必须保留，禁止用 stale parent.state 整块覆盖（EA12 / Design Lock V2.1 §17）。
        fresh_parent = self._repo.get_run(parent.run_id)
        parent_state = fresh_parent.state if fresh_parent is not None and isinstance(fresh_parent.state, dict) \
            else (parent.state if isinstance(parent.state, dict) else {})
        carried_refs = {s.stepId: s.resultRef for s in v2.steps if s.metadata.get("carriedForward")}
        child_state = build_child_state(
            parent_state, child_lineage, parent.run_id, parent.version, carried_refs,
        )

        child_run = WorkflowRun(
            run_id=child_run_id, definition_id=v2.planId,
            version=parent.version + 1,  # placeholder，事务内重新分配
            session_id=parent.session_id, event_thread_id=parent.event_thread_id,
            status=WorkflowRunStatus.PENDING, state=child_state,
            triggered_by="replan",
        )

        # 更新 parent state：lineage 指针 + termination metadata
        new_parent_state = dict(parent_state)
        new_parent_state["replannedToRunId"] = child_run_id
        # replannedToVersion 由事务内实际分配的 next_version 覆盖（不在此猜 parent.version+1）
        new_parent_state["terminationReason"] = "replanned"
        new_parent_state["executionLineage"] = child_lineage.to_dict()
        parent_status = WorkflowRunStatus.REJECTED if parent.status == WorkflowRunStatus.REJECTED else WorkflowRunStatus.FAILED

        try:
            new_version = self._repo.create_child_continuation_tx(
                child_run=child_run,
                parent_run_id=parent.run_id,
                parent_status=parent_status.value,
                parent_state=new_parent_state,
                definition_json=child_definition.to_dict(),
            )
        except Exception as e:
            # 幂等（确定性 run_id PK 冲突）→ 返回既有 child
            existing = self._repo.get_run(child_run_id)
            if existing is not None:
                return {"childRunId": child_run_id, "alreadyReplanned": True}
            return {"error": f"child cutover failed: {e}"}

        # child driver_managed 已在事务内落库（create_child_continuation_tx），此处无需 post-commit mark
        return {"childRunId": child_run_id, "version": new_version, "started": True}
