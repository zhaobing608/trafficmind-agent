"""Deterministic adapter from persisted Agent collaboration results to Planning."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from backend.agent.collaboration.db_repository import SQLiteCollaborationRepository
from backend.agent.tool_registry import get_tool_registry
from backend.planning.capability_snapshot import is_planner_executable_action
from backend.planning.param_schema import normalize_parameter_hints
from backend.grounding.rendering import grounding_audit_summary
from backend.tools.event_identity import (
    EventIdentityError,
    compact_event_context,
    extract_event_id,
    hydrate_authoritative_event,
)


class AgentPlanningAdapterError(ValueError):
    """Raised when a persisted Agent result cannot truthfully create a Plan."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass
class AgentPlanningInput:
    goal: str
    event: Dict[str, Any]
    ragEvidence: Dict[str, Any]
    memoryContext: Dict[str, Any]
    constraints: Dict[str, Any]
    planMetadata: Dict[str, Any]

    def to_request_dict(self) -> Dict[str, Any]:
        return {
            "goal": self.goal,
            "event": self.event,
            "ragEvidence": self.ragEvidence,
            "memoryContext": self.memoryContext,
            "constraints": self.constraints,
            "planMetadata": self.planMetadata,
        }


def build_planning_input_from_agent(
    event_id: str,
    session_id: str,
    collaboration_run_id: str,
    *,
    repo: Optional[SQLiteCollaborationRepository] = None,
) -> AgentPlanningInput:
    """Map persisted structured Agent output into the existing planning input."""
    canonical_event_id = str(event_id or "").strip()
    if not canonical_event_id:
        raise AgentPlanningAdapterError("missing_event_id", "eventId 不能为空")
    if not collaboration_run_id:
        raise AgentPlanningAdapterError("missing_collaboration_run_id", "collaborationRunId 不能为空")

    try:
        authoritative_event = hydrate_authoritative_event(canonical_event_id)
    except EventIdentityError as err:
        raise AgentPlanningAdapterError(err.code, err.message)

    repo = repo or SQLiteCollaborationRepository()
    run = repo.get_run(collaboration_run_id)
    if not run:
        raise AgentPlanningAdapterError(
            "collaboration_run_not_found",
            f"协作运行 {collaboration_run_id} 不存在",
        )
    if session_id and run.get("session_id") != session_id:
        raise AgentPlanningAdapterError(
            "session_mismatch",
            f"协作运行 {collaboration_run_id} 不属于会话 {session_id}",
        )

    normalized_event = _parse_json(run.get("normalized_event"), {})
    run_event_id = extract_event_id(normalized_event)
    if not run_event_id:
        raise AgentPlanningAdapterError(
            "collaboration_run_unbound",
            f"协作运行 {collaboration_run_id} 未绑定真实事件",
        )
    if run_event_id != canonical_event_id:
        raise AgentPlanningAdapterError(
            "event_id_mismatch",
            f"协作运行绑定事件 {run_event_id}，不能为事件 {canonical_event_id} 生成方案",
        )

    tasks = repo.list_tasks(collaboration_run_id)
    selected_agents = _parse_json(run.get("selected_agents"), [])
    final_decision = _parse_json(run.get("final_decision"), run.get("final_decision") or "")
    grounding_context = _parse_json(run.get("grounding_context"), {})
    grounding_audit = grounding_audit_summary(grounding_context) if grounding_context else {}

    findings, recommendations, accepted, rejected, evidence_refs = _extract_agent_outputs(tasks)
    source_agent = {
        "sessionId": run.get("session_id", ""),
        "collaborationRunId": collaboration_run_id,
        "selectedAgents": selected_agents,
        "finalStatus": run.get("status", ""),
        "runKind": normalized_event.get("runKind", "live"),
    }
    if grounding_audit:
        source_agent["groundingStatus"] = grounding_audit.get("groundingStatus", "MINIMAL")
        source_agent["groundingContextAvailable"] = True
    recommendation_audit = {
        "accepted": accepted,
        "rejected": rejected,
    }
    event = compact_event_context(authoritative_event)
    goal = _derive_goal(event)
    constraints = {
        "sourceAgent": source_agent,
        "agentFindings": findings,
        "agentRecommendations": recommendations,
        "agentRecommendationAudit": recommendation_audit,
        "actionExecutionAllowed": bool(
            normalized_event.get("actionExecutionAllowed", True)
        ),
    }
    if final_decision:
        constraints["agentFinalDecision"] = final_decision
    if grounding_audit:
        constraints["agentGroundingAudit"] = grounding_audit

    plan_metadata = {
        "eventSnapshot": event,
        "sourceAgent": source_agent,
        "agentRecommendationAudit": recommendation_audit,
        "runKind": normalized_event.get("runKind", "live"),
        "replayId": normalized_event.get("replayId", ""),
        "replayOfEventId": normalized_event.get("replayOfEventId", ""),
        "actionExecutionAllowed": bool(
            normalized_event.get("actionExecutionAllowed", True)
        ),
    }
    if grounding_audit:
        plan_metadata["agentGroundingAudit"] = grounding_audit
    rag_evidence = _evidence_refs_to_rag(evidence_refs)

    return AgentPlanningInput(
        goal=goal,
        event=event,
        ragEvidence=rag_evidence,
        memoryContext={},
        constraints=constraints,
        planMetadata=plan_metadata,
    )


def _parse_json(value: Any, default: Any) -> Any:
    if isinstance(value, str):
        if not value:
            return default
        try:
            return json.loads(value)
        except Exception:
            return default
    return value if value is not None else default


def _derive_goal(event: Dict[str, Any]) -> str:
    road = str(event.get("roadName") or "").strip()
    event_type = str(event.get("eventTypeCn") or event.get("eventType") or "").strip()
    if road and event_type:
        return f"{road}{event_type}处置方案"
    if event_type:
        return f"{event_type}处置方案"
    return "交通事件处置方案"


def _extract_agent_outputs(tasks: List[Dict[str, Any]]):
    findings: List[Dict[str, Any]] = []
    recommendations: List[Dict[str, Any]] = []
    accepted: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    evidence_refs: List[Any] = []

    for task in tasks:
        output = _parse_json(task.get("output_snapshot"), {})
        if not isinstance(output, dict):
            continue
        agent_name = output.get("agentName") or output.get("agent_name") or task.get("agent_name", "")
        task_id = output.get("taskId") or output.get("task_id") or task.get("task_id", "")

        task_findings = output.get("findings") if isinstance(output.get("findings"), list) else []
        if task_findings:
            findings.append({
                "agentName": agent_name,
                "taskId": task_id,
                "findings": task_findings,
                "urgency": output.get("urgency", ""),
                "confidence": output.get("confidence"),
            })

        proposed_actions = output.get("proposed_actions") or output.get("proposedActions") or []
        if not isinstance(proposed_actions, list):
            proposed_actions = []
        recommendation = {
            "agentName": agent_name,
            "taskId": task_id,
            "suggestion": output.get("suggestion") or output.get("recommendation") or "",
            "proposedActions": proposed_actions,
        }
        if recommendation["suggestion"] or proposed_actions:
            recommendations.append(recommendation)

        for action in proposed_actions:
            accepted_item, rejected_item = _classify_agent_action(action, agent_name, task_id)
            if accepted_item:
                accepted.append(accepted_item)
            if rejected_item:
                rejected.append(rejected_item)

        task_refs = output.get("evidence_refs") or output.get("evidenceRefs") or []
        if isinstance(task_refs, list):
            evidence_refs.extend(task_refs)

    return findings, recommendations, accepted, rejected, _dedupe_evidence_refs(evidence_refs)


def _classify_agent_action(action: Any, agent_name: str, task_id: str):
    if not isinstance(action, dict):
        return None, {
            "agentName": agent_name,
            "taskId": task_id,
            "reason": "invalid_structure",
        }
    action_type = str(action.get("actionType") or action.get("action_type") or "").strip()
    if not action_type:
        return None, {
            "agentName": agent_name,
            "taskId": task_id,
            "reason": "invalid_structure",
        }

    registry = get_tool_registry()
    meta = registry.get(action_type)
    if meta is None:
        return None, {
            "agentName": agent_name,
            "taskId": task_id,
            "actionType": action_type,
            "reason": "not_registered",
        }
    if meta.category == "simulation" or action_type.startswith("simulation_"):
        return None, {
            "agentName": agent_name,
            "taskId": task_id,
            "actionType": action_type,
            "reason": "simulation_only",
        }
    if action_type == "save_result" or not is_planner_executable_action(action_type):
        return None, {
            "agentName": agent_name,
            "taskId": task_id,
            "actionType": action_type,
            "reason": "unsupported_action",
        }

    params = action.get("params") or action.get("action_params") or action.get("parameterHints") or {}
    try:
        params = normalize_parameter_hints(action_type, params if isinstance(params, dict) else {})
    except Exception:
        return None, {
            "agentName": agent_name,
            "taskId": task_id,
            "actionType": action_type,
            "reason": "invalid_structure",
        }
    return {
        "agentName": agent_name,
        "taskId": task_id,
        "actionType": action_type,
        "paramsTemplate": params,
        "reason": "agent_structured_proposed_action",
    }, None


def _dedupe_evidence_refs(refs: List[Any]) -> List[Any]:
    seen = set()
    out = []
    for ref in refs:
        key = json.dumps(ref, sort_keys=True, ensure_ascii=False) if isinstance(ref, dict) else str(ref)
        if key in seen:
            continue
        seen.add(key)
        out.append(ref)
    return out


def _evidence_refs_to_rag(refs: List[Any]) -> Dict[str, Any]:
    results = []
    for ref in refs:
        if isinstance(ref, dict):
            rid = ref.get("id") or ref.get("evidenceId") or ref.get("docId")
            if rid:
                results.append({
                    "id": rid,
                    "source": ref.get("source", "agent_evidence_ref"),
                    "score": ref.get("score"),
                })
        elif isinstance(ref, str) and ref.strip():
            results.append({"id": ref.strip(), "source": "agent_evidence_ref"})
    return {
        "query": "",
        "results": results,
        "resultCount": len(results),
        "traceId": "",
        "degraded": False,
    }
