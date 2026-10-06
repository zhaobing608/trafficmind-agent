"""Build traffic case memories from persisted workflow source chains."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from backend.agent.collaboration.db_repository import SQLiteCollaborationRepository
from backend.case_memory.models import (
    CaseMemoryError,
    CaseMemoryQuality,
    EventOutcome,
    FeedbackEffectiveness,
    FeedbackLifecycle,
    TrafficCaseMemory,
    TrafficEventFeedback,
    build_case_id,
    utc_now_iso,
)
from backend.case_memory.repository import SQLiteCaseMemoryRepository
from backend.planning.agent_planning_adapter import _extract_agent_outputs
from backend.planning.models import Plan
from backend.regional.repository import SQLiteRegionalRepository
from backend.tools.db_tools import get_event_by_id
from backend.workflow.models import (
    ActionStatus,
    ApprovalDecision,
    WorkflowActionRecord,
    WorkflowApproval,
    WorkflowRun,
)
from backend.workflow.repository import SQLiteWorkflowRepository
from backend.workflow.action_execution import sanitize_public_text, sanitize_public_value


class TrafficCaseBuilder:
    def __init__(
        self,
        *,
        workflow_repo: Optional[SQLiteWorkflowRepository] = None,
        regional_repo: Optional[SQLiteRegionalRepository] = None,
        collaboration_repo: Optional[SQLiteCollaborationRepository] = None,
        feedback_repository: Optional[SQLiteCaseMemoryRepository] = None,
    ):
        self.workflow_repo = workflow_repo or SQLiteWorkflowRepository()
        self.regional_repo = regional_repo or SQLiteRegionalRepository()
        self.collaboration_repo = collaboration_repo or SQLiteCollaborationRepository()
        self.feedback_repository = feedback_repository or SQLiteCaseMemoryRepository()

    def build_from_workflow_run(self, run_id: str) -> TrafficCaseMemory:
        run = self.workflow_repo.get_run(run_id)
        if run is None:
            raise CaseMemoryError(
                "WORKFLOW_RUN_NOT_FOUND",
                f"workflow run not found: {run_id}",
                status_code=404,
            )
        if not run.is_terminal():
            raise CaseMemoryError(
                "CASE_NOT_BUILDABLE_WORKFLOW_NOT_TERMINAL",
                "case memory requires a terminal workflow run",
                status_code=409,
            )

        state = run.state if isinstance(run.state, dict) else {}
        event_id = _extract_state_event_id(state)
        if not event_id:
            raise CaseMemoryError(
                "CASE_NOT_BUILDABLE_EVENT_RELATION_MISSING",
                "workflow state must include currentEvent.eventId",
                status_code=409,
            )
        if _is_simulation_source(event_id, state):
            raise CaseMemoryError(
                "CASE_NOT_BUILDABLE_SIMULATION_SOURCE",
                "simulation-derived workflows cannot create traffic case memory",
                status_code=409,
            )
        authoritative_event = get_event_by_id(event_id)
        if not authoritative_event:
            raise CaseMemoryError(
                "CASE_NOT_BUILDABLE_EVENT_NOT_FOUND",
                f"source event not found: {event_id}",
                status_code=409,
            )
        if _is_simulation_source(event_id, authoritative_event):
            raise CaseMemoryError(
                "CASE_NOT_BUILDABLE_SIMULATION_SOURCE",
                "simulation-derived events cannot create traffic case memory",
                status_code=409,
            )

        binding = self.regional_repo.get_active_event_location_binding(event_id)
        if not binding or not binding.get("regionId"):
            raise CaseMemoryError(
                "CASE_NOT_BUILDABLE_CANONICAL_REGION_MISSING",
                "case memory requires a resolved canonical event location",
                status_code=409,
            )

        event_snapshot = _compact_event_snapshot(authoritative_event)
        event_type = str(event_snapshot.get("eventType") or "").strip()
        if not event_type:
            raise CaseMemoryError(
                "CASE_NOT_BUILDABLE_EVENT_TYPE_MISSING",
                "source event must include eventType",
                status_code=409,
            )

        plan, plan_provenance = self._recover_source_plan(run, event_id)
        if plan and plan.goalType.value == "simulation_evaluation":
            raise CaseMemoryError(
                "CASE_NOT_BUILDABLE_SIMULATION_SOURCE",
                "simulation evaluation plans cannot create traffic case memory",
                status_code=409,
            )
        collaboration_run_id = _source_collaboration_run_id(plan)
        collaboration_run, tasks, agent_provenance = self._recover_source_agent(
            collaboration_run_id,
            event_id,
            run.session_id,
        )

        approvals = self.workflow_repo.list_approvals(run.run_id)
        action_records = self.workflow_repo.list_action_records(run.run_id)
        feedback = self.feedback_repository.get_feedback(event_id, run.run_id)
        plan_facts = _build_plan_facts(plan, run, self.workflow_repo) if plan else {}
        agent_facts = _build_agent_facts(collaboration_run, tasks) if collaboration_run else {}
        human_decisions = _build_human_decisions(approvals)
        recommendation_feedback = _build_recommendation_feedback(
            run, plan_facts, approvals, action_records
        )
        action_feedback = _build_action_feedback(
            plan_facts,
            approvals,
            action_records,
            feedback,
            self.workflow_repo,
        )
        event_outcome = _build_event_outcome(
            run, event_snapshot, action_records, feedback
        )
        workflow_outcome = _build_workflow_outcome(
            run, approvals, action_records, feedback
        )
        lessons = _build_lessons(run, human_decisions, action_records, agent_facts, plan_facts)

        quality = _quality_status(
            run=run,
            feedback=feedback,
            event_outcome=event_outcome,
            action_records=action_records,
        )
        system_fact_horizons = [
            _source_fact_horizon([event_snapshot.get("updatedAt")]),
            _source_fact_horizon([
                binding.get("updatedAt"),
                binding.get("resolvedAt"),
                binding.get("createdAt"),
            ]),
            _source_fact_horizon([
                run.completed_at,
                run.updated_at,
                run.started_at,
            ]),
        ]
        if plan:
            system_fact_horizons.append(
                _source_fact_horizon([plan_facts.get("latestVersionCreatedAt")])
            )
        if collaboration_run:
            system_fact_horizons.append(_source_fact_horizon([
                collaboration_run.get("updated_at"),
                collaboration_run.get("completed_at"),
                collaboration_run.get("started_at"),
            ]))
        system_fact_horizons.extend(
            _source_fact_horizon([
                task.get("started_at"),
                task.get("completed_at"),
            ])
            for task in tasks
        )
        system_fact_horizons.extend(
            _source_fact_horizon([approval.decided_at, approval.created_at])
            for approval in approvals
        )
        system_fact_horizons.extend(
            _source_fact_horizon([
                record.last_reconciled_at,
                record.finished_at,
                record.completed_at,
                record.created_at,
            ])
            for record in action_records
        )
        case = TrafficCaseMemory(
            case_id=build_case_id(run.run_id),
            region_id=str(binding["regionId"]),
            event_id=event_id,
            event_type=event_type,
            road_id=binding.get("roadId"),
            intersection_id=binding.get("intersectionId"),
            source_session_id=run.session_id or None,
            source_collaboration_run_id=collaboration_run_id if collaboration_run else None,
            source_plan_id=plan.planId if plan else None,
            source_workflow_run_id=run.run_id,
            final_status=run.status.value,
            quality_status=quality,
            event_snapshot=event_snapshot,
            agent_facts=agent_facts,
            plan_facts=plan_facts,
            human_decisions=human_decisions,
            workflow_outcome=workflow_outcome,
            recommendation_feedback=recommendation_feedback,
            action_feedback=action_feedback,
            event_outcome=event_outcome,
            feedback_lifecycle=(
                feedback.lifecycle if feedback else FeedbackLifecycle.PENDING
            ),
            feedback_updated_at=feedback.updated_at if feedback else None,
            feedback_revision=feedback.revision if feedback else None,
            system_updated_at=(
                _latest_timestamp(system_fact_horizons) or utc_now_iso()
            ),
            lessons=lessons,
            generated_summary=_build_generated_summary(event_snapshot, run),
            started_at=run.started_at or None,
            completed_at=run.completed_at or None,
            source_reference=f"workflow_runs:{run.run_id}",
            provenance={
                "sourceWorkflowRunId": run.run_id,
                "sourceEventId": event_id,
                "eventSource": "event_records",
                "eventLocationBindingId": binding.get("bindingId"),
                "canonicalLocation": {
                    "regionId": binding.get("regionId"),
                    "roadId": binding.get("roadId"),
                    "intersectionId": binding.get("intersectionId"),
                },
                "plan": plan_provenance,
                "agent": agent_provenance,
                "workflow": {
                    "approvals": len(approvals),
                    "actions": len(action_records),
                    "terminalStatus": run.status.value,
                },
                "rawTranscriptStored": False,
                "structuredFactsAuthoritative": True,
                "businessOutcomeInferred": False,
                "feedbackId": feedback.feedback_id if feedback else None,
                "feedbackSource": "traffic_event_feedback" if feedback else None,
            },
        )
        return case

    def _recover_source_plan(
        self,
        run: WorkflowRun,
        event_id: str,
    ) -> Tuple[Optional[Plan], Dict[str, Any]]:
        definition_payload: Optional[Dict[str, Any]] = None
        definition_version = self.workflow_repo.get_definition_version(run.definition_id, run.version)
        if definition_version and isinstance(definition_version.definition_json, dict):
            definition_payload = definition_version.definition_json
        if definition_payload is None:
            definition = self.workflow_repo.get_definition(run.definition_id)
            if definition:
                definition_payload = definition.to_dict()

        metadata = (definition_payload or {}).get("metadata") or {}
        plan_payload = metadata.get("plan") if isinstance(metadata, dict) else None
        if not isinstance(plan_payload, dict):
            return None, {
                "status": "not_found",
                "source": "workflow_definition.metadata.plan",
            }

        try:
            plan = Plan.from_dict(plan_payload)
        except Exception as exc:
            raise CaseMemoryError(
                "CASE_SOURCE_PLAN_INVALID",
                f"source plan metadata is invalid: {exc}",
                status_code=409,
            ) from exc
        if plan.eventId and plan.eventId != event_id:
            raise CaseMemoryError(
                "CASE_SOURCE_PLAN_EVENT_MISMATCH",
                "source plan eventId does not match workflow currentEvent.eventId",
                status_code=409,
            )
        return plan, {
            "status": "attached",
            "source": "workflow_definition_version.metadata.plan"
            if definition_version
            else "workflow_definition.metadata.plan",
            "planId": plan.planId,
            "version": plan.version,
        }

    def _recover_source_agent(
        self,
        collaboration_run_id: Optional[str],
        event_id: str,
        workflow_session_id: str,
    ) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
        if not collaboration_run_id:
            return None, [], {
                "status": "not_found",
                "source": "plan.metadata.sourceAgent.collaborationRunId",
            }
        run = self.collaboration_repo.get_run(collaboration_run_id)
        if not run:
            return None, [], {
                "status": "not_found",
                "source": "collaboration_runs",
                "collaborationRunId": collaboration_run_id,
            }
        normalized_event = _parse_json(run.get("normalized_event"), {})
        if str(normalized_event.get("eventId") or "").strip() != event_id:
            return None, [], {
                "status": "event_mismatch",
                "source": "collaboration_runs.normalized_event.eventId",
                "collaborationRunId": collaboration_run_id,
            }
        if workflow_session_id and run.get("session_id") and run.get("session_id") != workflow_session_id:
            return None, [], {
                "status": "session_mismatch",
                "source": "collaboration_runs.session_id",
                "collaborationRunId": collaboration_run_id,
            }
        tasks = self.collaboration_repo.list_tasks(collaboration_run_id)
        return run, tasks, {
            "status": "attached",
            "source": "collaboration_runs + collaboration_tasks",
            "collaborationRunId": collaboration_run_id,
            "taskCount": len(tasks),
        }


def _extract_state_event_id(state: Dict[str, Any]) -> str:
    current = state.get("currentEvent") or state.get("current_event") or {}
    if not isinstance(current, dict):
        return ""
    return str(current.get("eventId") or current.get("event_id") or "").strip()


def _is_simulation_source(event_id: str, payload: Any) -> bool:
    if str(event_id or "").lower().startswith("simevt_"):
        return True
    return _contains_simulation_marker(payload)


def _contains_simulation_marker(payload: Any) -> bool:
    provenance_ref_keys = {
        "simulationrefs",
        "simulation_refs",
    }
    source_type_keys = {
        "sourcetype",
        "source_type",
    }
    explicit_marker_keys = {
        "simulationrunid",
        "simulation_run_id",
        "simulationid",
        "simulation_id",
        "scenarioid",
        "scenario_id",
        "simulationscenarioid",
        "simulation_scenario_id",
        "simulated",
        "simulationderived",
        "simulation_derived",
    }
    if isinstance(payload, dict):
        for key, value in payload.items():
            normalized = str(key).replace("-", "_").lower()
            compact = normalized.replace("_", "")
            if normalized in provenance_ref_keys or compact in provenance_ref_keys:
                if _has_simulation_ref_payload(value):
                    return True
                continue
            if normalized in source_type_keys or compact in source_type_keys:
                if _is_simulation_source_type(value):
                    return True
                continue
            if normalized in explicit_marker_keys or compact in explicit_marker_keys:
                if _has_explicit_simulation_marker(value):
                    return True
                continue
            if _contains_simulation_marker(value):
                return True
    elif isinstance(payload, list):
        return any(_contains_simulation_marker(item) for item in payload)
    return False


def _has_simulation_ref_payload(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (dict, list, tuple, set)):
        return bool(value)
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _has_explicit_simulation_marker(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        return bool(text) and text not in {"false", "0", "no", "none", "null"}
    if isinstance(value, (dict, list, tuple, set)):
        return bool(value)
    return True


def _is_simulation_source_type(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        text = value.strip().lower()
        return text in {
            "simulation",
            "simulated",
            "simulation_derived",
            "traffic_simulation",
            "demo_simulation",
        }
    return False


def _compact_event_snapshot(event: Dict[str, Any]) -> Dict[str, Any]:
    standard = _parse_json(event.get("standardEvent"), {})
    full = _parse_json(event.get("fullResult"), {})
    full_standard = full.get("standardEvent") if isinstance(full, dict) else {}
    raw_event = _parse_json(event.get("rawEvent"), {})
    if not isinstance(standard, dict):
        standard = {}
    if not isinstance(full_standard, dict):
        full_standard = {}
    if not isinstance(raw_event, dict):
        raw_event = {}
    return {
        "eventId": _pick(event.get("eventId"), standard.get("eventId"), full_standard.get("eventId")),
        "eventType": _pick(event.get("eventType"), standard.get("eventType"), full_standard.get("eventType")),
        "eventTypeCn": _pick(
            event.get("eventTypeCn"),
            standard.get("eventTypeCn"),
            full_standard.get("eventTypeCn"),
        ),
        "roadName": _pick(event.get("roadName"), standard.get("roadName"), full_standard.get("roadName")),
        "direction": _pick(event.get("direction"), standard.get("direction"), full_standard.get("direction")),
        "riskScore": event.get("riskScore"),
        "riskLevel": event.get("riskLevel"),
        "status": event.get("status"),
        "duration": _pick(event.get("duration"), standard.get("duration"), full_standard.get("duration")),
        "avgSpeed": _pick(event.get("avgSpeed"), standard.get("avgSpeed"), full_standard.get("avgSpeed")),
        "queueLength": _pick(event.get("queueLength"), standard.get("queueLength"), full_standard.get("queueLength")),
        "weather": _pick(event.get("weather"), standard.get("weather"), full_standard.get("weather")),
        "timePeriod": _pick(event.get("timePeriod"), standard.get("timePeriod"), full_standard.get("timePeriod")),
        "isMainRoad": _pick(event.get("isMainRoad"), standard.get("isMainRoad"), full_standard.get("isMainRoad")),
        "nearbySchool": _pick(event.get("nearbySchool"), standard.get("nearbySchool"), full_standard.get("nearbySchool")),
        "nearbyHospital": _pick(event.get("nearbyHospital"), standard.get("nearbyHospital"), full_standard.get("nearbyHospital")),
        "createdAt": event.get("createdAt"),
        "updatedAt": event.get("updatedAt"),
        "report": _short_text(event.get("report")),
        "sourcePayloadStored": {
            "rawEvent": bool(raw_event),
            "fullResult": bool(full),
        },
    }


def _source_collaboration_run_id(plan: Optional[Plan]) -> Optional[str]:
    if plan is None:
        return None
    metadata = plan.metadata if isinstance(plan.metadata, dict) else {}
    source_agent = metadata.get("sourceAgent") if isinstance(metadata.get("sourceAgent"), dict) else {}
    return (
        str(source_agent.get("collaborationRunId") or "").strip()
        or str(metadata.get("collaborationRunId") or "").strip()
        or None
    )


def _build_agent_facts(
    collaboration_run: Dict[str, Any],
    tasks: List[Dict[str, Any]],
) -> Dict[str, Any]:
    findings, recommendations, accepted, rejected, evidence_refs = _extract_agent_outputs(tasks)
    return {
        "source": "collaboration_tasks.output_snapshot",
        "collaborationRunId": collaboration_run.get("run_id"),
        "sessionId": collaboration_run.get("session_id"),
        "status": collaboration_run.get("status"),
        "selectedAgents": _parse_json(collaboration_run.get("selected_agents"), []),
        "failedAgents": _parse_json(collaboration_run.get("failed_agents"), []),
        "taskCount": len(tasks),
        "findings": _compact_json(findings, max_items=20),
        "recommendations": _compact_json(recommendations, max_items=20),
        "acceptedActions": _compact_json(accepted, max_items=20),
        "rejectedActions": _compact_json(rejected, max_items=20),
        "evidenceRefs": _compact_json(evidence_refs, max_items=20),
        "finalDecision": _compact_json(_parse_json(collaboration_run.get("final_decision"), {}), max_items=20),
        "rawMessagesStored": False,
    }


def _build_plan_facts(
    plan: Plan,
    run: WorkflowRun,
    workflow_repo: SQLiteWorkflowRepository,
) -> Dict[str, Any]:
    versions = workflow_repo.list_definition_versions(plan.planId)
    latest_version = versions[0].version if versions else plan.version
    latest_version_created_at = versions[0].created_at if versions else plan.updatedAt
    replan_count = max(0, latest_version - 1)
    metadata = plan.metadata if isinstance(plan.metadata, dict) else {}
    return {
        "source": "workflow_definition.metadata.plan",
        "planId": plan.planId,
        "planFingerprint": plan.planFingerprint,
        "goal": plan.goal,
        "goalType": plan.goalType.value,
        "definitionStatus": plan.definitionStatus.value,
        "version": plan.version,
        "workflowVersion": run.version,
        "latestVersion": latest_version or plan.version,
        "latestVersionCreatedAt": latest_version_created_at,
        "replanCount": replan_count,
        "stepCount": len(plan.steps),
        "steps": [
            {
                "stepId": step.stepId,
                "stepType": step.stepType.value,
                "objective": step.objective,
                "agentType": step.agentType,
                "toolName": step.toolName,
                "actionType": step.actionType,
                "approvalRequired": bool(step.approvalRequired),
                "riskLevel": step.riskLevel,
                "expectedOutcome": step.expectedOutcome,
                "params": _compact_json(
                    _redact_sensitive(
                        dict((step.metadata or {}).get("paramsTemplate") or {})
                    ),
                    max_items=20,
                ),
                "evidenceRefs": _compact_json(step.evidenceRefs, max_items=10),
            }
            for step in plan.steps
        ],
        "agentRecommendationAudit": _compact_json(
            metadata.get("agentRecommendationAudit") or {},
            max_items=20,
        ),
        "createdAt": plan.createdAt,
        "updatedAt": plan.updatedAt,
    }


def _build_human_decisions(approvals: List[WorkflowApproval]) -> List[Dict[str, Any]]:
    decisions: List[Dict[str, Any]] = []
    for approval in approvals:
        final_actions = (
            approval.edited_actions
            if approval.decision == ApprovalDecision.EDITED
            else approval.proposed_actions
            if approval.decision == ApprovalDecision.APPROVED
            else []
        )
        decisions.append({
            "approvalId": approval.approval_id,
            "nodeId": approval.node_id,
            "decision": approval.decision.value,
            "reviewer": approval.reviewer,
            "comment": _short_text(approval.comment),
            "reasonCode": approval.reason_code.value,
            "proposedActions": [_compact_action(item) for item in approval.proposed_actions],
            "editedActions": [_compact_action(item) for item in approval.edited_actions],
            "finalActions": [_compact_action(item) for item in final_actions],
            "modifications": _structured_action_diff(
                approval.proposed_actions,
                final_actions,
            ),
            "editedActionCount": len(approval.edited_actions),
            "manualAdjustment": bool(approval.edited_actions),
            "createdAt": approval.created_at,
            "decidedAt": approval.decided_at,
        })
    return decisions


def _build_workflow_outcome(
    run: WorkflowRun,
    approvals: List[WorkflowApproval],
    action_records: List[WorkflowActionRecord],
    feedback: Optional[TrafficEventFeedback] = None,
) -> Dict[str, Any]:
    action_status_counts: Dict[str, int] = {}
    for record in action_records:
        action_status_counts[record.status.value] = action_status_counts.get(record.status.value, 0) + 1
    state = run.state if isinstance(run.state, dict) else {}
    errors = state.get("errors") if isinstance(state.get("errors"), list) else []
    audit_events = state.get("auditEvents") if isinstance(state.get("auditEvents"), list) else []
    return {
        "source": "workflow_runs + workflow_approvals + workflow_action_records",
        "workflowRunId": run.run_id,
        "definitionId": run.definition_id,
        "sessionId": run.session_id,
        "finalStatus": run.status.value,
        "systemTerminalStatus": True,
        "startedAt": run.started_at,
        "completedAt": run.completed_at,
        "updatedAt": run.updated_at,
        "currentNodeId": run.current_node_id,
        "approvalCounts": _approval_counts(approvals),
        "actionCounts": action_status_counts,
        "actions": [
            {
                "actionId": record.action_id,
                "nodeId": record.node_id,
                "actionType": record.action_type,
                "status": record.status.value,
                "attempt": int(record.attempt or 0),
                "retryCount": max(0, int(record.attempt or 0) - 1),
                "reconciliationAttempts": int(record.reconciliation_attempts or 0),
                "lastReconciledAt": record.last_reconciled_at or None,
                "error": _short_text(record.error),
                "result": _compact_json(record.result, max_items=20),
                "createdAt": record.created_at,
                "completedAt": record.completed_at,
            }
            for record in action_records
        ],
        "errors": _compact_json(errors, max_items=10),
        "auditEventTypes": [
            str(item.get("type") or item.get("eventType") or "")
            for item in audit_events[:20]
            if isinstance(item, dict)
        ],
        "businessOutcome": {
            "status": (
                feedback.effectiveness.value
                if feedback
                else "unknown_without_external_evidence"
            ),
            "eventOutcome": feedback.event_outcome.value if feedback else EventOutcome.UNKNOWN.value,
            "source": "operator_feedback" if feedback else "none",
            "confirmed": bool(
                feedback
                and feedback.effectiveness != FeedbackEffectiveness.UNKNOWN
                and feedback.event_outcome != EventOutcome.UNKNOWN
            ),
            "reason": (
                None
                if feedback
                else "workflow terminal status is a system execution outcome only"
            ),
        },
    }


def _build_recommendation_feedback(
    run: WorkflowRun,
    plan_facts: Dict[str, Any],
    approvals: List[WorkflowApproval],
    action_records: List[WorkflowActionRecord],
) -> Dict[str, Any]:
    decisions = [approval.decision for approval in approvals]
    approved_count = sum(
        decision == ApprovalDecision.APPROVED for decision in decisions
    )
    edited_count = sum(decision == ApprovalDecision.EDITED for decision in decisions)
    rejected_count = sum(
        decision == ApprovalDecision.REJECTED for decision in decisions
    )
    decided_count = approved_count + edited_count + rejected_count
    if rejected_count and rejected_count == decided_count:
        status = "rejected"
    elif edited_count or (rejected_count and (approved_count or edited_count)):
        status = "modified"
    elif approved_count or (not approvals and action_records):
        status = "accepted"
    elif run.status.value == "rejected":
        status = "rejected"
    else:
        status = "unknown"

    plan_actions = [
        {
            "actionStepId": step.get("stepId"),
            "actionType": step.get("actionType"),
            "params": step.get("params") or {},
        }
        for step in (plan_facts.get("steps") or [])
        if isinstance(step, dict) and step.get("actionType")
    ]
    approval_proposals = [
        item
        for approval in approvals
        for item in approval.proposed_actions
        if isinstance(item, dict)
    ]
    final_actions: List[Dict[str, Any]] = []
    modifications: List[Dict[str, Any]] = []
    for approval in approvals:
        adopted = (
            approval.edited_actions
            if approval.decision == ApprovalDecision.EDITED
            else approval.proposed_actions
            if approval.decision == ApprovalDecision.APPROVED
            else []
        )
        final_actions.extend(_compact_action(item) for item in adopted)
        modifications.extend(
            {
                "approvalId": approval.approval_id,
                **item,
            }
            for item in _structured_action_diff(
                approval.proposed_actions,
                adopted,
            )
        )
    # Approval records describe the human-adopted subset, while action records
    # are the durable source for actions that actually ran without an approval
    # gate.  Keep both: a mixed Workflow must not lose its automatic actions,
    # and a rejected proposal only enters the final plan if it nevertheless has
    # a real execution record.
    final_actions.extend(
        _compact_action(
            {
                "actionStepId": record.node_id,
                "actionType": record.action_type,
                "params": record.params,
            }
        )
        for record in action_records
    )

    return {
        "status": status,
        "source": "workflow_approvals + frozen_plan + action_records",
        "originalRecommendation": {
            "planId": plan_facts.get("planId"),
            "planVersion": plan_facts.get("version"),
            # Durable approval proposals carry the concrete review-time params
            # for gated actions; frozen Plan actions fill in the remainder.
            "actionRefs": _dedupe_actions(approval_proposals + plan_actions),
        },
        "finalPlan": {
            "planId": plan_facts.get("planId"),
            # The Workflow is bound to its frozen Plan snapshot.  A later Plan
            # revision must never be relabelled as this run's adopted version.
            "planVersion": plan_facts.get("version"),
            "actions": _dedupe_actions(final_actions),
        },
        "modifications": modifications[:100],
        "approvalIds": [approval.approval_id for approval in approvals],
        "reviewers": sorted({
            _short_text(approval.reviewer, max_text=200)
            for approval in approvals
            if approval.reviewer
        }),
        "rejectionReasons": [
            {
                "approvalId": approval.approval_id,
                "reasonCode": approval.reason_code.value,
            }
            for approval in approvals
            if approval.decision == ApprovalDecision.REJECTED
        ],
        "decidedAt": max(
            (approval.decided_at for approval in approvals if approval.decided_at),
            default=None,
        ),
        "counts": {
            "accepted": approved_count,
            "modified": edited_count,
            "rejected": rejected_count,
        },
    }


def _build_action_feedback(
    plan_facts: Dict[str, Any],
    approvals: List[WorkflowApproval],
    action_records: List[WorkflowActionRecord],
    feedback: Optional[TrafficEventFeedback],
    workflow_repo: SQLiteWorkflowRepository,
) -> List[Dict[str, Any]]:
    proposed = [
        {
            "actionStepId": step.get("stepId"),
            "actionType": step.get("actionType"),
            "params": step.get("params") or {},
        }
        for step in (plan_facts.get("steps") or [])
        if isinstance(step, dict) and step.get("actionType")
    ]
    for approval in approvals:
        proposed.extend(approval.proposed_actions)
    proposed = _dedupe_actions(proposed)

    final_actions: List[Dict[str, Any]] = []
    for approval in approvals:
        if approval.decision == ApprovalDecision.APPROVED:
            final_actions.extend(approval.proposed_actions)
        elif approval.decision == ApprovalDecision.EDITED:
            final_actions.extend(approval.edited_actions)
    # An approval covers only its gated actions.  Automatic actions still need
    # to appear in the final projection, and the action record is the durable
    # proof that they were actually executed.  Rejected, unexecuted proposals
    # are intentionally absent because they have no adopted action or record.
    final_actions.extend(
        {
            "actionStepId": record.node_id,
            "actionType": record.action_type,
            "params": record.params,
        }
        for record in action_records
    )
    final_actions = _dedupe_actions(final_actions)

    entries: List[Dict[str, Any]] = []
    consumed_record_ids: set[str] = set()
    for index, proposed_action in enumerate(proposed):
        final_action = _find_matching_action(proposed_action, final_actions, index)
        record = _find_matching_record(
            final_action or proposed_action,
            action_records,
            consumed_record_ids,
        )
        if record:
            consumed_record_ids.add(record.action_id)
        entries.append(_project_action_feedback(
            proposed_action=proposed_action,
            final_action=final_action,
            record=record,
            feedback=feedback,
            workflow_repo=workflow_repo,
        ))
    for record in action_records:
        if record.action_id in consumed_record_ids:
            continue
        final_action = {
            "actionStepId": record.node_id,
            "actionType": record.action_type,
            "params": record.params,
        }
        entries.append(_project_action_feedback(
            proposed_action=None,
            final_action=final_action,
            record=record,
            feedback=feedback,
            workflow_repo=workflow_repo,
        ))
    return entries[:100]


def _project_action_feedback(
    *,
    proposed_action: Optional[Dict[str, Any]],
    final_action: Optional[Dict[str, Any]],
    record: Optional[WorkflowActionRecord],
    feedback: Optional[TrafficEventFeedback],
    workflow_repo: SQLiteWorkflowRepository,
) -> Dict[str, Any]:
    status = record.status.value if record else (
        "rejected" if proposed_action and not final_action else "approved"
    )
    attempts = workflow_repo.list_action_attempts(record.action_id) if record else []
    terminal_execution_statuses = {
        ActionStatus.SUCCEEDED,
        ActionStatus.FAILED,
        ActionStatus.UNKNOWN,
        ActionStatus.CANCELLED,
    }
    assessment = None
    if feedback and record:
        assessment = next(
            (
                item for item in feedback.action_assessments
                if isinstance(item, dict)
                and item.get("actionExecutionId") == record.action_id
            ),
            None,
        )
    if assessment:
        action_effectiveness = str(
            assessment.get("effectiveness") or FeedbackEffectiveness.UNKNOWN.value
        )
        business_reason = str(
            assessment.get("reasonCode") or "NONE"
        )
        effectiveness_source = "operator_action_feedback"
    elif feedback and record and feedback.action_execution_id == record.action_id:
        action_effectiveness = feedback.effectiveness.value
        business_reason = feedback.reason_code.value
        effectiveness_source = "operator_event_feedback_legacy_link"
    else:
        action_effectiveness = FeedbackEffectiveness.UNKNOWN.value
        business_reason = "NONE"
        effectiveness_source = "none"
    return {
        "actionExecutionId": record.action_id if record else None,
        "actionStepId": (
            (final_action or {}).get("actionStepId")
            or (final_action or {}).get("stepId")
            or (proposed_action or {}).get("actionStepId")
            or (proposed_action or {}).get("stepId")
            or (record.node_id if record else None)
        ),
        "actionType": (
            _action_type(final_action)
            or _action_type(proposed_action)
            or (record.action_type if record else None)
        ),
        "proposed": proposed_action is not None,
        "approved": final_action is not None,
        "executed": bool(record and record.status in terminal_execution_statuses),
        "succeeded": bool(record and record.status == ActionStatus.SUCCEEDED),
        "failed": bool(record and record.status == ActionStatus.FAILED),
        "cancelled": bool(record and record.status == ActionStatus.CANCELLED),
        "blocked": bool(record and record.status == ActionStatus.BLOCKED),
        "status": status,
        "retryCount": max(
            max(0, int(record.attempt or 0) - 1) if record else 0,
            max(0, len(attempts) - 1),
        ),
        "enteredUnknown": bool(
            record and (
                record.status == ActionStatus.UNKNOWN
                or any(attempt.status == ActionStatus.UNKNOWN for attempt in attempts)
            )
        ),
        "reconciled": bool(
            record
            and (record.reconciliation_attempts or record.last_reconciled_at)
        ),
        "reconciliationAttempts": int(
            record.reconciliation_attempts or 0
        ) if record else 0,
        "humanReplaced": bool(
            proposed_action
            and final_action
            and _compact_action(proposed_action) != _compact_action(final_action)
        ),
        "businessEffectiveness": action_effectiveness,
        "businessEffectivenessSource": effectiveness_source,
        "businessReasonCode": business_reason,
        "proposedAction": _compact_action(proposed_action) if proposed_action else None,
        "finalAction": _compact_action(final_action) if final_action else None,
        "error": _short_text(record.error) if record else None,
    }


def _build_event_outcome(
    run: WorkflowRun,
    event_snapshot: Dict[str, Any],
    action_records: List[WorkflowActionRecord],
    feedback: Optional[TrafficEventFeedback],
) -> Dict[str, Any]:
    counts: Dict[str, int] = {}
    for record in action_records:
        counts[record.status.value] = counts.get(record.status.value, 0) + 1
    operator = {
        "outcome": feedback.event_outcome.value if feedback else EventOutcome.UNKNOWN.value,
        "effectiveness": (
            feedback.effectiveness.value if feedback else FeedbackEffectiveness.UNKNOWN.value
        ),
        "reasonCode": feedback.reason_code.value if feedback else "NONE",
        "comment": _short_text(feedback.comment, max_text=1000) if feedback else None,
        "reviewer": _short_text(feedback.reviewer, max_text=200) if feedback else None,
        "assessedAt": feedback.updated_at if feedback else None,
        "source": "operator_feedback" if feedback else "none",
    }
    return {
        "systemAssessment": {
            "workflowStatus": run.status.value,
            "eventStatus": event_snapshot.get("status"),
            "eventMarkedHandled": event_snapshot.get("status") in {
                "已处置", "待复盘", "已归档"
            },
            "actionStatusCounts": counts,
            "source": "workflow_runs + event_records + workflow_action_records",
        },
        "operatorAssessment": operator,
        "businessOutcomeConfirmed": bool(
            feedback
            and feedback.event_outcome != EventOutcome.UNKNOWN
            and feedback.effectiveness != FeedbackEffectiveness.UNKNOWN
        ),
        "workflowCompletionEqualsBusinessEffect": False,
    }


def _build_lessons(
    run: WorkflowRun,
    human_decisions: List[Dict[str, Any]],
    action_records: List[WorkflowActionRecord],
    agent_facts: Dict[str, Any],
    plan_facts: Dict[str, Any],
) -> List[Dict[str, Any]]:
    lessons: List[Dict[str, Any]] = []
    for item in agent_facts.get("rejectedActions") or []:
        lessons.append({
            "type": "agent_action_rejected",
            "source": "agentFacts.rejectedActions",
            "actionType": item.get("actionType"),
            "reason": item.get("reason"),
        })
    for decision in human_decisions:
        if decision.get("decision") == ApprovalDecision.REJECTED.value:
            lessons.append({
                "type": "human_approval_rejected",
                "source": "workflow_approvals",
                "approvalId": decision.get("approvalId"),
                "reasonCode": decision.get("reasonCode"),
            })
        if decision.get("manualAdjustment"):
            lessons.append({
                "type": "human_edited_action",
                "source": "workflow_approvals.edited_actions",
                "approvalId": decision.get("approvalId"),
            })
    for record in action_records:
        if record.status == ActionStatus.FAILED:
            lessons.append({
                "type": "action_failed",
                "source": "workflow_action_records",
                "actionId": record.action_id,
                "actionType": record.action_type,
                "error": _short_text(record.error),
            })
    if run.status.value in {"failed", "rejected", "cancelled"}:
        lessons.append({
            "type": f"workflow_{run.status.value}",
            "source": "workflow_runs.status",
            "workflowRunId": run.run_id,
        })
    if int(plan_facts.get("replanCount") or 0) > 0:
        lessons.append({
            "type": "replan_occurred",
            "source": "workflow_definition_versions",
            "replanCount": plan_facts.get("replanCount"),
        })
    return lessons


def _quality_status(
    *,
    run: WorkflowRun,
    feedback: Optional[TrafficEventFeedback],
    event_outcome: Dict[str, Any],
    action_records: List[WorkflowActionRecord],
) -> CaseMemoryQuality:
    # Workflow completion and HTTP success are system facts, not proof that the
    # traffic intervention worked.  Without an operator outcome this case must
    # remain explicitly unverified.
    if feedback is None or (
        feedback.effectiveness == FeedbackEffectiveness.UNKNOWN
        and feedback.event_outcome == EventOutcome.UNKNOWN
    ):
        return CaseMemoryQuality.UNVERIFIED

    if (
        feedback.effectiveness == FeedbackEffectiveness.INEFFECTIVE
        or feedback.event_outcome in {
            EventOutcome.UNRESOLVED,
            EventOutcome.CANCELLED,
        }
    ):
        return CaseMemoryQuality.FAILED_OUTCOME

    if (
        feedback.effectiveness == FeedbackEffectiveness.PARTIALLY_EFFECTIVE
        or feedback.event_outcome == EventOutcome.PARTIALLY_RESOLVED
    ):
        return CaseMemoryQuality.PARTIAL_SUCCESS

    if feedback.lifecycle != FeedbackLifecycle.COMPLETE:
        return CaseMemoryQuality.INCOMPLETE

    unsafe_action_statuses = {
        ActionStatus.FAILED,
        ActionStatus.UNKNOWN,
        ActionStatus.CANCELLED,
        ActionStatus.BLOCKED,
        ActionStatus.RUNNING,
        ActionStatus.EXECUTING,
        ActionStatus.PENDING,
    }
    if (
        feedback.effectiveness == FeedbackEffectiveness.EFFECTIVE
        and feedback.event_outcome == EventOutcome.RESOLVED
        and run.status.value == "completed"
        and not any(record.status in unsafe_action_statuses for record in action_records)
        and event_outcome.get("businessOutcomeConfirmed") is True
    ):
        return CaseMemoryQuality.VERIFIED_SUCCESS
    return CaseMemoryQuality.PARTIAL_SUCCESS


def _build_generated_summary(event_snapshot: Dict[str, Any], run: WorkflowRun) -> str:
    road = str(event_snapshot.get("roadName") or "未知道路").strip()
    event_type = str(event_snapshot.get("eventTypeCn") or event_snapshot.get("eventType") or "交通事件").strip()
    return f"{road}{event_type}: workflow ended with {run.status.value}"


def _approval_counts(approvals: List[WorkflowApproval]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for approval in approvals:
        counts[approval.decision.value] = counts.get(approval.decision.value, 0) + 1
    return counts


def _action_type(action: Any) -> str:
    if not isinstance(action, dict):
        return ""
    return str(action.get("actionType") or action.get("action_type") or "").strip()


def _action_identity(action: Any, index: int = 0) -> str:
    if not isinstance(action, dict):
        return f"index:{index}"
    for key in ("actionStepId", "targetActionStepId", "stepId"):
        value = str(action.get(key) or "").strip()
        if value:
            return f"step:{value}"
    action_type = _action_type(action)
    return f"type:{action_type}" if action_type else f"index:{index}"


def _dedupe_actions(actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for index, action in enumerate(actions):
        if not isinstance(action, dict):
            continue
        compact = _compact_action(action)
        identity = _action_identity(compact, index)
        # Same-type actions can be legitimate when they bind different params;
        # include the compact value in that fallback identity.
        if identity.startswith("type:"):
            identity += ":" + json.dumps(
                compact.get("params") or {},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        if identity in seen:
            continue
        seen.add(identity)
        result.append(compact)
    return result


def _find_matching_action(
    action: Dict[str, Any],
    candidates: List[Dict[str, Any]],
    index: int = 0,
) -> Optional[Dict[str, Any]]:
    identity = _action_identity(action, index)
    if identity.startswith("step:"):
        for candidate_index, candidate in enumerate(candidates):
            if _action_identity(candidate, candidate_index) == identity:
                return candidate
        action_type = _action_type(action)
        if action_type:
            # Legacy edited approvals may omit their step id.  Permit that
            # fallback, but never bind this proposal to a different explicit
            # step merely because the action types happen to match.
            for candidate_index, candidate in enumerate(candidates):
                if (
                    not _action_identity(candidate, candidate_index).startswith("step:")
                    and _action_type(candidate) == action_type
                ):
                    return candidate
        return None
    action_type = _action_type(action)
    if action_type:
        for candidate in candidates:
            if _action_type(candidate) == action_type:
                return candidate
        # A typed rejected proposal must never be paired positionally with an
        # unrelated automatic action merely because both occupy the same list
        # index.  Positional recovery is reserved for legacy untyped payloads.
        return None
    return candidates[index] if index < len(candidates) else None


def _find_matching_record(
    action: Dict[str, Any],
    records: List[WorkflowActionRecord],
    consumed: set[str],
) -> Optional[WorkflowActionRecord]:
    step_id = str(
        action.get("actionStepId")
        or action.get("targetActionStepId")
        or action.get("stepId")
        or ""
    ).strip()
    action_type = _action_type(action)
    for record in records:
        if record.action_id not in consumed and step_id and record.node_id == step_id:
            return record
    for record in records:
        if (
            record.action_id not in consumed
            and action_type
            and record.action_type == action_type
        ):
            return record
    return None


def _structured_action_diff(
    proposed_actions: List[Dict[str, Any]],
    final_actions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Return bounded field-level edits, never a lossy string diff."""

    proposed = [item for item in proposed_actions if isinstance(item, dict)]
    final = [item for item in final_actions if isinstance(item, dict)]
    remaining = list(enumerate(final))
    diffs: List[Dict[str, Any]] = []
    for proposed_index, raw_proposed in enumerate(proposed):
        match_position: Optional[int] = None
        proposed_identity = _action_identity(raw_proposed, proposed_index)
        for position, (final_index, raw_final) in enumerate(remaining):
            final_identity = _action_identity(raw_final, final_index)
            if (
                proposed_identity == final_identity
                or (
                    _action_type(raw_proposed)
                    and _action_type(raw_proposed) == _action_type(raw_final)
                )
            ):
                match_position = position
                break
        if match_position is None:
            diffs.append({
                "actionKey": proposed_identity,
                "field": "$action",
                "proposedValue": _compact_action(raw_proposed),
                "finalValue": None,
                "modificationType": "removed",
            })
            continue
        final_index, raw_final = remaining.pop(match_position)
        before = _flatten_action(_compact_action(raw_proposed))
        after = _flatten_action(_compact_action(raw_final))
        for field_name in sorted(set(before) | set(after)):
            old = before.get(field_name)
            new = after.get(field_name)
            if old == new:
                continue
            if field_name not in before:
                kind = "added"
            elif field_name not in after:
                kind = "removed"
            else:
                kind = "changed"
            diffs.append({
                "actionKey": _action_identity(raw_final, final_index),
                "field": field_name,
                "proposedValue": old,
                "finalValue": new,
                "modificationType": kind,
            })
    for final_index, raw_final in remaining:
        diffs.append({
            "actionKey": _action_identity(raw_final, final_index),
            "field": "$action",
            "proposedValue": None,
            "finalValue": _compact_action(raw_final),
            "modificationType": "added",
        })
    return diffs[:100]


def _flatten_action(value: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    flattened: Dict[str, Any] = {}
    for key, nested in value.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(nested, dict):
            flattened.update(_flatten_action(nested, path))
        else:
            flattened[path] = _compact_json(nested)
    return flattened


def _compact_action(action: Any) -> Dict[str, Any]:
    if not isinstance(action, dict):
        return {"value": _short_text(action)}
    result: Dict[str, Any] = {}
    for key in (
        "actionType",
        "action_type",
        "actionStepId",
        "targetActionStepId",
        "stepId",
        "reason",
        "status",
    ):
        if key in action:
            result[key] = _compact_json(action[key])
    params = action.get("params") or action.get("paramsTemplate") or action.get("parameterHints")
    if isinstance(params, dict):
        result["params"] = _compact_json(_redact_sensitive(params), max_items=20)
    return result


def _compact_json(value: Any, max_items: int = 50, max_text: int = 500) -> Any:
    value = sanitize_public_value(value)
    if isinstance(value, dict):
        items = list(value.items())[:max_items]
        return {str(k): _compact_json(v, max_items=max_items, max_text=max_text) for k, v in items}
    if isinstance(value, list):
        return [_compact_json(item, max_items=max_items, max_text=max_text) for item in value[:max_items]]
    return _short_text(value, max_text=max_text)


def _redact_sensitive(value: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, item in value.items():
        key_text = str(key)
        if any(token in key_text.lower() for token in ("secret", "token", "password", "apikey", "api_key")):
            out[key_text] = "[redacted]"
        elif isinstance(item, dict):
            out[key_text] = _redact_sensitive(item)
        else:
            out[key_text] = item
    return out


def _parse_json(value: Any, default: Any) -> Any:
    if not isinstance(value, str):
        return value if value is not None else default
    try:
        return json.loads(value) if value else default
    except json.JSONDecodeError:
        return default


def _pick(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def _short_text(value: Any, max_text: int = 500) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float, bool)):
        return value
    text = sanitize_public_text(value)
    if len(text) <= max_text:
        return text
    return text[: max_text - 1] + "..."


def _latest_timestamp(values: List[Any]) -> Optional[str]:
    latest: Optional[tuple[datetime, str]] = None
    for value in values:
        if not isinstance(value, str) or not value.strip():
            continue
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        parsed = parsed.astimezone(timezone.utc)
        candidate = (parsed, value)
        if latest is None or candidate[0] > latest[0]:
            latest = candidate
    return latest[1] if latest else None


def _source_fact_horizon(values: List[Any]) -> str:
    """Return a trustworthy horizon or fail closed at projection time."""

    present = [value for value in values if value is not None and value != ""]
    if not present:
        return utc_now_iso()
    for value in present:
        if not isinstance(value, str):
            return utc_now_iso()
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return utc_now_iso()
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
    return _latest_timestamp(present) or utc_now_iso()
