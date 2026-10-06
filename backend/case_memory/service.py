"""Traffic case memory service layer."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import sqlite3
from typing import Any, Dict, List, Optional

from backend.case_memory.builder import (
    TrafficCaseBuilder,
    _compact_event_snapshot,
    _source_collaboration_run_id,
)
from backend.case_memory.models import (
    CaseBuildResult,
    CaseMemoryError,
    CaseMemoryQuality,
    EventOutcome,
    FeedbackEffectiveness,
    FeedbackLifecycle,
    FeedbackReasonCode,
    TrafficCaseMemory,
    TrafficEventFeedback,
    build_feedback_id,
    utc_now_iso,
)
from backend.case_memory.repository import CaseProjectionConflict, SQLiteCaseMemoryRepository
from backend.regional.repository import SQLiteRegionalRepository
from backend.tools.db_tools import get_event_by_id
from backend.workflow.action_execution import sanitize_public_text
from backend.workflow.repository import SQLiteWorkflowRepository


class TrafficCaseMemoryService:
    def __init__(
        self,
        *,
        repository: Optional[SQLiteCaseMemoryRepository] = None,
        builder: Optional[TrafficCaseBuilder] = None,
        regional_repo: Optional[SQLiteRegionalRepository] = None,
    ):
        self.repository = repository or SQLiteCaseMemoryRepository()
        self.regional_repo = regional_repo or SQLiteRegionalRepository()
        self.builder = builder or TrafficCaseBuilder(
            regional_repo=self.regional_repo,
            feedback_repository=self.repository,
        )

    def build_from_workflow_run(self, run_id: str, *, rebuild: bool = False) -> CaseBuildResult:
        for _ in range(3):
            existing = self.repository.get_case_by_source_workflow_run_id(run_id)
            if existing and not rebuild:
                durable_feedback = self.repository.get_feedback(
                    existing.event_id,
                    run_id,
                )
                if durable_feedback and int(existing.feedback_revision or 0) != int(
                    durable_feedback.revision
                ):
                    # A previous best-effort projection may have been pending.
                    # Any later idempotent build request acts as a safe repair.
                    return self.refresh_feedback_projection(run_id)
                return CaseBuildResult(case=existing, created=False, rebuilt=False)
            built = self.builder.build_from_workflow_run(run_id)
            expected_revision = int(built.feedback_revision or 0)
            try:
                if existing:
                    # A full explicit rebuild may observe a corrected location,
                    # later reconciliation, or another mutable system fact.
                    # Record that horizon so replay cannot see it retroactively.
                    built.system_updated_at = _latest_timestamp(
                        built.system_updated_at,
                        utc_now_iso(),
                    )
                    case = self.repository.update_case_preserving_identity(
                        existing,
                        built,
                        expected_feedback_revision=expected_revision,
                    )
                    return CaseBuildResult(case=case, created=False, rebuilt=True)
                case = self.repository.insert_case(
                    built,
                    expected_feedback_revision=expected_revision,
                )
                return CaseBuildResult(case=case, created=True, rebuilt=False)
            except CaseProjectionConflict:
                # Feedback changed after the builder read it.  Re-read the full
                # source chain so an older projection can never overwrite it.
                continue
            except sqlite3.IntegrityError:
                # Another terminal hook may have inserted the same Run between
                # our read and write.  A retry returns or rebuilds that row.
                continue
        raise CaseMemoryError(
            "CASE_PROJECTION_CONFLICT",
            "source facts changed repeatedly while Case Memory was being built",
            status_code=409,
        )

    def refresh_feedback_projection(self, run_id: str) -> CaseBuildResult:
        """Apply the latest Feedback revision without rewriting frozen system facts.

        The revision check closes the lost-update window between reading Feedback
        and writing its derived Case projection.  A bounded retry is sufficient
        because each attempt re-reads the durable latest row.
        """

        for _ in range(3):
            built = self.builder.build_from_workflow_run(run_id)
            expected_revision = built.feedback_revision
            if expected_revision is None:
                raise CaseMemoryError(
                    "CASE_FEEDBACK_NOT_FOUND",
                    "feedback projection requires a durable Feedback revision",
                    status_code=409,
                )
            existing = self.repository.get_case_by_source_workflow_run_id(run_id)
            try:
                if existing is None:
                    case = self.repository.insert_case(
                        built,
                        expected_feedback_revision=expected_revision,
                    )
                    return CaseBuildResult(case=case, created=True, rebuilt=False)
                merged = _merge_feedback_projection(existing, built)
                case = self.repository.update_case_preserving_identity(
                    existing,
                    merged,
                    expected_feedback_revision=expected_revision,
                )
                return CaseBuildResult(case=case, created=False, rebuilt=True)
            except CaseProjectionConflict:
                continue
            except sqlite3.IntegrityError:
                # Terminal auto-projection may have inserted the exact chain
                # between our read and write.  Retry and update that row.
                continue
        raise CaseMemoryError(
            "CASE_FEEDBACK_PROJECTION_CONFLICT",
            "feedback changed repeatedly while Case Memory was being refreshed",
            status_code=409,
        )

    def get_case(self, case_id: str) -> TrafficCaseMemory:
        case = self.repository.get_case(case_id)
        if not case:
            raise CaseMemoryError("CASE_NOT_FOUND", f"case not found: {case_id}", status_code=404)
        return case

    def query_cases(
        self,
        *,
        region_id: str,
        event_type: str,
        road_id: Optional[str] = None,
        intersection_id: Optional[str] = None,
        final_status: Optional[str] = None,
        quality_status: Optional[str] = None,
        as_of: Optional[str] = None,
        limit: int = 5,
        for_agent: bool = False,
    ) -> Dict[str, Any]:
        if not region_id:
            raise CaseMemoryError("REGION_ID_REQUIRED", "regionId is required", status_code=422)
        if not event_type:
            raise CaseMemoryError("EVENT_TYPE_REQUIRED", "eventType is required", status_code=422)
        result = self.repository.query_cases(
            region_id=region_id,
            event_type=event_type,
            road_id=road_id,
            intersection_id=intersection_id,
            final_status=final_status,
            quality_status=quality_status,
            as_of=as_of,
            limit=limit,
            for_agent=for_agent,
        )
        if for_agent and as_of:
            result["cases"] = [
                _mask_case_feedback_as_of(case, str(as_of))
                for case in result["cases"]
            ]
        return result

    def get_case_context_for_event(self, event_id: str, *, limit: int = 5) -> Dict[str, Any]:
        event = get_event_by_id(event_id)
        if not event:
            raise CaseMemoryError("EVENT_NOT_FOUND", f"event not found: {event_id}", status_code=404)
        binding = self.regional_repo.get_active_event_location_binding(event_id)
        if not binding or not binding.get("regionId"):
            raise CaseMemoryError(
                "EVENT_CANONICAL_REGION_MISSING",
                "case context requires a resolved canonical event location",
                status_code=409,
            )
        snapshot = _compact_event_snapshot(event)
        event_type = str(snapshot.get("eventType") or "").strip()
        if not event_type:
            raise CaseMemoryError(
                "EVENT_TYPE_REQUIRED",
                "eventType is required for case context retrieval",
                status_code=422,
            )
        as_of = snapshot.get("createdAt")
        if not _valid_timestamp(as_of):
            raise CaseMemoryError(
                "INVALID_EVENT_TIMESTAMP",
                "strict-past case retrieval requires a valid Event createdAt timestamp",
                status_code=409,
            )
        result = self.repository.find_context_candidates(
            region_id=str(binding["regionId"]),
            event_type=event_type,
            road_id=binding.get("roadId"),
            intersection_id=binding.get("intersectionId"),
            as_of=as_of,
            limit=limit,
            exclude_event_id=event_id,
        )
        projected = [
            _retrieval_projection(
                case,
                metadata=(result.get("retrievalMetadata") or {}).get(case.case_id) or {},
                road_id=binding.get("roadId"),
                intersection_id=binding.get("intersectionId"),
                as_of=str(as_of),
            )
            for case in result["cases"]
        ]
        positive = [item for item in projected if item["experienceType"] == "positive"]
        partial = [item for item in projected if item["experienceType"] == "partial"]
        negative = [item for item in projected if item["experienceType"] == "negative"]
        unverified = [item for item in projected if item["experienceType"] == "unverified"]
        return {
            "eventId": event_id,
            "regionId": binding["regionId"],
            "eventType": event_type,
            "asOf": as_of,
            "location": {
                "roadId": binding.get("roadId"),
                "intersectionId": binding.get("intersectionId"),
            },
            # Combined list retained for existing consumers and holdout tooling.
            "cases": projected,
            "positiveCases": positive,
            "partialCases": partial,
            "negativeCases": negative,
            "unverifiedCases": unverified,
            "total": result["total"],
            "limit": result["limit"],
            "retrievalPolicy": {
                "crossRegionBlocked": True,
                "futureCaseLeakageBlocked": True,
                "currentEventExcluded": True,
                "futureFeedbackLeakageBlocked": True,
                "futureSystemProjectionLeakageBlocked": True,
                "canonicalEventCaseOnly": True,
                "negativeEvidenceReservedWhenAvailable": limit >= 2,
                "roadNameUsedAsIdentity": False,
                "ranking": ["canonical_location_tier", "outcome_tier", "recency"],
                "semanticSimilarityUsed": False,
                "qualityStatuses": [
                    "VERIFIED_SUCCESS", "PARTIAL_SUCCESS", "FAILED_OUTCOME",
                    "INCOMPLETE", "UNVERIFIED", "validated", "partial",
                ],
            },
        }


class TrafficFeedbackService:
    """Persist minimal operator feedback and rebuild the existing Case Memory."""

    def __init__(
        self,
        *,
        repository: Optional[SQLiteCaseMemoryRepository] = None,
        workflow_repo: Optional[SQLiteWorkflowRepository] = None,
        regional_repo: Optional[SQLiteRegionalRepository] = None,
        case_service: Optional[TrafficCaseMemoryService] = None,
    ):
        self.repository = repository or SQLiteCaseMemoryRepository()
        self.workflow_repo = workflow_repo or SQLiteWorkflowRepository()
        self.regional_repo = regional_repo or SQLiteRegionalRepository()
        self.case_service = case_service or TrafficCaseMemoryService(
            repository=self.repository,
            regional_repo=self.regional_repo,
            builder=TrafficCaseBuilder(
                workflow_repo=self.workflow_repo,
                regional_repo=self.regional_repo,
                feedback_repository=self.repository,
            ),
        )

    def submit_feedback(
        self,
        *,
        event_id: str,
        workflow_run_id: str,
        event_outcome: EventOutcome,
        effectiveness: FeedbackEffectiveness,
        reason_code: FeedbackReasonCode = FeedbackReasonCode.NONE,
        comment: str = "",
        reviewer: str = "",
        approval_id: Optional[str] = None,
        action_execution_id: Optional[str] = None,
        action_assessments: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        if get_event_by_id(event_id) is None:
            raise CaseMemoryError(
                "EVENT_NOT_FOUND", f"event not found: {event_id}", status_code=404
            )
        run = self.workflow_repo.get_run(workflow_run_id)
        if run is None:
            raise CaseMemoryError(
                "WORKFLOW_RUN_NOT_FOUND",
                f"workflow run not found: {workflow_run_id}",
                status_code=404,
            )
        state = run.state if isinstance(run.state, dict) else {}
        current = state.get("currentEvent") or state.get("current_event") or {}
        run_event_id = str(
            current.get("eventId") or current.get("event_id") or ""
        ).strip() if isinstance(current, dict) else ""
        if run_event_id != event_id:
            raise CaseMemoryError(
                "FEEDBACK_WORKFLOW_EVENT_MISMATCH",
                "workflowRunId does not belong to the requested Event",
                status_code=409,
            )
        if not run.is_terminal():
            raise CaseMemoryError(
                "FEEDBACK_WORKFLOW_NOT_TERMINAL",
                "operator outcome feedback requires a terminal Workflow",
                status_code=409,
            )

        plan, _ = self.case_service.builder._recover_source_plan(run, event_id)
        approvals = self.workflow_repo.list_approvals(workflow_run_id)
        selected_approval = None
        if approval_id:
            selected_approval = self.workflow_repo.get_approval(approval_id)
            if selected_approval is None or selected_approval.run_id != workflow_run_id:
                raise CaseMemoryError(
                    "FEEDBACK_APPROVAL_MISMATCH",
                    "approvalId does not belong to workflowRunId",
                    status_code=409,
                )
        else:
            decided = [item for item in approvals if item.decided_at]
            selected_approval = decided[-1] if decided else None

        selected_action = None
        if action_execution_id:
            selected_action = self.workflow_repo.get_action_record(action_execution_id)
            if (
                selected_action is None
                or selected_action.run_id != workflow_run_id
                or (selected_action.event_id and selected_action.event_id != event_id)
            ):
                raise CaseMemoryError(
                    "FEEDBACK_ACTION_MISMATCH",
                    "actionExecutionId does not belong to the Event/Workflow chain",
                    status_code=409,
                )

        action_assessment_mode = "replace"
        if action_assessments is None:
            action_assessment_mode = "merge" if action_execution_id else "preserve"
        normalized_action_assessments: List[Dict[str, Any]] = []
        seen_action_ids: set[str] = set()
        for raw in action_assessments or []:
            action_id = str(
                raw.get("actionExecutionId") or raw.get("action_execution_id") or ""
            ).strip()
            if not action_id or action_id in seen_action_ids:
                raise CaseMemoryError(
                    "FEEDBACK_ACTION_ASSESSMENT_INVALID",
                    "actionAssessments must contain unique actionExecutionId values",
                    status_code=422,
                )
            record = self.workflow_repo.get_action_record(action_id)
            if (
                record is None
                or record.run_id != workflow_run_id
                or (record.event_id and record.event_id != event_id)
            ):
                raise CaseMemoryError(
                    "FEEDBACK_ACTION_MISMATCH",
                    "actionAssessments contains an Action outside the Event/Workflow chain",
                    status_code=409,
                )
            try:
                action_effectiveness = FeedbackEffectiveness(
                    raw.get("effectiveness", FeedbackEffectiveness.UNKNOWN.value)
                )
                action_reason = FeedbackReasonCode(
                    raw.get("reasonCode", FeedbackReasonCode.NONE.value)
                )
            except ValueError as exc:
                raise CaseMemoryError(
                    "FEEDBACK_ACTION_ASSESSMENT_INVALID",
                    "actionAssessments contains an unsupported enum value",
                    status_code=422,
                ) from exc
            seen_action_ids.add(action_id)
            normalized_action_assessments.append({
                "actionExecutionId": action_id,
                "effectiveness": action_effectiveness.value,
                "reasonCode": action_reason.value,
            })

        if selected_action and selected_action.action_id not in seen_action_ids:
            normalized_action_assessments.append({
                "actionExecutionId": selected_action.action_id,
                "effectiveness": effectiveness.value,
                "reasonCode": reason_code.value,
            })

        lifecycle = _feedback_lifecycle(event_outcome, effectiveness)
        feedback = TrafficEventFeedback(
            feedback_id=build_feedback_id(event_id, workflow_run_id),
            event_id=event_id,
            workflow_run_id=workflow_run_id,
            agent_run_id=_source_collaboration_run_id(plan),
            plan_id=plan.planId if plan else None,
            plan_version=plan.version if plan else None,
            approval_id=(selected_approval.approval_id if selected_approval else None),
            action_execution_id=(selected_action.action_id if selected_action else None),
            action_assessments=normalized_action_assessments,
            event_outcome=event_outcome,
            effectiveness=effectiveness,
            reason_code=reason_code,
            comment=sanitize_public_text(comment)[:1000],
            reviewer=sanitize_public_text(reviewer)[:200],
            lifecycle=lifecycle,
        )
        saved, created = self.repository.upsert_feedback(
            feedback,
            action_assessment_mode=action_assessment_mode,
            preserve_action_execution_id=action_execution_id is None,
        )

        projection: Dict[str, Any]
        try:
            build = self.case_service.refresh_feedback_projection(workflow_run_id)
            projection = {
                "status": "updated",
                "caseId": build.case.case_id,
                "qualityStatus": build.case.quality_status.value,
                "feedbackLifecycle": build.case.feedback_lifecycle.value,
                "created": build.created,
            }
        except CaseMemoryError as exc:
            # Feedback remains durable even if canonical location/source data is
            # not yet sufficient to materialise the derived Case read model.
            projection = {
                "status": "pending",
                "code": exc.code,
            }
            _log_projection_pending(
                event_id=event_id,
                workflow_run_id=workflow_run_id,
                code=exc.code,
                error_type=type(exc).__name__,
            )
        except Exception as exc:
            # The Feedback commit is authoritative.  A transient read-model
            # failure is reported as pending rather than turning a successful
            # submission into a misleading client-visible rollback.
            projection = {
                "status": "pending",
                "code": "CASE_FEEDBACK_PROJECTION_FAILED",
            }
            _log_projection_pending(
                event_id=event_id,
                workflow_run_id=workflow_run_id,
                code="CASE_FEEDBACK_PROJECTION_FAILED",
                error_type=type(exc).__name__,
            )
        return {
            "feedback": saved.to_dict(),
            "created": created,
            "caseMemoryProjection": projection,
        }

    def get_feedback(
        self,
        event_id: str,
        *,
        workflow_run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        if get_event_by_id(event_id) is None:
            raise CaseMemoryError(
                "EVENT_NOT_FOUND", f"event not found: {event_id}", status_code=404
            )
        if workflow_run_id:
            item = self.repository.get_feedback(event_id, workflow_run_id)
            items = [item] if item else []
        else:
            items = self.repository.list_feedback_for_event(event_id)
        cases = self.repository.list_cases_for_source_event(event_id)
        return {
            "eventId": event_id,
            "feedback": [item.to_dict() for item in items],
            "caseMemories": [case.to_dict() for case in cases],
            "total": len(items),
        }


def _latest_timestamp(*values: Any) -> str:
    """Keep a projection horizon monotonic across rebuilds and clock skew."""

    latest_value = ""
    latest_time: Optional[datetime] = None
    for value in values:
        if not isinstance(value, str) or not value.strip():
            continue
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        if latest_time is None or parsed > latest_time:
            latest_time = parsed
            latest_value = value
    return latest_value


def _merge_feedback_projection(
    existing: TrafficCaseMemory,
    rebuilt: TrafficCaseMemory,
) -> TrafficCaseMemory:
    """Merge operator-owned fields while keeping the terminal system snapshot frozen."""

    if not existing.event_outcome or "systemAssessment" not in existing.event_outcome:
        # One-time upgrade for a pre-21.5 row.  Because current source state is
        # read now, strict-past must use this upgrade time as its system horizon.
        rebuilt.system_updated_at = _latest_timestamp(
            rebuilt.system_updated_at,
            utc_now_iso(),
        )
        return rebuilt

    merged = TrafficCaseMemory.from_dict(existing.to_dict())
    merged.quality_status = rebuilt.quality_status
    merged.feedback_lifecycle = rebuilt.feedback_lifecycle
    merged.feedback_updated_at = rebuilt.feedback_updated_at
    merged.feedback_revision = rebuilt.feedback_revision

    built_actions = {
        str(item.get("actionExecutionId")): item
        for item in rebuilt.action_feedback
        if isinstance(item, dict) and item.get("actionExecutionId")
    }
    for action in merged.action_feedback:
        if not isinstance(action, dict):
            continue
        latest = built_actions.get(str(action.get("actionExecutionId")))
        if not latest:
            continue
        for key in (
            "businessEffectiveness",
            "businessEffectivenessSource",
            "businessReasonCode",
        ):
            if key in latest:
                action[key] = deepcopy(latest[key])

    existing_event_outcome = deepcopy(existing.event_outcome)
    existing_event_outcome["operatorAssessment"] = deepcopy(
        rebuilt.event_outcome.get("operatorAssessment") or {}
    )
    existing_event_outcome["businessOutcomeConfirmed"] = bool(
        rebuilt.event_outcome.get("businessOutcomeConfirmed")
    )
    existing_event_outcome["workflowCompletionEqualsBusinessEffect"] = False
    merged.event_outcome = existing_event_outcome

    merged.workflow_outcome = deepcopy(existing.workflow_outcome)
    merged.workflow_outcome["businessOutcome"] = deepcopy(
        rebuilt.workflow_outcome.get("businessOutcome") or {}
    )
    merged.provenance = deepcopy(existing.provenance)
    merged.provenance["feedbackId"] = rebuilt.provenance.get("feedbackId")
    merged.provenance["feedbackSource"] = rebuilt.provenance.get("feedbackSource")
    merged.provenance["businessOutcomeInferred"] = False
    merged.system_updated_at = existing.system_updated_at or existing.completed_at
    return merged


def _strictly_before_timestamp(value: Any, as_of: Any) -> bool:
    try:
        left = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        right = datetime.fromisoformat(str(as_of).replace("Z", "+00:00"))
        return left < right
    except (TypeError, ValueError):
        return False


def _mask_case_feedback_as_of(
    case: TrafficCaseMemory,
    as_of: str,
) -> TrafficCaseMemory:
    if not case.feedback_updated_at or _strictly_before_timestamp(
        case.feedback_updated_at, as_of
    ):
        return case
    payload = deepcopy(case.to_dict())
    payload["qualityStatus"] = CaseMemoryQuality.UNVERIFIED.value
    payload["feedbackLifecycle"] = FeedbackLifecycle.PENDING.value
    payload["feedbackUpdatedAt"] = None
    payload["feedbackRevision"] = None
    operator = ((payload.get("eventOutcome") or {}).get("operatorAssessment") or {})
    operator.update({
        "outcome": EventOutcome.UNKNOWN.value,
        "effectiveness": FeedbackEffectiveness.UNKNOWN.value,
        "reasonCode": FeedbackReasonCode.NONE.value,
        "comment": None,
        "reviewer": None,
        "assessedAt": None,
        "source": "not_available_as_of",
    })
    payload.setdefault("eventOutcome", {})["operatorAssessment"] = operator
    payload["eventOutcome"]["businessOutcomeConfirmed"] = False
    business = (payload.get("workflowOutcome") or {}).get("businessOutcome")
    if isinstance(business, dict):
        business.update({
            "status": "unknown_without_external_evidence",
            "eventOutcome": EventOutcome.UNKNOWN.value,
            "source": "not_available_as_of",
            "confirmed": False,
        })
    for action in payload.get("actionFeedback") or []:
        if isinstance(action, dict):
            action["businessEffectiveness"] = FeedbackEffectiveness.UNKNOWN.value
            action["businessEffectivenessSource"] = "not_available_as_of"
            action["businessReasonCode"] = FeedbackReasonCode.NONE.value
    return TrafficCaseMemory.from_dict(payload)


def _feedback_lifecycle(
    event_outcome: EventOutcome,
    effectiveness: FeedbackEffectiveness,
) -> FeedbackLifecycle:
    known_outcome = event_outcome != EventOutcome.UNKNOWN
    known_effectiveness = effectiveness != FeedbackEffectiveness.UNKNOWN
    if known_outcome and known_effectiveness:
        return FeedbackLifecycle.COMPLETE
    if known_outcome or known_effectiveness:
        return FeedbackLifecycle.PARTIAL
    return FeedbackLifecycle.PENDING


def _valid_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def _retrieval_projection(
    case: TrafficCaseMemory,
    *,
    metadata: Dict[str, Any],
    road_id: Optional[str],
    intersection_id: Optional[str],
    as_of: str,
) -> Dict[str, Any]:
    projected = deepcopy(case.to_dict())
    effective_quality = str(
        metadata.get("effectiveQuality") or case.quality_status.value
    )
    feedback_visible = bool(metadata.get("feedbackVisible", True))
    if not feedback_visible:
        projected["qualityStatus"] = CaseMemoryQuality.UNVERIFIED.value
        projected["feedbackLifecycle"] = FeedbackLifecycle.PENDING.value
        projected["feedbackUpdatedAt"] = None
        projected["feedbackRevision"] = None
        projected["feedbackAvailability"] = {
            "status": "not_available_as_of",
            "asOf": as_of,
        }
        operator = ((projected.get("eventOutcome") or {}).get("operatorAssessment") or {})
        operator.update({
            "outcome": EventOutcome.UNKNOWN.value,
            "effectiveness": FeedbackEffectiveness.UNKNOWN.value,
            "reasonCode": FeedbackReasonCode.NONE.value,
            "comment": None,
            "reviewer": None,
            "assessedAt": None,
            "source": "not_available_as_of",
        })
        projected.setdefault("eventOutcome", {})["operatorAssessment"] = operator
        projected["eventOutcome"]["businessOutcomeConfirmed"] = False
        workflow_business = (projected.get("workflowOutcome") or {}).get("businessOutcome")
        if isinstance(workflow_business, dict):
            workflow_business.update({
                "status": "unknown_without_external_evidence",
                "eventOutcome": EventOutcome.UNKNOWN.value,
                "source": "not_available_as_of",
                "confirmed": False,
            })
        for action in projected.get("actionFeedback") or []:
            if isinstance(action, dict):
                action["businessEffectiveness"] = FeedbackEffectiveness.UNKNOWN.value
                action["businessEffectivenessSource"] = "not_available_as_of"
                action["businessReasonCode"] = FeedbackReasonCode.NONE.value
    else:
        projected["qualityStatus"] = effective_quality

    quality = projected["qualityStatus"]
    if quality == CaseMemoryQuality.VERIFIED_SUCCESS.value:
        experience_type = "positive"
        outcome_tier = 4
    elif quality == CaseMemoryQuality.PARTIAL_SUCCESS.value:
        experience_type = "partial"
        outcome_tier = 3
    elif quality == CaseMemoryQuality.FAILED_OUTCOME.value:
        experience_type = "negative"
        outcome_tier = 0
    else:
        # Legacy validated/partial only means the source chain was complete; it
        # is not durable proof of business success.
        experience_type = "unverified"
        outcome_tier = 1
    if intersection_id and case.intersection_id == intersection_id:
        location_tier = 3
        reason = "same event type, region, and canonical intersection"
    elif road_id and case.road_id == road_id:
        location_tier = 2
        reason = "same event type, region, and canonical road"
    else:
        location_tier = 1
        reason = "same event type and canonical region"
    projected["experienceType"] = experience_type
    projected["whyRelevant"] = reason
    projected["retrievalScore"] = {
        "locationTier": location_tier,
        "outcomeTier": outcome_tier,
        "semanticSimilarity": None,
        "recencyTieBreak": case.completed_at,
    }
    if experience_type == "negative":
        projected["caution"] = _negative_caution(projected)
    return projected


def _negative_caution(case: Dict[str, Any]) -> Dict[str, Any]:
    rejected = []
    for item in (case.get("recommendationFeedback") or {}).get("rejectionReasons") or []:
        if isinstance(item, dict):
            rejected.append({
                "approvalId": item.get("approvalId"),
                "reasonCode": item.get("reasonCode"),
            })
    failed_actions = [
        {
            "actionType": item.get("actionType"),
            "status": item.get("status"),
        }
        for item in case.get("actionFeedback") or []
        if isinstance(item, dict)
        and (item.get("failed") or item.get("blocked") or item.get("enteredUnknown"))
    ]
    return {
        "source": "durable_feedback_and_execution",
        "rejectedRecommendations": rejected,
        "failedActions": failed_actions,
        "instruction": "treat_as_caution_not_positive_template",
    }


def _log_projection_pending(
    *,
    event_id: str,
    workflow_run_id: str,
    code: str,
    error_type: str,
) -> None:
    """Emit a bounded/redacted operational signal without failing Feedback."""

    try:
        from backend.observability.logging import log_runtime_event

        log_runtime_event(
            component="case_memory",
            operation="feedback_projection",
            status="pending",
            event_id=event_id,
            workflow_run_id=workflow_run_id,
            errorCode=code,
            errorType=error_type,
        )
    except Exception:
        # Logging is secondary to the already-durable Feedback write.
        return
