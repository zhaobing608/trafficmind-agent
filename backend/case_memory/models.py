"""Data contracts for persisted traffic case memory."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


class CaseMemoryQuality(str, Enum):
    VERIFIED_SUCCESS = "VERIFIED_SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILED_OUTCOME = "FAILED_OUTCOME"
    INCOMPLETE = "INCOMPLETE"
    UNVERIFIED = "UNVERIFIED"

    # Read compatibility for case rows created before Phase 21.5.  New case
    # projections never emit these values, but an in-place upgrade must remain
    # able to load and retrieve the existing durable history.
    VALIDATED = "validated"
    PARTIAL = "partial"
    LOW_EVIDENCE = "low_evidence"
    ARCHIVED = "archived"


class FeedbackEffectiveness(str, Enum):
    EFFECTIVE = "EFFECTIVE"
    PARTIALLY_EFFECTIVE = "PARTIALLY_EFFECTIVE"
    INEFFECTIVE = "INEFFECTIVE"
    UNKNOWN = "UNKNOWN"


class EventOutcome(str, Enum):
    RESOLVED = "RESOLVED"
    PARTIALLY_RESOLVED = "PARTIALLY_RESOLVED"
    UNRESOLVED = "UNRESOLVED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


class FeedbackLifecycle(str, Enum):
    PENDING = "PENDING"
    PARTIAL = "PARTIAL"
    COMPLETE = "COMPLETE"


class FeedbackReasonCode(str, Enum):
    NONE = "NONE"
    UNSUPPORTED_ACTION = "UNSUPPORTED_ACTION"
    TOO_HIGH_RISK = "TOO_HIGH_RISK"
    INCORRECT_CONTEXT = "INCORRECT_CONTEXT"
    UNNECESSARY = "UNNECESSARY"
    DUPLICATE = "DUPLICATE"
    OPERATOR_JUDGMENT = "OPERATOR_JUDGMENT"
    OTHER = "OTHER"


class CaseMemoryError(Exception):
    """Expected case-memory domain error."""

    def __init__(self, code: str, message: str, status_code: int = 400):
        self.code = code
        self.message = message
        self.status_code = status_code
        super().__init__(message)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_case_id(source_workflow_run_id: str) -> str:
    """Stable case identity: one case per source workflow run."""

    digest = hashlib.sha256(source_workflow_run_id.encode("utf-8")).hexdigest()[:16]
    return f"case_{digest}"


def build_feedback_id(event_id: str, workflow_run_id: str) -> str:
    """Stable identity for one operator assessment of one Event/Run chain."""

    digest = hashlib.sha256(
        json.dumps(
            [event_id, workflow_run_id],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:16]
    return f"feedback_{digest}"


def _parse_bool(value: Any, *, default: bool) -> bool:
    """Parse persisted boolean values without treating ``"false"`` as true."""

    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
    return default


@dataclass
class TrafficEventFeedback:
    feedback_id: str
    event_id: str
    workflow_run_id: str
    agent_run_id: Optional[str] = None
    plan_id: Optional[str] = None
    plan_version: Optional[int] = None
    approval_id: Optional[str] = None
    action_execution_id: Optional[str] = None
    action_assessments: List[Dict[str, Any]] = field(default_factory=list)
    event_outcome: EventOutcome = EventOutcome.UNKNOWN
    effectiveness: FeedbackEffectiveness = FeedbackEffectiveness.UNKNOWN
    reason_code: FeedbackReasonCode = FeedbackReasonCode.NONE
    comment: str = ""
    reviewer: str = ""
    lifecycle: FeedbackLifecycle = FeedbackLifecycle.PENDING
    revision: int = 1
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self) -> None:
        now = utc_now_iso()
        if not self.created_at:
            self.created_at = now
        if not self.updated_at:
            self.updated_at = self.created_at
        if isinstance(self.event_outcome, str):
            self.event_outcome = EventOutcome(self.event_outcome)
        if isinstance(self.effectiveness, str):
            self.effectiveness = FeedbackEffectiveness(self.effectiveness)
        if isinstance(self.reason_code, str):
            self.reason_code = FeedbackReasonCode(self.reason_code)
        if isinstance(self.lifecycle, str):
            self.lifecycle = FeedbackLifecycle(self.lifecycle)
        self.revision = max(1, int(self.revision or 1))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "feedbackId": self.feedback_id,
            "eventId": self.event_id,
            "workflowRunId": self.workflow_run_id,
            "agentRunId": self.agent_run_id,
            "planId": self.plan_id,
            "planVersion": self.plan_version,
            "approvalId": self.approval_id,
            "actionExecutionId": self.action_execution_id,
            "actionAssessments": self.action_assessments,
            "eventOutcome": self.event_outcome.value,
            "effectiveness": self.effectiveness.value,
            "reasonCode": self.reason_code.value,
            "comment": self.comment or None,
            "reviewer": self.reviewer or None,
            "lifecycle": self.lifecycle.value,
            "revision": self.revision,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
        }


@dataclass
class TrafficCaseMemory:
    case_id: str
    region_id: str
    event_id: str
    event_type: str
    source_workflow_run_id: str
    final_status: str
    quality_status: CaseMemoryQuality
    road_id: Optional[str] = None
    intersection_id: Optional[str] = None
    source_session_id: Optional[str] = None
    source_collaboration_run_id: Optional[str] = None
    source_plan_id: Optional[str] = None
    event_snapshot: Dict[str, Any] = field(default_factory=dict)
    agent_facts: Dict[str, Any] = field(default_factory=dict)
    plan_facts: Dict[str, Any] = field(default_factory=dict)
    human_decisions: List[Dict[str, Any]] = field(default_factory=list)
    workflow_outcome: Dict[str, Any] = field(default_factory=dict)
    recommendation_feedback: Dict[str, Any] = field(default_factory=dict)
    action_feedback: List[Dict[str, Any]] = field(default_factory=list)
    event_outcome: Dict[str, Any] = field(default_factory=dict)
    feedback_lifecycle: FeedbackLifecycle = FeedbackLifecycle.PENDING
    feedback_updated_at: Optional[str] = None
    feedback_revision: Optional[int] = None
    projection_revision: int = 1
    system_updated_at: Optional[str] = None
    is_canonical: bool = True
    superseded_by_case_id: Optional[str] = None
    lessons: List[Dict[str, Any]] = field(default_factory=list)
    generated_summary: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    source_type: str = "workflow_case_builder"
    source_reference: str = ""
    provenance: Dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self) -> None:
        now = utc_now_iso()
        if not self.created_at:
            self.created_at = now
        if not self.updated_at:
            self.updated_at = self.created_at
        if isinstance(self.quality_status, str):
            self.quality_status = CaseMemoryQuality(self.quality_status)
        if isinstance(self.feedback_lifecycle, str):
            self.feedback_lifecycle = FeedbackLifecycle(self.feedback_lifecycle)
        self.projection_revision = max(1, int(self.projection_revision or 1))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "caseId": self.case_id,
            "regionId": self.region_id,
            "eventId": self.event_id,
            "eventType": self.event_type,
            "roadId": self.road_id,
            "intersectionId": self.intersection_id,
            "sourceSessionId": self.source_session_id,
            "sourceCollaborationRunId": self.source_collaboration_run_id,
            "sourcePlanId": self.source_plan_id,
            "sourceWorkflowRunId": self.source_workflow_run_id,
            "finalStatus": self.final_status,
            "qualityStatus": self.quality_status.value,
            "eventSnapshot": self.event_snapshot,
            "agentFacts": self.agent_facts,
            "planFacts": self.plan_facts,
            "humanDecisions": self.human_decisions,
            "workflowOutcome": self.workflow_outcome,
            "recommendationFeedback": self.recommendation_feedback,
            "actionFeedback": self.action_feedback,
            "eventOutcome": self.event_outcome,
            "feedbackLifecycle": self.feedback_lifecycle.value,
            "feedbackUpdatedAt": self.feedback_updated_at,
            "feedbackRevision": self.feedback_revision,
            "projectionRevision": self.projection_revision,
            "systemUpdatedAt": self.system_updated_at,
            "isCanonical": self.is_canonical,
            "supersededByCaseId": self.superseded_by_case_id,
            "lessons": self.lessons,
            "generatedSummary": self.generated_summary,
            "startedAt": self.started_at,
            "completedAt": self.completed_at,
            "sourceType": self.source_type,
            "sourceReference": self.source_reference,
            "provenance": self.provenance,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TrafficCaseMemory":
        return cls(
            case_id=data["caseId"],
            region_id=data["regionId"],
            event_id=data["eventId"],
            event_type=data.get("eventType", ""),
            road_id=data.get("roadId"),
            intersection_id=data.get("intersectionId"),
            source_session_id=data.get("sourceSessionId"),
            source_collaboration_run_id=data.get("sourceCollaborationRunId"),
            source_plan_id=data.get("sourcePlanId"),
            source_workflow_run_id=data["sourceWorkflowRunId"],
            final_status=data.get("finalStatus", ""),
            quality_status=CaseMemoryQuality(data.get("qualityStatus", "partial")),
            event_snapshot=data.get("eventSnapshot") or {},
            agent_facts=data.get("agentFacts") or {},
            plan_facts=data.get("planFacts") or {},
            human_decisions=data.get("humanDecisions") or [],
            workflow_outcome=data.get("workflowOutcome") or {},
            recommendation_feedback=data.get("recommendationFeedback") or {},
            action_feedback=data.get("actionFeedback") or [],
            event_outcome=data.get("eventOutcome") or {},
            feedback_lifecycle=FeedbackLifecycle(
                data.get("feedbackLifecycle", FeedbackLifecycle.PENDING.value)
            ),
            feedback_updated_at=data.get("feedbackUpdatedAt"),
            feedback_revision=data.get("feedbackRevision"),
            projection_revision=data.get("projectionRevision", 1),
            system_updated_at=data.get("systemUpdatedAt"),
            is_canonical=_parse_bool(data.get("isCanonical"), default=True),
            superseded_by_case_id=data.get("supersededByCaseId"),
            lessons=data.get("lessons") or [],
            generated_summary=data.get("generatedSummary"),
            started_at=data.get("startedAt"),
            completed_at=data.get("completedAt"),
            source_type=data.get("sourceType", "workflow_case_builder"),
            source_reference=data.get("sourceReference", ""),
            provenance=data.get("provenance") or {},
            created_at=data.get("createdAt", ""),
            updated_at=data.get("updatedAt", ""),
        )


@dataclass
class CaseBuildResult:
    case: TrafficCaseMemory
    created: bool
    rebuilt: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "case": self.case.to_dict(),
            "created": self.created,
            "rebuilt": self.rebuilt,
            "newCases": 1 if self.created else 0,
        }


@dataclass
class CaseQueryResult:
    cases: List[TrafficCaseMemory]
    total: int
    limit: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cases": [case.to_dict() for case in self.cases],
            "total": self.total,
            "limit": self.limit,
        }
