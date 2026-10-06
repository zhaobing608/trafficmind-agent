"""Traffic Case Memory API."""

from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from backend.case_memory.models import (
    CaseMemoryError,
    EventOutcome,
    FeedbackEffectiveness,
    FeedbackReasonCode,
)
from backend.case_memory.service import TrafficCaseMemoryService, TrafficFeedbackService


router = APIRouter(prefix="/case-memory", tags=["Traffic Case Memory"])
feedback_router = APIRouter(prefix="/events", tags=["Event Feedback"])


class ActionFeedbackAssessmentRequest(BaseModel):
    actionExecutionId: str = Field(min_length=1, max_length=200)
    effectiveness: FeedbackEffectiveness = FeedbackEffectiveness.UNKNOWN
    reasonCode: FeedbackReasonCode = FeedbackReasonCode.NONE

    model_config = ConfigDict(extra="forbid")


class EventFeedbackRequest(BaseModel):
    workflowRunId: str = Field(min_length=1, max_length=200)
    eventOutcome: EventOutcome = EventOutcome.UNKNOWN
    effectiveness: FeedbackEffectiveness = FeedbackEffectiveness.UNKNOWN
    reasonCode: FeedbackReasonCode = FeedbackReasonCode.NONE
    comment: str = Field(default="", max_length=1000)
    reviewer: str = Field(default="", max_length=200)
    approvalId: Optional[str] = Field(default=None, max_length=200)
    actionExecutionId: Optional[str] = Field(default=None, max_length=200)
    actionAssessments: Optional[List[ActionFeedbackAssessmentRequest]] = Field(
        default=None,
        max_length=50,
    )

    model_config = ConfigDict(extra="forbid")


def _service() -> TrafficCaseMemoryService:
    return TrafficCaseMemoryService()


def _feedback_service() -> TrafficFeedbackService:
    return TrafficFeedbackService()


def _raise_http(exc: CaseMemoryError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    )


@router.post("/from-workflow/{run_id}")
def build_case_from_workflow(
    run_id: str,
    rebuild: bool = Query(False),
):
    try:
        return _service().build_from_workflow_run(run_id, rebuild=rebuild).to_dict()
    except CaseMemoryError as exc:
        _raise_http(exc)


@router.get("")
def query_case_memories(
    regionId: str = Query(...),
    eventType: str = Query(...),
    roadId: Optional[str] = Query(None),
    intersectionId: Optional[str] = Query(None),
    finalStatus: Optional[str] = Query(None),
    qualityStatus: Optional[str] = Query(None),
    asOf: Optional[str] = Query(None),
    limit: int = Query(5, ge=1, le=50),
    forAgent: bool = Query(False),
):
    try:
        result = _service().query_cases(
            region_id=regionId,
            event_type=eventType,
            road_id=roadId,
            intersection_id=intersectionId,
            final_status=finalStatus,
            quality_status=qualityStatus,
            as_of=asOf,
            limit=limit,
            for_agent=forAgent,
        )
        return {
            "cases": [case.to_dict() for case in result["cases"]],
            "total": result["total"],
            "limit": result["limit"],
        }
    except CaseMemoryError as exc:
        _raise_http(exc)


@router.get("/events/{event_id}")
def get_case_context_for_event(
    event_id: str,
    limit: int = Query(5, ge=1, le=20),
):
    try:
        return _service().get_case_context_for_event(event_id, limit=limit)
    except CaseMemoryError as exc:
        _raise_http(exc)


@router.get("/{case_id}")
def get_case_memory(case_id: str):
    try:
        return _service().get_case(case_id).to_dict()
    except CaseMemoryError as exc:
        _raise_http(exc)


@feedback_router.post("/{event_id}/feedback")
def submit_event_feedback(event_id: str, body: EventFeedbackRequest):
    """Store only operator-known outcome fields; identities are server-resolved."""

    try:
        return _feedback_service().submit_feedback(
            event_id=event_id,
            workflow_run_id=body.workflowRunId,
            event_outcome=body.eventOutcome,
            effectiveness=body.effectiveness,
            reason_code=body.reasonCode,
            comment=body.comment,
            reviewer=body.reviewer,
            approval_id=body.approvalId,
            action_execution_id=body.actionExecutionId,
            action_assessments=(
                [item.model_dump() for item in body.actionAssessments]
                if body.actionAssessments is not None
                else None
            ),
        )
    except CaseMemoryError as exc:
        _raise_http(exc)


@feedback_router.get("/{event_id}/feedback")
def get_event_feedback(
    event_id: str,
    workflowRunId: Optional[str] = Query(None),
):
    try:
        return _feedback_service().get_feedback(
            event_id,
            workflow_run_id=workflowRunId,
        )
    except CaseMemoryError as exc:
        _raise_http(exc)
