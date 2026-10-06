"""Grounded event context assembly for the existing Agent pipeline."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from backend.case_memory.models import CaseMemoryError
from backend.regional.historical import DEFAULT_WINDOW_DAYS, HistoricalTrafficService
from backend.regional.repository import SQLiteRegionalRepository
from backend.tools.event_identity import (
    EventIdentityError,
    compact_event_context,
    extract_event_id,
    hydrate_authoritative_event,
)

from backend.grounding.models import (
    CaseMemoryContext,
    CurrentEventContext,
    GroundedEventContext,
    GroundingProvenance,
    HistoricalContext,
    KnowledgeContext,
    RegionalContext,
    RegionalLocation,
    utc_now_iso,
)

MAX_CONNECTED_ROADS = 5
MAX_NEARBY_POIS = 5
MAX_HISTORY_REFS = 5
MAX_KNOWLEDGE_EVIDENCE = 5
MAX_CASES = 5
MAX_EXCERPT_CHARS = 280


class GroundedEventContextAssembler:
    """Build one immutable, compact grounding snapshot for an event analysis."""

    def __init__(
        self,
        *,
        regional_repository: Optional[SQLiteRegionalRepository] = None,
        historical_service: Optional[HistoricalTrafficService] = None,
        knowledge_service: Optional[Any] = None,
        case_memory_service: Optional[Any] = None,
    ):
        self.regional_repository = regional_repository or SQLiteRegionalRepository()
        self.historical_service = historical_service or HistoricalTrafficService(self.regional_repository)
        if knowledge_service is None:
            from backend.knowledge.regional_context import EventKnowledgeContextService

            knowledge_service = EventKnowledgeContextService(self.regional_repository)
        if case_memory_service is None:
            from backend.case_memory.service import TrafficCaseMemoryService

            case_memory_service = TrafficCaseMemoryService(regional_repo=self.regional_repository)
        self.knowledge_service = knowledge_service
        self.case_memory_service = case_memory_service

    def assemble(
        self,
        event_id: str,
        *,
        query: str = "",
        knowledge_top_k: int = MAX_KNOWLEDGE_EVIDENCE,
        case_top_k: int = MAX_CASES,
        history_window_days: int = DEFAULT_WINDOW_DAYS,
        authoritative_event: Optional[Dict[str, Any]] = None,
    ) -> GroundedEventContext:
        snapshot = authoritative_event or hydrate_authoritative_event(event_id)
        canonical_event_id = extract_event_id(snapshot)
        current_event = self._current_event(snapshot)
        binding_error = False
        try:
            binding = self.regional_repository.get_active_event_location_binding(canonical_event_id)
        except Exception:
            binding = None
            binding_error = True

        regional = self._regional_context(canonical_event_id, binding, binding_error=binding_error)
        history = self._historical_context(
            canonical_event_id,
            binding,
            history_window_days=history_window_days,
        )
        knowledge = self._knowledge_context(
            canonical_event_id,
            query=query,
            top_k=knowledge_top_k,
        )
        cases = self._case_context(canonical_event_id, binding, top_k=case_top_k)

        grounding_refs = self._grounding_refs(regional, history, knowledge, cases)
        return GroundedEventContext(
            currentEvent=current_event,
            regionalContext=regional,
            historicalContext=history,
            knowledgeContext=knowledge,
            caseMemoryContext=cases,
            groundingStatus=self._overall_status(regional, history, knowledge, cases),
            groundingRefs=grounding_refs,
        )

    def _current_event(self, snapshot: Dict[str, Any]) -> CurrentEventContext:
        return _current_event_from_snapshot(snapshot)

    def _regional_context(
        self,
        event_id: str,
        binding: Optional[Dict[str, Any]],
        *,
        binding_error: bool = False,
    ) -> RegionalContext:
        if binding_error:
            return RegionalContext(
                status="UNAVAILABLE",
                reason="LOCATION_BINDING_LOOKUP_ERROR",
                provenance=GroundingProvenance(
                    sourceType="regional_core",
                    bindingSource="event_location_bindings",
                    capturedAt=utc_now_iso(),
                    queryModel="active_resolved_event_location_binding",
                    notes=["canonical binding lookup failed"],
                ),
            )
        if not binding or not binding.get("regionId"):
            return RegionalContext(
                status="UNRESOLVED",
                reason="LOCATION_NOT_RESOLVED",
                provenance=GroundingProvenance(
                    sourceType="regional_core",
                    bindingSource="event_location_bindings",
                    capturedAt=utc_now_iso(),
                    queryModel="active_resolved_event_location_binding",
                    notes=["no active canonical event location binding"],
                ),
            )
        try:
            location_context = self.regional_repository.build_regional_location_context(binding)
        except Exception:
            return RegionalContext(
                status="UNAVAILABLE",
                reason="REGIONAL_CONTEXT_ERROR",
                provenance=self._provenance_from_binding(binding, source_type="regional_core"),
            )

        region = _compact_region(location_context.get("region"))
        road = _compact_road(location_context.get("road"))
        intersection = _compact_intersection(location_context.get("intersection"))
        connected_roads = [
            compact
            for item in (location_context.get("connectedRoads") or [])[:MAX_CONNECTED_ROADS]
            if isinstance(item, dict)
            for compact in [_compact_road(item)]
            if compact
        ]
        nearby_pois = [
            compact
            for item in (location_context.get("nearbyPois") or [])[:MAX_NEARBY_POIS]
            if isinstance(item, dict)
            for compact in [_compact_poi(item)]
            if compact
        ]
        granularity = "intersection" if binding.get("intersectionId") else "road"
        return RegionalContext(
            status="READY",
            region=region,
            location=RegionalLocation(
                regionId=binding.get("regionId"),
                roadId=binding.get("roadId"),
                intersectionId=binding.get("intersectionId"),
                roadName=str((road or {}).get("name") or ""),
                intersectionName=str((intersection or {}).get("name") or ""),
                locationGranularity=granularity,
            ),
            connectedRoads=connected_roads,
            nearbyPois=nearby_pois,
            provenance=self._provenance_from_binding(binding, source_type="regional_core"),
        )

    def _historical_context(
        self,
        event_id: str,
        binding: Optional[Dict[str, Any]],
        *,
        history_window_days: int,
    ) -> HistoricalContext:
        if not binding or not binding.get("regionId"):
            return HistoricalContext(
                status="UNAVAILABLE",
                reason="LOCATION_NOT_RESOLVED",
                provenance=GroundingProvenance(
                    sourceType="event_records",
                    bindingSource="event_location_bindings",
                    capturedAt=utc_now_iso(),
                    queryModel="historical_context_for_event",
                    notes=["regional history is disabled without canonical binding"],
                ),
            )
        try:
            context = self.historical_service.get_historical_context_for_event(
                event_id,
                window_days=max(1, min(int(history_window_days or DEFAULT_WINDOW_DAYS), 365)),
            )
        except Exception:
            return HistoricalContext(
                status="UNAVAILABLE",
                reason="HISTORICAL_CONTEXT_ERROR",
                provenance=self._provenance_from_binding(
                    binding,
                    source_type="event_records",
                    query_model="historical_context_for_event",
                ),
            )
        return HistoricalContext(
            status=str(context.get("status") or "UNAVAILABLE"),
            reason=str(context.get("reason") or ""),
            window=dict(context.get("window") or {}),
            eventCount=int(context.get("eventCount") or 0),
            eventTypeDistribution=dict(context.get("eventTypeDistribution") or {}),
            riskDistribution=dict(context.get("riskDistribution") or {}),
            averageDuration=_finite_number(context.get("averageDuration")),
            maxRisk=_finite_number(context.get("maxRisk")),
            unclosedCount=int(context.get("unclosedCount") or 0),
            timeOfDayDistribution=dict(context.get("timeOfDayDistribution") or {}),
            recentEventRefs=[
                _compact_history_ref(item)
                for item in (context.get("recentEventRefs") or [])[:MAX_HISTORY_REFS]
                if isinstance(item, dict)
            ],
            provenance=_history_provenance(context.get("provenance")),
        )

    def _knowledge_context(
        self,
        event_id: str,
        *,
        query: str,
        top_k: int,
    ) -> KnowledgeContext:
        try:
            context = self.knowledge_service.get_context_for_event(
                event_id,
                query=query,
                limit=max(1, min(int(top_k or MAX_KNOWLEDGE_EVIDENCE), MAX_KNOWLEDGE_EVIDENCE)),
            )
        except Exception:
            return KnowledgeContext(
                status="UNAVAILABLE",
                reason="KNOWLEDGE_CONTEXT_ERROR",
                regionalGroundingStatus="UNAVAILABLE",
                provenance=GroundingProvenance(
                    sourceType="rag_v2_documents",
                    bindingSource="event_location_bindings",
                    capturedAt=utc_now_iso(),
                    queryModel="event_bound_knowledge_context",
                ),
            )

        evidence = [
            _compact_knowledge_evidence(item)
            for item in (context.get("evidence") or [])[:MAX_KNOWLEDGE_EVIDENCE]
            if isinstance(item, dict)
        ]
        raw_status = str(context.get("status") or "").upper()
        status = "READY" if raw_status == "READY" or context.get("status") == "ready" else raw_status or "UNAVAILABLE"
        if status == "READY" and not evidence:
            status = "EMPTY"
        return KnowledgeContext(
            status=status,
            reason=str(context.get("reason") or ""),
            regionalGroundingStatus=str(context.get("regionalGroundingStatus") or "UNAVAILABLE"),
            scope=dict(context.get("scope") or {}),
            evidence=evidence,
            provenance=GroundingProvenance(
                sourceType="rag_v2_documents",
                bindingSource="event_location_bindings",
                capturedAt=str((context.get("provenance") or {}).get("capturedAt") or utc_now_iso()),
                asOf=(context.get("scope") or {}).get("asOf"),
                regionId=(context.get("scope") or {}).get("regionId"),
                roadId=(context.get("scope") or {}).get("roadId"),
                intersectionId=(context.get("scope") or {}).get("intersectionId"),
                bindingId=(context.get("scope") or {}).get("bindingId"),
                queryModel=str((context.get("provenance") or {}).get("retrievalPipeline") or "event_bound_knowledge_context"),
                notes=[
                    "structured_pre_retrieval",
                    str((context.get("provenance") or {}).get("globalKnowledgePolicy") or "explicit_global_only"),
                ],
            ),
        )

    def _case_context(
        self,
        event_id: str,
        binding: Optional[Dict[str, Any]],
        *,
        top_k: int,
    ) -> CaseMemoryContext:
        if not binding or not binding.get("regionId"):
            return CaseMemoryContext(
                status="UNRESOLVED",
                reason="LOCATION_NOT_RESOLVED",
                provenance=GroundingProvenance(
                    sourceType="traffic_case_memories",
                    bindingSource="event_location_bindings",
                    capturedAt=utc_now_iso(),
                    queryModel="event_bound_case_memory_context",
                    notes=["case memory is disabled without canonical binding"],
                ),
            )
        try:
            context = self.case_memory_service.get_case_context_for_event(
                event_id,
                limit=max(1, min(int(top_k or MAX_CASES), MAX_CASES)),
            )
        except CaseMemoryError as err:
            return CaseMemoryContext(
                status="UNAVAILABLE",
                reason=err.code,
                scope=self._case_scope_from_binding(event_id, binding),
                provenance=self._provenance_from_binding(
                    binding,
                    source_type="traffic_case_memories",
                    query_model="event_bound_case_memory_context",
                ),
            )
        except Exception:
            return CaseMemoryContext(
                status="UNAVAILABLE",
                reason="CASE_MEMORY_CONTEXT_ERROR",
                scope=self._case_scope_from_binding(event_id, binding),
                provenance=self._provenance_from_binding(
                    binding,
                    source_type="traffic_case_memories",
                    query_model="event_bound_case_memory_context",
                ),
            )

        cases = [
            _compact_case(item)
            for item in (context.get("cases") or [])[:MAX_CASES]
            if isinstance(item, dict)
        ]
        positive_cases = [item for item in cases if item.get("experienceType") == "positive"]
        partial_cases = [item for item in cases if item.get("experienceType") == "partial"]
        negative_cases = [item for item in cases if item.get("experienceType") == "negative"]
        unverified_cases = [item for item in cases if item.get("experienceType") == "unverified"]
        return CaseMemoryContext(
            status="READY" if cases else "EMPTY",
            scope={
                "eventId": context.get("eventId"),
                "regionId": context.get("regionId"),
                "eventType": context.get("eventType"),
                "asOf": context.get("asOf"),
                "location": dict(context.get("location") or {}),
                "retrievalPolicy": dict(context.get("retrievalPolicy") or {}),
            },
            cases=cases,
            positiveCases=positive_cases,
            partialCases=partial_cases,
            negativeCases=negative_cases,
            unverifiedCases=unverified_cases,
            total=int(context.get("total") or 0),
            provenance=self._provenance_from_binding(
                binding,
                source_type="traffic_case_memories",
                as_of=context.get("asOf"),
                query_model="event_bound_case_memory_context",
                notes=[
                    "strict_past_completed_cases",
                    "current_event_excluded",
                    "future_feedback_masked",
                    "outcome_aware_positive_negative_cases",
                ],
            ),
        )

    def _case_scope_from_binding(self, event_id: str, binding: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "eventId": event_id,
            "regionId": binding.get("regionId"),
            "location": {
                "roadId": binding.get("roadId"),
                "intersectionId": binding.get("intersectionId"),
            },
        }

    def _provenance_from_binding(
        self,
        binding: Dict[str, Any],
        *,
        source_type: str,
        as_of: Optional[str] = None,
        query_model: str = "active_resolved_event_location_binding",
        notes: Optional[List[str]] = None,
    ) -> GroundingProvenance:
        return GroundingProvenance(
            sourceType=source_type,
            bindingSource="event_location_bindings",
            capturedAt=utc_now_iso(),
            asOf=as_of,
            regionId=binding.get("regionId"),
            roadId=binding.get("roadId"),
            intersectionId=binding.get("intersectionId"),
            bindingId=binding.get("bindingId"),
            queryModel=query_model,
            notes=notes or [],
        )

    def _grounding_refs(
        self,
        regional: RegionalContext,
        history: HistoricalContext,
        knowledge: KnowledgeContext,
        cases: CaseMemoryContext,
    ) -> List[Dict[str, Any]]:
        refs: List[Dict[str, Any]] = []
        if regional.provenance.bindingId:
            refs.append({
                "type": "regional_location",
                "bindingId": regional.provenance.bindingId,
                "regionId": regional.provenance.regionId,
                "roadId": regional.provenance.roadId,
                "intersectionId": regional.provenance.intersectionId,
            })
        if history.status == "READY":
            refs.append({
                "type": "historical_traffic",
                "asOf": history.window.get("asOf"),
                "windowStart": history.window.get("start"),
                "windowEnd": history.window.get("end"),
                "eventCount": history.eventCount,
            })
        for item in knowledge.evidence[:MAX_KNOWLEDGE_EVIDENCE]:
            refs.append({
                "type": "knowledge_evidence",
                "evidenceId": item.get("evidenceId"),
                "documentId": item.get("documentId"),
                "chunkId": item.get("chunkId"),
                "scopeMatch": item.get("scopeMatch"),
            })
        for item in cases.cases[:MAX_CASES]:
            refs.append({
                "type": "case_memory",
                "caseId": item.get("caseId"),
                "sourceWorkflowRunId": item.get("sourceWorkflowRunId"),
                "finalStatus": item.get("finalStatus"),
                "qualityStatus": item.get("qualityStatus"),
                "experienceType": item.get("experienceType"),
            })
        return [ref for ref in refs if any(v for k, v in ref.items() if k != "type")]

    def _overall_status(
        self,
        regional: RegionalContext,
        history: HistoricalContext,
        knowledge: KnowledgeContext,
        cases: CaseMemoryContext,
    ) -> str:
        if regional.status != "READY":
            return "MINIMAL"
        unavailable = {history.status, knowledge.status, cases.status} & {"UNAVAILABLE", "UNRESOLVED"}
        if unavailable:
            return "PARTIAL"
        return "FULL"


def _finite_number(value: Any) -> Optional[float]:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None
    return number


def _compact_region(region: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(region, dict):
        return None
    return {
        "regionId": region.get("regionId"),
        "name": region.get("name"),
        "city": region.get("city"),
        "district": region.get("district"),
        "timezone": region.get("timezone"),
        "verificationStatus": region.get("verificationStatus"),
    }


def _compact_road(road: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(road, dict):
        return None
    return {
        "roadId": road.get("roadId"),
        "regionId": road.get("regionId"),
        "name": road.get("name"),
        "roadType": road.get("roadType"),
        "directionMode": road.get("directionMode"),
        "verificationStatus": road.get("verificationStatus"),
    }


def _compact_intersection(intersection: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(intersection, dict):
        return None
    return {
        "intersectionId": intersection.get("intersectionId"),
        "regionId": intersection.get("regionId"),
        "name": intersection.get("name"),
        "importance": intersection.get("importance"),
        "verificationStatus": intersection.get("verificationStatus"),
    }


def _compact_poi(poi: Any) -> Dict[str, Any]:
    return {
        "poiId": poi.get("poiId"),
        "regionId": poi.get("regionId"),
        "name": poi.get("name"),
        "type": poi.get("type"),
        "roadId": poi.get("roadId"),
        "intersectionId": poi.get("intersectionId"),
        "importance": poi.get("importance"),
        "verificationStatus": poi.get("verificationStatus"),
    }


def _history_provenance(value: Any) -> GroundingProvenance:
    source = value if isinstance(value, dict) else {}
    return GroundingProvenance(
        sourceType=str(source.get("sourceType") or "event_records"),
        bindingSource=str(source.get("bindingSource") or "event_location_bindings"),
        capturedAt=str(source.get("capturedAt") or utc_now_iso()),
        asOf=source.get("asOf"),
        regionId=source.get("regionId"),
        roadId=source.get("roadId"),
        intersectionId=source.get("intersectionId"),
        bindingId=source.get("bindingId"),
        queryModel=str(source.get("queryModel") or "historical_context_for_event"),
        notes=["strict_past_by_event_created_at"],
    )


def _compact_history_ref(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "eventId": item.get("eventId"),
        "eventType": item.get("eventType"),
        "createdAt": item.get("createdAt"),
        "riskLevel": item.get("riskLevel"),
        "status": item.get("status"),
    }


def _compact_knowledge_evidence(item: Dict[str, Any]) -> Dict[str, Any]:
    regional = item.get("regionalMetadata") if isinstance(item.get("regionalMetadata"), dict) else {}
    return {
        "evidenceId": item.get("evidenceId"),
        "documentId": item.get("documentId"),
        "chunkId": item.get("chunkId"),
        "parentChunkId": item.get("parentChunkId"),
        "title": item.get("title"),
        "docType": item.get("docType"),
        "sectionPath": item.get("sectionPath"),
        "excerpt": _short_text(item.get("content")),
        "authorityLevel": item.get("authorityLevel"),
        "sourceUri": item.get("sourceUri"),
        "effectiveFrom": item.get("effectiveFrom"),
        "effectiveTo": item.get("effectiveTo"),
        "eventType": item.get("eventType"),
        "regionId": item.get("regionId"),
        "roadId": item.get("roadId"),
        "intersectionId": item.get("intersectionId"),
        "groundingScope": item.get("groundingScope"),
        "scopeMatch": regional.get("scopeMatch"),
        "score": _finite_number(item.get("score")),
        "retrievalChannels": list(item.get("retrievalChannels") or [])[:4],
    }


def _compact_case(item: Dict[str, Any]) -> Dict[str, Any]:
    recommendation = (
        item.get("recommendationFeedback")
        if isinstance(item.get("recommendationFeedback"), dict)
        else {}
    )
    final_plan = (
        recommendation.get("finalPlan")
        if isinstance(recommendation.get("finalPlan"), dict)
        else {}
    )
    original = (
        recommendation.get("originalRecommendation")
        if isinstance(recommendation.get("originalRecommendation"), dict)
        else {}
    )
    event_outcome = (
        item.get("eventOutcome") if isinstance(item.get("eventOutcome"), dict) else {}
    )
    operator = (
        event_outcome.get("operatorAssessment")
        if isinstance(event_outcome.get("operatorAssessment"), dict)
        else {}
    )
    return {
        "caseId": item.get("caseId"),
        "eventId": item.get("eventId"),
        "eventType": item.get("eventType"),
        "regionId": item.get("regionId"),
        "roadId": item.get("roadId"),
        "intersectionId": item.get("intersectionId"),
        "sourceWorkflowRunId": item.get("sourceWorkflowRunId"),
        "sourceCollaborationRunId": item.get("sourceCollaborationRunId"),
        "sourcePlanId": item.get("sourcePlanId"),
        "finalStatus": item.get("finalStatus"),
        "qualityStatus": item.get("qualityStatus"),
        "experienceType": item.get("experienceType"),
        "whyRelevant": _short_text(item.get("whyRelevant")),
        "retrievalScore": dict(item.get("retrievalScore") or {}),
        "generatedSummary": _short_text(item.get("generatedSummary")),
        "completedAt": item.get("completedAt"),
        "recommendation": {
            "status": recommendation.get("status"),
            "planId": final_plan.get("planId") or original.get("planId"),
            "planVersion": final_plan.get("planVersion"),
            "proposedActions": list(original.get("actionRefs") or [])[:10],
            "finalActions": list(final_plan.get("actions") or [])[:10],
            "modifications": list(recommendation.get("modifications") or [])[:20],
            "rejectionReasons": list(recommendation.get("rejectionReasons") or [])[:10],
        },
        "actionOutcomes": [
            {
                "actionType": action.get("actionType"),
                "status": action.get("status"),
                "approved": action.get("approved"),
                "succeeded": action.get("succeeded"),
                "failed": action.get("failed"),
                "blocked": action.get("blocked"),
                "enteredUnknown": action.get("enteredUnknown"),
                "reconciled": action.get("reconciled"),
                "retryCount": action.get("retryCount"),
                "businessEffectiveness": action.get("businessEffectiveness"),
                "finalAction": action.get("finalAction"),
            }
            for action in (item.get("actionFeedback") or [])[:20]
            if isinstance(action, dict)
        ],
        "outcome": {
            "eventOutcome": operator.get("outcome"),
            "effectiveness": operator.get("effectiveness"),
            "assessedAt": operator.get("assessedAt"),
            "businessOutcomeConfirmed": event_outcome.get("businessOutcomeConfirmed"),
        },
        # This contains only structured durable reason/action codes; free-form
        # operator comments are deliberately excluded from Agent context.
        "caution": dict(item.get("caution") or {}),
        "lessonRefs": [
            {
                "type": lesson.get("type"),
                "severity": lesson.get("severity"),
                "summary": _short_text(lesson.get("summary") or lesson.get("message")),
            }
            for lesson in (item.get("lessons") or [])[:3]
            if isinstance(lesson, dict)
        ],
    }


def _short_text(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) <= MAX_EXCERPT_CHARS:
        return text
    return text[:MAX_EXCERPT_CHARS].rstrip() + "..."


def minimal_grounded_context_from_event(
    authoritative_event: Dict[str, Any],
    *,
    reason: str = "GROUNDING_ASSEMBLY_ERROR",
) -> GroundedEventContext:
    """Return an honest event-only snapshot when optional grounding fails hard."""

    current_event = _current_event_from_snapshot(authoritative_event)
    provenance = GroundingProvenance(
        sourceType="grounding_assembler",
        bindingSource="event_records",
        capturedAt=utc_now_iso(),
        queryModel="minimal_event_only_context",
        notes=[reason],
    )
    return GroundedEventContext(
        currentEvent=current_event,
        regionalContext=RegionalContext(status="UNAVAILABLE", reason=reason, provenance=provenance),
        historicalContext=HistoricalContext(status="UNAVAILABLE", reason=reason, provenance=provenance),
        knowledgeContext=KnowledgeContext(
            status="UNAVAILABLE",
            reason=reason,
            regionalGroundingStatus="UNAVAILABLE",
            provenance=provenance,
        ),
        caseMemoryContext=CaseMemoryContext(status="UNAVAILABLE", reason=reason, provenance=provenance),
        groundingStatus="MINIMAL",
        groundingRefs=[],
    )


def _current_event_from_snapshot(snapshot: Dict[str, Any]) -> CurrentEventContext:
    event = compact_event_context(snapshot)
    return CurrentEventContext(
        eventId=str(event.get("eventId") or ""),
        eventType=str(event.get("eventType") or ""),
        eventTypeCn=str(event.get("eventTypeCn") or ""),
        roadName=str(event.get("roadName") or ""),
        direction=str(event.get("direction") or ""),
        avgSpeed=_finite_number(event.get("avgSpeed")),
        queueLength=_finite_number(event.get("queueLength")),
        duration=_finite_number(event.get("duration")),
        vehicleCount=_finite_number(event.get("vehicleCount")),
        riskScore=_finite_number(event.get("riskScore")),
        riskLevel=str(event.get("riskLevel") or ""),
        status=str(event.get("status") or ""),
        weather=str(event.get("weather") or "clear"),
        timePeriod=str(event.get("timePeriod") or "off_peak"),
        isMainRoad=bool(event.get("isMainRoad")),
        nearbySchool=bool(event.get("nearbySchool")),
        nearbyHospital=bool(event.get("nearbyHospital")),
        createdAt=str(snapshot.get("createdAt") or ""),
        updatedAt=str(snapshot.get("updatedAt") or ""),
        snapshotSource=str(snapshot.get("snapshotSource") or ""),
        capturedAt=str(snapshot.get("capturedAt") or ""),
    )
