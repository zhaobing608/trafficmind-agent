"""Typed GroundedEventContext contracts for Phase21 Wave F."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class GroundingBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class GroundingProvenance(GroundingBaseModel):
    sourceType: str = ""
    bindingSource: str = ""
    capturedAt: str = Field(default_factory=utc_now_iso)
    asOf: Optional[str] = None
    regionId: Optional[str] = None
    roadId: Optional[str] = None
    intersectionId: Optional[str] = None
    bindingId: Optional[str] = None
    queryModel: str = ""
    notes: List[str] = Field(default_factory=list)


class CurrentEventContext(GroundingBaseModel):
    eventId: str = ""
    eventType: str = ""
    eventTypeCn: str = ""
    roadName: str = ""
    direction: str = ""
    avgSpeed: Optional[float] = None
    queueLength: Optional[float] = None
    duration: Optional[float] = None
    vehicleCount: Optional[float] = None
    riskScore: Optional[float] = None
    riskLevel: str = ""
    status: str = ""
    weather: str = "clear"
    timePeriod: str = "off_peak"
    isMainRoad: bool = False
    nearbySchool: bool = False
    nearbyHospital: bool = False
    createdAt: str = ""
    updatedAt: str = ""
    snapshotSource: str = ""
    capturedAt: str = ""


class RegionalLocation(GroundingBaseModel):
    regionId: Optional[str] = None
    roadId: Optional[str] = None
    intersectionId: Optional[str] = None
    roadName: str = ""
    intersectionName: str = ""
    locationGranularity: str = "unresolved"


class RegionalContext(GroundingBaseModel):
    status: str = "UNRESOLVED"
    reason: str = ""
    region: Optional[Dict[str, Any]] = None
    location: RegionalLocation = Field(default_factory=RegionalLocation)
    connectedRoads: List[Dict[str, Any]] = Field(default_factory=list)
    nearbyPois: List[Dict[str, Any]] = Field(default_factory=list)
    provenance: GroundingProvenance = Field(default_factory=GroundingProvenance)


class HistoricalContext(GroundingBaseModel):
    status: str = "UNAVAILABLE"
    reason: str = ""
    window: Dict[str, Any] = Field(default_factory=dict)
    eventCount: int = 0
    eventTypeDistribution: Dict[str, int] = Field(default_factory=dict)
    riskDistribution: Dict[str, int] = Field(default_factory=dict)
    averageDuration: Optional[float] = None
    maxRisk: Optional[float] = None
    unclosedCount: int = 0
    timeOfDayDistribution: Dict[str, int] = Field(default_factory=dict)
    recentEventRefs: List[Dict[str, Any]] = Field(default_factory=list)
    provenance: GroundingProvenance = Field(default_factory=GroundingProvenance)


class KnowledgeContext(GroundingBaseModel):
    status: str = "UNAVAILABLE"
    reason: str = ""
    regionalGroundingStatus: str = "UNAVAILABLE"
    scope: Dict[str, Any] = Field(default_factory=dict)
    evidence: List[Dict[str, Any]] = Field(default_factory=list)
    provenance: GroundingProvenance = Field(default_factory=GroundingProvenance)


class CaseMemoryContext(GroundingBaseModel):
    status: str = "UNAVAILABLE"
    reason: str = ""
    scope: Dict[str, Any] = Field(default_factory=dict)
    cases: List[Dict[str, Any]] = Field(default_factory=list)
    positiveCases: List[Dict[str, Any]] = Field(default_factory=list)
    partialCases: List[Dict[str, Any]] = Field(default_factory=list)
    negativeCases: List[Dict[str, Any]] = Field(default_factory=list)
    unverifiedCases: List[Dict[str, Any]] = Field(default_factory=list)
    total: int = 0
    provenance: GroundingProvenance = Field(default_factory=GroundingProvenance)


class GroundedEventContext(GroundingBaseModel):
    currentEvent: CurrentEventContext
    regionalContext: RegionalContext
    historicalContext: HistoricalContext
    knowledgeContext: KnowledgeContext
    caseMemoryContext: CaseMemoryContext
    groundingStatus: str = "MINIMAL"
    groundingRefs: List[Dict[str, Any]] = Field(default_factory=list)
    assembledAt: str = Field(default_factory=utc_now_iso)

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump(mode="json", exclude_none=False)
