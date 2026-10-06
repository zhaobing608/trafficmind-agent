"""Compact rendering helpers for grounded Agent input and audit refs."""

from __future__ import annotations

from typing import Any, Dict, List

MAX_RENDERED_FACTS = 12
MAX_RENDERED_REFS = 12


def render_grounded_context_for_agent(context: Dict[str, Any]) -> Dict[str, Any]:
    """Project a GroundedEventContext snapshot into bounded Agent-facing facts."""

    if not isinstance(context, dict):
        return {
            "groundingStatus": "MINIMAL",
            "facts": [],
            "evidenceRefs": [],
        }
    facts = _facts(context)[:MAX_RENDERED_FACTS]
    refs = _evidence_refs(context)[:MAX_RENDERED_REFS]
    return {
        "groundingStatus": context.get("groundingStatus", "MINIMAL"),
        "assembledAt": context.get("assembledAt", ""),
        "facts": facts,
        "evidenceRefs": refs,
    }


def grounding_audit_summary(context: Dict[str, Any]) -> Dict[str, Any]:
    """Return a compact auditable summary suitable for final decisions and Plans."""

    if not isinstance(context, dict):
        return {
            "groundingStatus": "MINIMAL",
            "refs": [],
        }
    regional = context.get("regionalContext") if isinstance(context.get("regionalContext"), dict) else {}
    history = context.get("historicalContext") if isinstance(context.get("historicalContext"), dict) else {}
    knowledge = context.get("knowledgeContext") if isinstance(context.get("knowledgeContext"), dict) else {}
    case_memory = context.get("caseMemoryContext") if isinstance(context.get("caseMemoryContext"), dict) else {}
    location = regional.get("location") if isinstance(regional.get("location"), dict) else {}
    return {
        "groundingStatus": context.get("groundingStatus", "MINIMAL"),
        "assembledAt": context.get("assembledAt", ""),
        "regionId": location.get("regionId"),
        "roadId": location.get("roadId"),
        "intersectionId": location.get("intersectionId"),
        "historicalWindow": history.get("window") or {},
        "historicalEventCount": history.get("eventCount", 0),
        "knowledgeEvidenceRefs": [
            {
                "evidenceId": item.get("evidenceId"),
                "documentId": item.get("documentId"),
                "chunkId": item.get("chunkId"),
                "scopeMatch": item.get("scopeMatch"),
            }
            for item in (knowledge.get("evidence") or [])[:MAX_RENDERED_REFS]
            if isinstance(item, dict)
        ],
        "caseMemoryRefs": [
            {
                "caseId": item.get("caseId"),
                "sourceWorkflowRunId": item.get("sourceWorkflowRunId"),
                "finalStatus": item.get("finalStatus"),
                "qualityStatus": item.get("qualityStatus"),
                "experienceType": item.get("experienceType"),
                "eventOutcome": (
                    (item.get("outcome") or {}).get("eventOutcome")
                    if isinstance(item.get("outcome"), dict) else None
                ),
            }
            for item in (case_memory.get("cases") or [])[:MAX_RENDERED_REFS]
            if isinstance(item, dict)
        ],
        "refs": list(context.get("groundingRefs") or [])[:MAX_RENDERED_REFS],
    }


def _facts(context: Dict[str, Any]) -> List[str]:
    facts: List[str] = []
    regional = context.get("regionalContext") if isinstance(context.get("regionalContext"), dict) else {}
    history = context.get("historicalContext") if isinstance(context.get("historicalContext"), dict) else {}
    knowledge = context.get("knowledgeContext") if isinstance(context.get("knowledgeContext"), dict) else {}
    case_memory = context.get("caseMemoryContext") if isinstance(context.get("caseMemoryContext"), dict) else {}

    location = regional.get("location") if isinstance(regional.get("location"), dict) else {}
    region = regional.get("region") if isinstance(regional.get("region"), dict) else {}
    if regional.get("status") == "READY":
        region_label = region.get("name") or location.get("regionId")
        loc_label = location.get("intersectionName") or location.get("roadName") or location.get("roadId")
        facts.append(f"regional:{region_label}/{loc_label}")
        pois = regional.get("nearbyPois") or []
        if pois:
            poi_names = [str(p.get("name") or p.get("poiId")) for p in pois[:3] if isinstance(p, dict)]
            if poi_names:
                facts.append(f"nearby_poi:{', '.join(poi_names)}")
        roads = regional.get("connectedRoads") or []
        if roads:
            road_names = [str(r.get("name") or r.get("roadId")) for r in roads[:3] if isinstance(r, dict)]
            if road_names:
                facts.append(f"connected_roads:{', '.join(road_names)}")
    else:
        facts.append(f"regional:{regional.get('status', 'UNRESOLVED')}")

    if history.get("status") == "READY":
        facts.append(
            "history:"
            f"count={int(history.get('eventCount') or 0)},"
            f"unclosed={int(history.get('unclosedCount') or 0)},"
            f"maxRisk={history.get('maxRisk')}"
        )
    else:
        facts.append(f"history:{history.get('status', 'UNAVAILABLE')}")

    if knowledge.get("evidence"):
        titles = [str(e.get("title") or e.get("documentId")) for e in knowledge.get("evidence", [])[:3] if isinstance(e, dict)]
        facts.append(f"knowledge:{len(knowledge.get('evidence') or [])} evidence; {', '.join(titles)}")
    else:
        facts.append(f"knowledge:{knowledge.get('status', 'UNAVAILABLE')}")

    if case_memory.get("cases"):
        case_ids = [str(c.get("caseId")) for c in case_memory.get("cases", [])[:3] if isinstance(c, dict)]
        facts.append(f"case_memory:{len(case_memory.get('cases') or [])} cases; {', '.join(case_ids)}")
        for case in (case_memory.get("positiveCases") or [])[:2]:
            if not isinstance(case, dict):
                continue
            recommendation = case.get("recommendation") if isinstance(case.get("recommendation"), dict) else {}
            actions = [
                str(item.get("actionType"))
                for item in (recommendation.get("finalActions") or [])[:4]
                if isinstance(item, dict) and item.get("actionType")
            ]
            facts.append(
                "historical_success:"
                f"case={case.get('caseId')},quality={case.get('qualityStatus')},"
                f"final_actions={','.join(actions) or 'none'},"
                f"why={case.get('whyRelevant') or 'same scoped context'}"
            )
        for case in (case_memory.get("negativeCases") or [])[:2]:
            if not isinstance(case, dict):
                continue
            caution = case.get("caution") if isinstance(case.get("caution"), dict) else {}
            failed = [
                str(item.get("actionType"))
                for item in (caution.get("failedActions") or [])[:4]
                if isinstance(item, dict) and item.get("actionType")
            ]
            facts.append(
                "known_failed_approach:"
                f"case={case.get('caseId')},quality={case.get('qualityStatus')},"
                f"actions={','.join(failed) or 'see_structured_rejection'},"
                "use=caution_only"
            )
    else:
        facts.append(f"case_memory:{case_memory.get('status', 'UNAVAILABLE')}")
    return facts


def _evidence_refs(context: Dict[str, Any]) -> List[Dict[str, Any]]:
    refs = []
    for ref in context.get("groundingRefs") or []:
        if isinstance(ref, dict):
            refs.append(dict(ref))
    return refs
