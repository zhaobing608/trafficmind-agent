"""Deterministic pilot evaluation for outcome-aware Case Memory.

This is an offline read-only replay, not model training and not a claim about
online Agent accuracy.  It measures the small Phase 21.5 retrieval/projection
policy against historical final plans while enforcing a strict-past cutoff.
"""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_DATASET = (
    Path(__file__).resolve().parents[1]
    / "evaluation_data"
    / "trafficmind_feedback_pilot_v1.json"
)

_OUTCOME_TIER = {
    "VERIFIED_SUCCESS": 4,
    "PARTIAL_SUCCESS": 3,
    "INCOMPLETE": 1,
    "UNVERIFIED": 1,
    "validated": 1,
    "partial": 1,
    "FAILED_OUTCOME": 0,
}


def _parse_time(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    # Historical fixtures may use a timezone-less ISO value.  The project has
    # always treated those legacy values as UTC, so normalize at the parser
    # boundary before strict-past or canonical comparisons can mix naive and
    # aware datetimes.
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _strictly_before(value: Any, as_of: Any) -> bool:
    left = _parse_time(value)
    right = _parse_time(as_of)
    return bool(left and right and left < right)


def _time_order_key(value: Any) -> datetime:
    """Return one offset-normalized key for deterministic replay ordering."""

    parsed = _parse_time(value)
    if parsed is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    return parsed


def strict_past_cases(
    current_event: Dict[str, Any],
    cases: Iterable[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Exclude self/future Cases and mask outcome feedback learned later."""

    case_list = list(cases)
    event_id = str(current_event.get("eventId") or "")
    as_of = current_event.get("createdAt")
    if not event_id or _parse_time(as_of) is None:
        return [], {
            "input": len(case_list),
            "eligible": 0,
            "selfExcluded": 0,
            "futureExcluded": 0,
            "futureSystemProjectionExcluded": 0,
            "futureFeedbackMasked": 0,
            "supersededExcluded": 0,
            "failedClosed": 1,
        }
    eligible: List[Dict[str, Any]] = []
    preliminary: List[Dict[str, Any]] = []
    stats = {
        "input": len(case_list),
        "eligible": 0,
        "selfExcluded": 0,
        "futureExcluded": 0,
        "futureSystemProjectionExcluded": 0,
        "futureFeedbackMasked": 0,
        "supersededExcluded": 0,
        "failedClosed": 0,
    }
    for source in case_list:
        if str(source.get("eventId") or "") == event_id:
            stats["selfExcluded"] += 1
            continue
        if not _strictly_before(source.get("completedAt"), as_of):
            stats["futureExcluded"] += 1
            continue
        system_updated_at = source.get("systemUpdatedAt") or source.get("completedAt")
        if not _strictly_before(system_updated_at, as_of):
            stats["futureSystemProjectionExcluded"] += 1
            continue
        item = deepcopy(source)
        feedback_updated_at = item.get("feedbackUpdatedAt")
        if feedback_updated_at and not _strictly_before(feedback_updated_at, as_of):
            item["effectiveQuality"] = "UNVERIFIED"
            item["operatorOutcomeAvailable"] = False
            stats["futureFeedbackMasked"] += 1
        else:
            item["effectiveQuality"] = str(item.get("qualityStatus") or "UNVERIFIED")
            item["operatorOutcomeAvailable"] = True
        preliminary.append(item)

    # Canonical identity is time-relative.  A currently superseded row remains
    # the valid historical winner when its replacement did not exist at the
    # replay cutoff.  Select one winner per Event from cutoff-eligible rows,
    # matching production's (completedAt, caseId) rule.  A non-canonical row
    # without its named successor is an incomplete input and fails closed.
    winners: Dict[str, Dict[str, Any]] = {}
    for item in preliminary:
        source_event_id = str(item.get("eventId") or "")
        current = winners.get(source_event_id)
        item_key = (_parse_time(item.get("completedAt")), str(item.get("caseId") or ""))
        current_key = (
            _parse_time(current.get("completedAt")),
            str(current.get("caseId") or ""),
        ) if current else None
        if current is None or item_key > current_key:
            winners[source_event_id] = item

    all_by_id = {
        str(item.get("caseId") or ""): item
        for item in case_list
        if item.get("caseId")
    }
    eligible_ids = {
        str(item.get("caseId") or "") for item in preliminary
    }
    for item in preliminary:
        source_event_id = str(item.get("eventId") or "")
        if winners.get(source_event_id) is not item:
            stats["supersededExcluded"] += 1
            continue
        if item.get("isCanonical") is False:
            successor_id = str(item.get("supersededByCaseId") or "")
            if not successor_id or successor_id not in all_by_id:
                stats["supersededExcluded"] += 1
                continue
            if successor_id in eligible_ids:
                stats["supersededExcluded"] += 1
                continue
        eligible.append(item)
    stats["eligible"] = len(eligible)
    return eligible, stats


def rank_cases(
    cases: Iterable[Dict[str, Any]],
    *,
    outcome_aware: bool,
) -> List[Dict[str, Any]]:
    """Rank with the same explainable tiers as production retrieval."""

    def key(item: Dict[str, Any]) -> tuple:
        similarity = float(item.get("baseSimilarity") or 0.0)
        if not outcome_aware:
            return (similarity, _time_order_key(item.get("completedAt")))
        return (
            int(item.get("locationTier") or 1),
            _OUTCOME_TIER.get(str(item.get("effectiveQuality") or "UNVERIFIED"), -1),
            _time_order_key(item.get("completedAt")),
        )

    # Match production's stable final case_id ordering when all policy keys
    # are equal.  Python's sort is stable, so a pre-sort supplies the ascending
    # tie-break while the policy sort remains descending.
    ranked = sorted(
        (deepcopy(item) for item in cases),
        key=lambda item: str(item.get("caseId") or ""),
    )
    return sorted(ranked, key=key, reverse=True)


def _action_type(action: Dict[str, Any]) -> str:
    return str(action.get("actionType") or "").strip()


def _signature(action: Dict[str, Any]) -> str:
    safe = {
        "actionType": _action_type(action),
        "params": action.get("params") if isinstance(action.get("params"), dict) else {},
    }
    return json.dumps(safe, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def acceptance_score(
    proposal: Iterable[Dict[str, Any]],
    reference: Iterable[Dict[str, Any]],
) -> float:
    """Jaccard agreement over exact action type + business params."""

    proposed = {_signature(item) for item in proposal if isinstance(item, dict)}
    expected = {_signature(item) for item in reference if isinstance(item, dict)}
    if not proposed and not expected:
        return 1.0
    union = proposed | expected
    return round(len(proposed & expected) / len(union), 4) if union else 1.0


def _negative_action_types(cases: Iterable[Dict[str, Any]]) -> set[str]:
    result: set[str] = set()
    for case in cases:
        if str(case.get("effectiveQuality")) != "FAILED_OUTCOME":
            continue
        for action in case.get("rejectedActions") or []:
            if isinstance(action, dict) and _action_type(action):
                result.add(_action_type(action))
        for action in case.get("failedActions") or []:
            if isinstance(action, dict) and _action_type(action):
                result.add(_action_type(action))
    return result


def project_outcome_aware_proposal(
    baseline: Iterable[Dict[str, Any]],
    ranked_cases: Iterable[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Apply final adopted params from positive history and negative cautions."""

    ranked = list(ranked_cases)
    proposal = [deepcopy(item) for item in baseline if isinstance(item, dict)]
    negative_types = _negative_action_types(ranked)
    proposal = [item for item in proposal if _action_type(item) not in negative_types]

    positive_cases = [
        item for item in ranked
        if str(item.get("effectiveQuality")) in {"VERIFIED_SUCCESS", "PARTIAL_SUCCESS"}
    ]
    adopted: Dict[str, Dict[str, Any]] = {}
    for case in positive_cases:
        for action in case.get("finalActions") or []:
            if isinstance(action, dict) and _action_type(action) and _action_type(action) not in adopted:
                adopted[_action_type(action)] = deepcopy(action)
    proposal_types = {_action_type(item) for item in proposal}
    proposal = [adopted.get(_action_type(item), item) for item in proposal]
    # A verified final plan may contain an adopted action absent from the raw
    # baseline.  Add it once, but never copy a FAILED_OUTCOME action.
    for action_type, action in adopted.items():
        if action_type not in proposal_types and action_type not in negative_types:
            proposal.append(action)
    return proposal, {
        "positiveCasesUsed": [case.get("caseId") for case in positive_cases],
        "negativeCasesUsed": [
            case.get("caseId") for case in ranked
            if str(case.get("effectiveQuality")) == "FAILED_OUTCOME"
        ],
        "avoidedActionTypes": sorted(negative_types),
    }

def _mean(values: Iterable[float]) -> float:
    items = list(values)
    return round(sum(items) / len(items), 4) if items else 0.0


def run_feedback_pilot(dataset_path: Optional[str] = None) -> Dict[str, Any]:
    path = Path(dataset_path) if dataset_path else DEFAULT_DATASET
    payload = json.loads(path.read_text(encoding="utf-8"))
    results: List[Dict[str, Any]] = []
    for replay in payload.get("replays") or []:
        current = replay.get("currentEvent") or {}
        eligible, leakage = strict_past_cases(current, replay.get("historicalCases") or [])
        baseline_ranked = rank_cases(eligible, outcome_aware=False)
        outcome_ranked = rank_cases(eligible, outcome_aware=True)
        baseline_proposal = list(replay.get("baselineProposal") or [])
        outcome_proposal, use = project_outcome_aware_proposal(
            baseline_proposal,
            outcome_ranked,
        )
        reference = list(replay.get("referenceFinalPlan") or [])
        negative_types = _negative_action_types(outcome_ranked)
        baseline_rejected = {
            _action_type(item) for item in baseline_proposal
            if isinstance(item, dict) and _action_type(item) in negative_types
        }
        outcome_rejected = {
            _action_type(item) for item in outcome_proposal
            if isinstance(item, dict) and _action_type(item) in negative_types
        }
        top_quality = (
            str(outcome_ranked[0].get("effectiveQuality")) if outcome_ranked else None
        )
        results.append({
            "replayId": replay.get("replayId"),
            "historyClass": replay.get("historyClass"),
            "strictPast": leakage,
            "referenceFinalPlan": reference,
            "baseline": {
                "proposal": baseline_proposal,
                "acceptance": acceptance_score(baseline_proposal, reference),
                "rejectedActionsRecommended": sorted(baseline_rejected),
                "topCaseId": baseline_ranked[0].get("caseId") if baseline_ranked else None,
            },
            "outcomeAware": {
                "proposal": outcome_proposal,
                "acceptance": acceptance_score(outcome_proposal, reference),
                "rejectedActionsRecommended": sorted(outcome_rejected),
                "topCaseId": outcome_ranked[0].get("caseId") if outcome_ranked else None,
                "topCaseQuality": top_quality,
                "successfulCaseRankedFirst": top_quality == "VERIFIED_SUCCESS",
                "negativeMemoryUsed": bool(use["negativeCasesUsed"]),
                **use,
            },
        })

    baseline_acceptance = _mean(item["baseline"]["acceptance"] for item in results)
    outcome_acceptance = _mean(item["outcomeAware"]["acceptance"] for item in results)
    baseline_rejected_rate = _mean(
        1.0 if item["baseline"]["rejectedActionsRecommended"] else 0.0
        for item in results
    )
    outcome_rejected_rate = _mean(
        1.0 if item["outcomeAware"]["rejectedActionsRecommended"] else 0.0
        for item in results
    )
    return {
        "metadata": {
            "evaluationType": "pilot",
            "datasetVersion": payload.get("datasetVersion", "feedback-pilot-v1"),
            "datasetPath": str(path),
            "caseCount": len(results),
            "usesLLM": False,
            "agentReplayExecuted": False,
            "onlineLearning": False,
            "evaluationScope": "memory_policy_replay",
            "referenceSource": "deterministic_fixture_final_plans",
            "limitations": [
                "small deterministic fixture set",
                "measures retrieval/projection policy, not live model accuracy",
            ],
        },
        "metrics": {
            "recommendationAcceptance": {
                "baseline": baseline_acceptance,
                "outcomeAware": outcome_acceptance,
                "delta": round(outcome_acceptance - baseline_acceptance, 4),
            },
            "rejectedActionRecommendationRate": {
                "baseline": baseline_rejected_rate,
                "outcomeAware": outcome_rejected_rate,
            },
            "rejectedActionAvoidance": round(1.0 - outcome_rejected_rate, 4),
            "outcomeAwareRetrieval": _mean(
                1.0 if item["outcomeAware"]["successfulCaseRankedFirst"] else 0.0
                for item in results
            ),
            "negativeMemoryUse": _mean(
                1.0 if item["outcomeAware"]["negativeMemoryUsed"] else 0.0
                for item in results
                if item["historyClass"] == "failed"
            ),
            "strictPastSelfExclusions": sum(
                item["strictPast"]["selfExcluded"] for item in results
            ),
            "strictPastFutureExclusions": sum(
                item["strictPast"]["futureExcluded"] for item in results
            ),
            "futureFeedbackMasks": sum(
                item["strictPast"]["futureFeedbackMasked"] for item in results
            ),
            "futureSystemProjectionExclusions": sum(
                item["strictPast"]["futureSystemProjectionExcluded"]
                for item in results
            ),
            "supersededCaseExclusions": sum(
                item["strictPast"]["supersededExcluded"] for item in results
            ),
        },
        "results": results,
    }
