"""SQLite repository for Traffic Case Memory."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import backend.config as _config
from backend.case_memory.models import (
    CaseMemoryQuality,
    FeedbackEffectiveness,
    FeedbackLifecycle,
    FeedbackReasonCode,
    EventOutcome,
    TrafficCaseMemory,
    TrafficEventFeedback,
    utc_now_iso,
)


_AGENT_RETRIEVAL_QUALITIES = (
    CaseMemoryQuality.VERIFIED_SUCCESS.value,
    CaseMemoryQuality.PARTIAL_SUCCESS.value,
    CaseMemoryQuality.FAILED_OUTCOME.value,
    CaseMemoryQuality.UNVERIFIED.value,
    CaseMemoryQuality.INCOMPLETE.value,
    # Existing databases retain these values until a source chain is rebuilt.
    CaseMemoryQuality.VALIDATED.value,
    CaseMemoryQuality.PARTIAL.value,
)


class CaseProjectionConflict(RuntimeError):
    """The Feedback revision changed while a derived Case was being rebuilt."""


_INITIALIZED_DATABASES: set[tuple[str, int, int]] = set()
_INITIALIZATION_LOCK = threading.RLock()


def _database_identity() -> Optional[tuple[str, int, int]]:
    """Identify a concrete DB file, including replacement at the same path."""

    path = os.path.realpath(_config.DB_PATH)
    try:
        stat = os.stat(path)
    except OSError:
        return None
    return (path, int(stat.st_dev), int(stat.st_ino))


def _get_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(_config.DB_PATH), exist_ok=True)
    conn = sqlite3.connect(_config.DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _json_dump(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False)


def _json_load(value: Any, default: Any) -> Any:
    if not isinstance(value, str):
        return value if value is not None else default
    try:
        return json.loads(value) if value else default
    except json.JSONDecodeError:
        return default


def _merge_action_assessments(
    existing: Sequence[Dict[str, Any]],
    incoming: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Merge by durable Action identity while preserving stable list order."""

    merged = [dict(item) for item in existing if isinstance(item, dict)]
    positions = {
        str(item.get("actionExecutionId") or ""): index
        for index, item in enumerate(merged)
        if item.get("actionExecutionId")
    }
    for item in incoming:
        if not isinstance(item, dict):
            continue
        copied = dict(item)
        action_id = str(copied.get("actionExecutionId") or "")
        if action_id and action_id in positions:
            merged[positions[action_id]] = copied
        else:
            if action_id:
                positions[action_id] = len(merged)
            merged.append(copied)
    return merged


def _strictly_before(value: Any, as_of: Any) -> bool:
    try:
        left = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        right = datetime.fromisoformat(str(as_of).replace("Z", "+00:00"))
        return left < right
    except (TypeError, ValueError):
        return False


def _timestamp_order_key(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        parsed = datetime.min
    # All project timestamps are expected to be UTC/offset-aware.  Normalize the
    # rare legacy naive value so tuple comparison remains deterministic.
    if parsed.tzinfo is not None:
        parsed = parsed.replace(tzinfo=None) - parsed.utcoffset()
    return parsed


def _case_order_key(
    completed_at: Any,
    case_id: Any,
) -> tuple[datetime, str]:
    return (
        _timestamp_order_key(completed_at),
        str(case_id or ""),
    )


def _canonical_as_of_clause() -> str:
    """Select the deterministic Event winner using only facts known at cutoff."""

    return """
        NOT EXISTS (
            SELECT 1 FROM traffic_case_memories AS later_case
            WHERE later_case.event_id = traffic_case_memories.event_id
              AND julianday(later_case.completed_at) IS NOT NULL
              AND julianday(later_case.completed_at) < julianday(?)
              AND julianday(COALESCE(later_case.system_updated_at,
                                     later_case.completed_at)) < julianday(?)
              AND (
                    julianday(later_case.completed_at) >
                        julianday(traffic_case_memories.completed_at)
                    OR (
                        julianday(later_case.completed_at) =
                            julianday(traffic_case_memories.completed_at)
                        AND later_case.case_id > traffic_case_memories.case_id
                    )
              )
        )
    """


def _reconcile_canonical_event(
    conn: sqlite3.Connection,
    event_id: str,
) -> Optional[str]:
    """Select one deterministic canonical Case and supersede every other attempt."""

    rows = conn.execute(
        """SELECT case_id, completed_at
           FROM traffic_case_memories WHERE event_id=?""",
        (event_id,),
    ).fetchall()
    if not rows:
        return None
    winner = max(
        rows,
        key=lambda row: _case_order_key(
            row["completed_at"], row["case_id"]
        ),
    )
    canonical_id = str(winner["case_id"])
    # Demote first so the partial unique index can never observe two canonical
    # rows, even when a later Workflow attempt replaces the current winner.
    conn.execute(
        """UPDATE traffic_case_memories
           SET is_canonical=0, superseded_by_case_id=?
           WHERE event_id=?""",
        (canonical_id, event_id),
    )
    conn.execute(
        """UPDATE traffic_case_memories
           SET is_canonical=1, superseded_by_case_id=NULL
           WHERE case_id=?""",
        (canonical_id,),
    )
    return canonical_id


def init_case_memory_tables() -> None:
    with _INITIALIZATION_LOCK:
        conn = _get_conn()
        try:
            conn.executescript(
                """
            CREATE TABLE IF NOT EXISTS traffic_case_memories (
                case_id TEXT PRIMARY KEY,
                region_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                road_id TEXT DEFAULT NULL,
                intersection_id TEXT DEFAULT NULL,
                source_session_id TEXT DEFAULT NULL,
                source_collaboration_run_id TEXT DEFAULT NULL,
                source_plan_id TEXT DEFAULT NULL,
                source_workflow_run_id TEXT NOT NULL UNIQUE,
                final_status TEXT NOT NULL,
                quality_status TEXT NOT NULL,
                event_snapshot_json TEXT NOT NULL DEFAULT '{}',
                agent_facts_json TEXT NOT NULL DEFAULT '{}',
                plan_facts_json TEXT NOT NULL DEFAULT '{}',
                human_decisions_json TEXT NOT NULL DEFAULT '[]',
                workflow_outcome_json TEXT NOT NULL DEFAULT '{}',
                recommendation_feedback_json TEXT NOT NULL DEFAULT '{}',
                action_feedback_json TEXT NOT NULL DEFAULT '[]',
                event_outcome_json TEXT NOT NULL DEFAULT '{}',
                feedback_lifecycle TEXT NOT NULL DEFAULT 'PENDING',
                feedback_updated_at TEXT DEFAULT NULL,
                feedback_revision INTEGER DEFAULT NULL,
                projection_revision INTEGER NOT NULL DEFAULT 1,
                system_updated_at TEXT DEFAULT NULL,
                is_canonical INTEGER NOT NULL DEFAULT 1,
                superseded_by_case_id TEXT DEFAULT NULL,
                lessons_json TEXT NOT NULL DEFAULT '[]',
                generated_summary TEXT DEFAULT NULL,
                started_at TEXT DEFAULT NULL,
                completed_at TEXT DEFAULT NULL,
                source_type TEXT NOT NULL DEFAULT 'workflow_case_builder',
                source_reference TEXT DEFAULT '',
                provenance_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS traffic_event_feedback (
                feedback_id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                workflow_run_id TEXT NOT NULL,
                agent_run_id TEXT DEFAULT NULL,
                plan_id TEXT DEFAULT NULL,
                plan_version INTEGER DEFAULT NULL,
                approval_id TEXT DEFAULT NULL,
                action_execution_id TEXT DEFAULT NULL,
                action_assessments_json TEXT NOT NULL DEFAULT '[]',
                event_outcome TEXT NOT NULL DEFAULT 'UNKNOWN',
                effectiveness TEXT NOT NULL DEFAULT 'UNKNOWN',
                reason_code TEXT NOT NULL DEFAULT 'NONE',
                comment TEXT NOT NULL DEFAULT '',
                reviewer TEXT NOT NULL DEFAULT '',
                lifecycle TEXT NOT NULL DEFAULT 'PENDING',
                revision INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(event_id, workflow_run_id)
            );

            CREATE INDEX IF NOT EXISTS idx_case_memory_region_event
                ON traffic_case_memories(region_id, event_type);
            CREATE INDEX IF NOT EXISTS idx_case_memory_region_road
                ON traffic_case_memories(region_id, road_id);
            CREATE INDEX IF NOT EXISTS idx_case_memory_region_intersection
                ON traffic_case_memories(region_id, intersection_id);
            CREATE INDEX IF NOT EXISTS idx_case_memory_source_event
                ON traffic_case_memories(event_id);
            CREATE INDEX IF NOT EXISTS idx_case_memory_source_plan
                ON traffic_case_memories(source_plan_id);
            CREATE INDEX IF NOT EXISTS idx_case_memory_status
                ON traffic_case_memories(final_status, quality_status);
            CREATE INDEX IF NOT EXISTS idx_case_memory_completed
                ON traffic_case_memories(completed_at);
            CREATE INDEX IF NOT EXISTS idx_event_feedback_event
                ON traffic_event_feedback(event_id, updated_at);
            CREATE INDEX IF NOT EXISTS idx_event_feedback_workflow
                ON traffic_event_feedback(workflow_run_id);
                """
            )
            # Serialize schema inspection + ALTER across worker processes.  The
            # process-local lock/cache only avoids repeated work within one
            # worker; BEGIN IMMEDIATE closes the cross-process check/ALTER race.
            conn.execute("BEGIN IMMEDIATE")
            # Non-destructive Phase 21.5 migration for existing Case Memory DBs.
            existing_case_columns = {
                row[1] for row in conn.execute(
                    "PRAGMA table_info(traffic_case_memories)"
                ).fetchall()
            }
            case_column_defs = {
                "recommendation_feedback_json": "TEXT NOT NULL DEFAULT '{}'",
                "action_feedback_json": "TEXT NOT NULL DEFAULT '[]'",
                "event_outcome_json": "TEXT NOT NULL DEFAULT '{}'",
                "feedback_lifecycle": "TEXT NOT NULL DEFAULT 'PENDING'",
                "feedback_updated_at": "TEXT DEFAULT NULL",
                "feedback_revision": "INTEGER DEFAULT NULL",
                "projection_revision": "INTEGER NOT NULL DEFAULT 1",
                "system_updated_at": "TEXT DEFAULT NULL",
                "is_canonical": "INTEGER NOT NULL DEFAULT 1",
                "superseded_by_case_id": "TEXT DEFAULT NULL",
            }
            for column_name, column_def in case_column_defs.items():
                if column_name not in existing_case_columns:
                    conn.execute(
                        f"ALTER TABLE traffic_case_memories ADD COLUMN {column_name} {column_def}"
                    )
            conn.execute(
                "UPDATE traffic_case_memories "
                "SET system_updated_at=COALESCE(updated_at, completed_at, created_at) "
                "WHERE system_updated_at IS NULL"
            )
            existing_feedback_columns = {
                row[1] for row in conn.execute(
                    "PRAGMA table_info(traffic_event_feedback)"
                ).fetchall()
            }
            feedback_column_defs = {
                "action_assessments_json": "TEXT NOT NULL DEFAULT '[]'",
                "revision": "INTEGER NOT NULL DEFAULT 1",
            }
            for column_name, column_def in feedback_column_defs.items():
                if column_name not in existing_feedback_columns:
                    conn.execute(
                        f"ALTER TABLE traffic_event_feedback ADD COLUMN {column_name} {column_def}"
                    )
            # Preserve every workflow attempt for audit, while exposing exactly one
            # canonical Case per Event to retrieval and metrics.  Existing databases
            # are reconciled deterministically before the partial unique index is made.
            inconsistent_events = conn.execute(
                """SELECT event_id FROM traffic_case_memories
                   GROUP BY event_id
                   HAVING SUM(CASE WHEN is_canonical=1 THEN 1 ELSE 0 END) <> 1"""
            ).fetchall()
            for inconsistent in inconsistent_events:
                _reconcile_canonical_event(conn, str(inconsistent["event_id"]))
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_case_memory_canonical_event "
                "ON traffic_case_memories(event_id) WHERE is_canonical=1"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_case_memory_feedback_updated "
                "ON traffic_case_memories(feedback_updated_at)"
            )
            conn.commit()
            identity = _database_identity()
            if identity is not None:
                _INITIALIZED_DATABASES.add(identity)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _ensure_case_memory_tables() -> None:
    with _INITIALIZATION_LOCK:
        identity = _database_identity()
        if identity is not None and identity in _INITIALIZED_DATABASES:
            return
        init_case_memory_tables()


class SQLiteCaseMemoryRepository:
    def get_feedback(
        self,
        event_id: str,
        workflow_run_id: str,
    ) -> Optional[TrafficEventFeedback]:
        _ensure_case_memory_tables()
        conn = _get_conn()
        try:
            row = conn.execute(
                """SELECT * FROM traffic_event_feedback
                   WHERE event_id = ? AND workflow_run_id = ?""",
                (event_id, workflow_run_id),
            ).fetchone()
            return self._row_to_feedback(row) if row else None
        finally:
            conn.close()

    def list_feedback_for_event(self, event_id: str) -> List[TrafficEventFeedback]:
        _ensure_case_memory_tables()
        conn = _get_conn()
        try:
            rows = conn.execute(
                """SELECT * FROM traffic_event_feedback WHERE event_id = ?
                   ORDER BY updated_at DESC, feedback_id""",
                (event_id,),
            ).fetchall()
            return [self._row_to_feedback(row) for row in rows]
        finally:
            conn.close()

    def upsert_feedback(
        self,
        feedback: TrafficEventFeedback,
        *,
        action_assessment_mode: str = "replace",
        preserve_action_execution_id: bool = False,
    ) -> tuple[TrafficEventFeedback, bool]:
        """Create or update the one feedback row for an exact Event/Run chain."""

        if action_assessment_mode not in {"replace", "preserve", "merge"}:
            raise ValueError("unsupported action assessment update mode")
        _ensure_case_memory_tables()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                """SELECT feedback_id, created_at, revision,
                          action_execution_id, action_assessments_json
                   FROM traffic_event_feedback
                   WHERE event_id = ? AND workflow_run_id = ?""",
                (feedback.event_id, feedback.workflow_run_id),
            ).fetchone()
            created = existing is None
            if existing is not None:
                feedback.feedback_id = str(existing["feedback_id"])
                feedback.created_at = str(existing["created_at"])
                feedback.updated_at = utc_now_iso()
                feedback.revision = int(existing["revision"] or 1) + 1
                if preserve_action_execution_id and not feedback.action_execution_id:
                    feedback.action_execution_id = existing["action_execution_id"]
                existing_assessments = _json_load(
                    existing["action_assessments_json"], []
                )
                if action_assessment_mode == "preserve":
                    feedback.action_assessments = existing_assessments
                elif action_assessment_mode == "merge":
                    feedback.action_assessments = _merge_action_assessments(
                        existing_assessments,
                        feedback.action_assessments,
                    )
            else:
                feedback.revision = 1
            conn.execute(
                """
                INSERT INTO traffic_event_feedback (
                    feedback_id, event_id, workflow_run_id, agent_run_id,
                    plan_id, plan_version, approval_id, action_execution_id,
                    action_assessments_json,
                    event_outcome, effectiveness, reason_code, comment, reviewer,
                    lifecycle, revision, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id, workflow_run_id) DO UPDATE SET
                    agent_run_id=excluded.agent_run_id,
                    plan_id=excluded.plan_id,
                    plan_version=excluded.plan_version,
                    approval_id=excluded.approval_id,
                    action_execution_id=excluded.action_execution_id,
                    action_assessments_json=excluded.action_assessments_json,
                    event_outcome=excluded.event_outcome,
                    effectiveness=excluded.effectiveness,
                    reason_code=excluded.reason_code,
                    comment=excluded.comment,
                    reviewer=excluded.reviewer,
                    lifecycle=excluded.lifecycle,
                    revision=excluded.revision,
                    updated_at=excluded.updated_at
                """,
                (
                    feedback.feedback_id,
                    feedback.event_id,
                    feedback.workflow_run_id,
                    feedback.agent_run_id,
                    feedback.plan_id,
                    feedback.plan_version,
                    feedback.approval_id,
                    feedback.action_execution_id,
                    _json_dump(feedback.action_assessments),
                    feedback.event_outcome.value,
                    feedback.effectiveness.value,
                    feedback.reason_code.value,
                    feedback.comment,
                    feedback.reviewer,
                    feedback.lifecycle.value,
                    feedback.revision,
                    feedback.created_at,
                    feedback.updated_at,
                ),
            )
            conn.commit()
            return feedback, created
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_case(self, case_id: str) -> Optional[TrafficCaseMemory]:
        _ensure_case_memory_tables()
        conn = _get_conn()
        try:
            row = conn.execute(
                "SELECT * FROM traffic_case_memories WHERE case_id = ?",
                (case_id,),
            ).fetchone()
            return self._row_to_case(row) if row else None
        finally:
            conn.close()

    def get_case_by_source_workflow_run_id(self, run_id: str) -> Optional[TrafficCaseMemory]:
        _ensure_case_memory_tables()
        conn = _get_conn()
        try:
            row = conn.execute(
                "SELECT * FROM traffic_case_memories WHERE source_workflow_run_id = ?",
                (run_id,),
            ).fetchone()
            return self._row_to_case(row) if row else None
        finally:
            conn.close()

    def list_cases_for_source_event(self, event_id: str) -> List[TrafficCaseMemory]:
        _ensure_case_memory_tables()
        conn = _get_conn()
        try:
            rows = conn.execute(
                """
                SELECT * FROM traffic_case_memories
                WHERE event_id = ?
                ORDER BY completed_at DESC, updated_at DESC, case_id
                """,
                (event_id,),
            ).fetchall()
            return [self._row_to_case(row) for row in rows]
        finally:
            conn.close()

    def insert_case(
        self,
        case: TrafficCaseMemory,
        *,
        expected_feedback_revision: Optional[int] = None,
    ) -> TrafficCaseMemory:
        _ensure_case_memory_tables()
        if not case.system_updated_at:
            case.system_updated_at = case.completed_at or case.created_at
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if expected_feedback_revision is not None:
                payload_revision = int(case.feedback_revision or 0)
                if payload_revision != int(expected_feedback_revision):
                    raise CaseProjectionConflict(
                        "Case payload does not match the expected Feedback revision"
                    )
                current = conn.execute(
                    """SELECT revision FROM traffic_event_feedback
                       WHERE event_id=? AND workflow_run_id=?""",
                    (case.event_id, case.source_workflow_run_id),
                ).fetchone()
                actual_revision = int(current["revision"] or 0) if current else 0
                if actual_revision != int(expected_feedback_revision):
                    raise CaseProjectionConflict(
                        "feedback changed while Case projection was being inserted"
                    )
            # Insert non-canonical first, then atomically elect the latest
            # completed attempt.  This avoids partial-index conflicts and keeps
            # every older audit row pointed at the current canonical Case.
            case.is_canonical = False
            case.superseded_by_case_id = None
            conn.execute(
                """
                INSERT INTO traffic_case_memories (
                    case_id, region_id, event_id, event_type, road_id, intersection_id,
                    source_session_id, source_collaboration_run_id, source_plan_id,
                    source_workflow_run_id, final_status, quality_status,
                    event_snapshot_json, agent_facts_json, plan_facts_json,
                    human_decisions_json, workflow_outcome_json, lessons_json,
                    recommendation_feedback_json, action_feedback_json,
                    event_outcome_json, feedback_lifecycle, feedback_updated_at,
                    feedback_revision, projection_revision, system_updated_at,
                    is_canonical,
                    superseded_by_case_id,
                    generated_summary, started_at, completed_at, source_type,
                    source_reference, provenance_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                self._case_values(case),
            )
            canonical_id = _reconcile_canonical_event(conn, case.event_id)
            case.is_canonical = canonical_id == case.case_id
            case.superseded_by_case_id = None if case.is_canonical else canonical_id
            conn.commit()
            return case
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def update_case_preserving_identity(
        self,
        existing: TrafficCaseMemory,
        rebuilt: TrafficCaseMemory,
        *,
        expected_feedback_revision: Optional[int] = None,
    ) -> TrafficCaseMemory:
        _ensure_case_memory_tables()
        if (
            rebuilt.event_id != existing.event_id
            or rebuilt.source_workflow_run_id != existing.source_workflow_run_id
        ):
            raise CaseProjectionConflict(
                "Case source identity cannot change during a projection update"
            )
        rebuilt.case_id = existing.case_id
        rebuilt.created_at = existing.created_at
        expected_projection_revision = int(existing.projection_revision or 1)
        rebuilt.projection_revision = expected_projection_revision + 1
        # ``existing`` may be stale: another Workflow attempt for the Event can
        # become canonical after it was read.  Always update this row in a
        # demoted state, then elect the winner inside the same transaction.
        rebuilt.is_canonical = False
        rebuilt.superseded_by_case_id = None
        if not rebuilt.system_updated_at:
            rebuilt.system_updated_at = existing.system_updated_at or rebuilt.completed_at
        rebuilt.updated_at = utc_now_iso()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if expected_feedback_revision is not None:
                payload_revision = int(rebuilt.feedback_revision or 0)
                if payload_revision != int(expected_feedback_revision):
                    raise CaseProjectionConflict(
                        "Case payload does not match the expected Feedback revision"
                    )
                current = conn.execute(
                    """SELECT revision FROM traffic_event_feedback
                       WHERE event_id=? AND workflow_run_id=?""",
                    (rebuilt.event_id, rebuilt.source_workflow_run_id),
                ).fetchone()
                actual_revision = int(current["revision"] or 0) if current else 0
                if actual_revision != int(expected_feedback_revision):
                    raise CaseProjectionConflict(
                        "feedback changed while Case projection was being rebuilt"
                    )
            cursor = conn.execute(
                """
                UPDATE traffic_case_memories SET
                    region_id = ?, event_id = ?, event_type = ?, road_id = ?,
                    intersection_id = ?, source_session_id = ?,
                    source_collaboration_run_id = ?, source_plan_id = ?,
                    source_workflow_run_id = ?, final_status = ?, quality_status = ?,
                    event_snapshot_json = ?, agent_facts_json = ?,
                    plan_facts_json = ?, human_decisions_json = ?,
                    workflow_outcome_json = ?, lessons_json = ?,
                    recommendation_feedback_json = ?, action_feedback_json = ?,
                    event_outcome_json = ?, feedback_lifecycle = ?,
                    feedback_updated_at = ?, feedback_revision = ?,
                    projection_revision = ?, system_updated_at = ?,
                    is_canonical = ?,
                    superseded_by_case_id = ?, generated_summary = ?,
                    started_at = ?, completed_at = ?, source_type = ?,
                    source_reference = ?, provenance_json = ?, created_at = ?,
                    updated_at = ?
                WHERE case_id = ? AND projection_revision = ?
                """,
                self._case_values(rebuilt)[1:]
                + (rebuilt.case_id, expected_projection_revision),
            )
            if cursor.rowcount != 1:
                raise CaseProjectionConflict(
                    "Case changed or disappeared during projection update"
                )
            canonical_id = _reconcile_canonical_event(conn, rebuilt.event_id)
            rebuilt.is_canonical = canonical_id == rebuilt.case_id
            rebuilt.superseded_by_case_id = (
                None if rebuilt.is_canonical else canonical_id
            )
            conn.commit()
            return rebuilt
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

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
        _ensure_case_memory_tables()
        bounded_limit = max(1, min(int(limit or 5), 50))
        where = ["region_id = ?", "event_type = ?"]
        params: List[Any] = [region_id, event_type]
        if road_id:
            where.append("road_id = ?")
            params.append(road_id)
        if intersection_id:
            where.append("intersection_id = ?")
            params.append(intersection_id)
        if final_status:
            where.append("final_status = ?")
            params.append(final_status)
        if quality_status:
            if for_agent and as_of:
                where.append(
                    "(CASE WHEN feedback_updated_at IS NOT NULL "
                    "AND julianday(feedback_updated_at) >= julianday(?) "
                    f"THEN '{CaseMemoryQuality.UNVERIFIED.value}' "
                    "ELSE quality_status END) = ?"
                )
                params.extend([as_of, quality_status])
            else:
                where.append("quality_status = ?")
                params.append(quality_status)
        elif for_agent:
            placeholders = ", ".join("?" for _ in _AGENT_RETRIEVAL_QUALITIES)
            where.append(f"quality_status IN ({placeholders})")
            params.extend(_AGENT_RETRIEVAL_QUALITIES)
        if for_agent and not as_of:
            where.append("is_canonical = 1")
        if as_of:
            where.append("julianday(completed_at) IS NOT NULL")
            where.append("julianday(completed_at) < julianday(?)")
            params.append(as_of)
            if for_agent:
                where.append(
                    "julianday(COALESCE(system_updated_at, completed_at)) < julianday(?)"
                )
                params.append(as_of)
                where.append(_canonical_as_of_clause())
                params.extend([as_of, as_of])

        clause = " AND ".join(where)
        order_by = (
            "julianday(completed_at) DESC, case_id"
            if for_agent and as_of
            else "completed_at DESC, updated_at DESC, case_id"
        )
        conn = _get_conn()
        try:
            total_row = conn.execute(
                f"SELECT COUNT(*) AS total FROM traffic_case_memories WHERE {clause}",
                tuple(params),
            ).fetchone()
            rows = conn.execute(
                f"""
                SELECT * FROM traffic_case_memories
                WHERE {clause}
                ORDER BY {order_by}
                LIMIT ?
                """,
                tuple(params + [bounded_limit]),
            ).fetchall()
            return {
                "cases": [self._row_to_case(row) for row in rows],
                "total": total_row["total"] if total_row else 0,
                "limit": bounded_limit,
            }
        finally:
            conn.close()

    def find_context_candidates(
        self,
        *,
        region_id: str,
        event_type: str,
        road_id: Optional[str],
        intersection_id: Optional[str],
        as_of: Optional[str],
        limit: int,
        exclude_event_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        # Strict-past retrieval fails closed when the source Event has no valid
        # as-of timestamp.  The service validates the timestamp and direct
        # repository callers receive an empty, non-leaking result.
        if not as_of:
            return {
                "cases": [],
                "total": 0,
                "limit": max(1, min(int(limit or 5), 20)),
                "retrievalMetadata": {},
            }
        bounded_limit = max(1, min(int(limit or 5), 20))
        placeholders = ", ".join("?" for _ in _AGENT_RETRIEVAL_QUALITIES)
        where = [
            "region_id = ?",
            "event_type = ?",
            f"quality_status IN ({placeholders})",
            "julianday(completed_at) IS NOT NULL",
            "julianday(completed_at) < julianday(?)",
            "julianday(COALESCE(system_updated_at, completed_at)) < julianday(?)",
            _canonical_as_of_clause(),
        ]
        params: List[Any] = [
            region_id,
            event_type,
            *_AGENT_RETRIEVAL_QUALITIES,
            as_of,
            as_of,
            as_of,
            as_of,
        ]
        if exclude_event_id:
            where.append("event_id <> ?")
            params.append(exclude_event_id)
        clause = " AND ".join(where)
        # A feedback assessment is available to a replay only when it existed
        # strictly before the replay cut-off.  Later feedback is ranked and
        # projected as UNVERIFIED; masking it only after SQL ordering would
        # still leak future outcome information.
        effective_quality_sql = (
            "CASE WHEN feedback_updated_at IS NOT NULL "
            "AND julianday(feedback_updated_at) >= julianday(?) "
            f"THEN '{CaseMemoryQuality.UNVERIFIED.value}' "
            "ELSE quality_status END"
        )
        order_params: List[Any] = [
            as_of,
            intersection_id,
            intersection_id,
            road_id,
            road_id,
            as_of,
        ]
        conn = _get_conn()
        try:
            total_row = conn.execute(
                f"SELECT COUNT(*) AS total FROM traffic_case_memories WHERE {clause}",
                tuple(params),
            ).fetchone()
            candidate_limit = min(max(bounded_limit * 4, bounded_limit), 200)
            rows = conn.execute(
                f"""
                SELECT *, {effective_quality_sql} AS retrieval_quality
                FROM traffic_case_memories
                WHERE {clause}
                ORDER BY
                    CASE
                        WHEN ? IS NOT NULL AND intersection_id = ? THEN 3
                        WHEN ? IS NOT NULL AND road_id = ? THEN 2
                        ELSE 1
                    END DESC,
                    CASE {effective_quality_sql}
                        WHEN '{CaseMemoryQuality.VERIFIED_SUCCESS.value}' THEN 4
                        WHEN '{CaseMemoryQuality.PARTIAL_SUCCESS.value}' THEN 3
                        WHEN '{CaseMemoryQuality.UNVERIFIED.value}' THEN 1
                        WHEN '{CaseMemoryQuality.INCOMPLETE.value}' THEN 1
                        WHEN '{CaseMemoryQuality.VALIDATED.value}' THEN 1
                        WHEN '{CaseMemoryQuality.PARTIAL.value}' THEN 1
                        WHEN '{CaseMemoryQuality.FAILED_OUTCOME.value}' THEN 0
                        ELSE -1
                    END DESC,
                    julianday(completed_at) DESC,
                    case_id
                LIMIT ?
                """,
                tuple(order_params[:1] + params + order_params[1:] + [candidate_limit]),
            ).fetchall()
            selected_rows = list(rows[:bounded_limit])
            if bounded_limit >= 2 and not any(
                str(row["retrieval_quality"]) == CaseMemoryQuality.FAILED_OUTCOME.value
                for row in selected_rows
            ):
                negative = next(
                    (
                        row for row in rows
                        if str(row["retrieval_quality"])
                        == CaseMemoryQuality.FAILED_OUTCOME.value
                    ),
                    None,
                )
                if negative is None:
                    negative = conn.execute(
                        f"""
                        SELECT *, '{CaseMemoryQuality.FAILED_OUTCOME.value}' AS retrieval_quality
                        FROM traffic_case_memories
                        WHERE {clause}
                          AND quality_status = ?
                          AND (
                              feedback_updated_at IS NULL
                              OR julianday(feedback_updated_at) < julianday(?)
                          )
                        ORDER BY
                            CASE
                                WHEN ? IS NOT NULL AND intersection_id = ? THEN 3
                                WHEN ? IS NOT NULL AND road_id = ? THEN 2
                                ELSE 1
                            END DESC,
                            julianday(completed_at) DESC,
                            case_id
                        LIMIT 1
                        """,
                        tuple(params + [
                            CaseMemoryQuality.FAILED_OUTCOME.value,
                            as_of,
                            intersection_id,
                            intersection_id,
                            road_id,
                            road_id,
                        ]),
                    ).fetchone()
                if negative is not None:
                    selected_rows[-1] = negative
            metadata = {
                str(row["case_id"]): {
                    "effectiveQuality": str(row["retrieval_quality"]),
                    "feedbackVisible": not row["feedback_updated_at"]
                    or _strictly_before(row["feedback_updated_at"], as_of),
                }
                for row in selected_rows
            }
            return {
                "cases": [self._row_to_case(row) for row in selected_rows],
                "total": total_row["total"] if total_row else 0,
                "limit": bounded_limit,
                "retrievalMetadata": metadata,
            }
        finally:
            conn.close()

    def _case_values(self, case: TrafficCaseMemory) -> Sequence[Any]:
        return (
            case.case_id,
            case.region_id,
            case.event_id,
            case.event_type,
            case.road_id,
            case.intersection_id,
            case.source_session_id,
            case.source_collaboration_run_id,
            case.source_plan_id,
            case.source_workflow_run_id,
            case.final_status,
            case.quality_status.value,
            _json_dump(case.event_snapshot),
            _json_dump(case.agent_facts),
            _json_dump(case.plan_facts),
            _json_dump(case.human_decisions),
            _json_dump(case.workflow_outcome),
            _json_dump(case.lessons),
            _json_dump(case.recommendation_feedback),
            _json_dump(case.action_feedback),
            _json_dump(case.event_outcome),
            case.feedback_lifecycle.value,
            case.feedback_updated_at,
            case.feedback_revision,
            case.projection_revision,
            case.system_updated_at,
            1 if case.is_canonical else 0,
            case.superseded_by_case_id,
            case.generated_summary,
            case.started_at,
            case.completed_at,
            case.source_type,
            case.source_reference,
            _json_dump(case.provenance),
            case.created_at,
            case.updated_at,
        )

    def _row_to_case(self, row: sqlite3.Row) -> TrafficCaseMemory:
        return TrafficCaseMemory(
            case_id=row["case_id"],
            region_id=row["region_id"],
            event_id=row["event_id"],
            event_type=row["event_type"],
            road_id=row["road_id"],
            intersection_id=row["intersection_id"],
            source_session_id=row["source_session_id"],
            source_collaboration_run_id=row["source_collaboration_run_id"],
            source_plan_id=row["source_plan_id"],
            source_workflow_run_id=row["source_workflow_run_id"],
            final_status=row["final_status"],
            quality_status=row["quality_status"],
            event_snapshot=_json_load(row["event_snapshot_json"], {}),
            agent_facts=_json_load(row["agent_facts_json"], {}),
            plan_facts=_json_load(row["plan_facts_json"], {}),
            human_decisions=_json_load(row["human_decisions_json"], []),
            workflow_outcome=_json_load(row["workflow_outcome_json"], {}),
            recommendation_feedback=_json_load(
                row["recommendation_feedback_json"], {}
            ),
            action_feedback=_json_load(row["action_feedback_json"], []),
            event_outcome=_json_load(row["event_outcome_json"], {}),
            feedback_lifecycle=row["feedback_lifecycle"] or FeedbackLifecycle.PENDING.value,
            feedback_updated_at=row["feedback_updated_at"],
            feedback_revision=row["feedback_revision"],
            projection_revision=row["projection_revision"],
            system_updated_at=row["system_updated_at"],
            is_canonical=bool(row["is_canonical"]),
            superseded_by_case_id=row["superseded_by_case_id"],
            lessons=_json_load(row["lessons_json"], []),
            generated_summary=row["generated_summary"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            source_type=row["source_type"],
            source_reference=row["source_reference"],
            provenance=_json_load(row["provenance_json"], {}),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_feedback(self, row: sqlite3.Row) -> TrafficEventFeedback:
        return TrafficEventFeedback(
            feedback_id=row["feedback_id"],
            event_id=row["event_id"],
            workflow_run_id=row["workflow_run_id"],
            agent_run_id=row["agent_run_id"],
            plan_id=row["plan_id"],
            plan_version=row["plan_version"],
            approval_id=row["approval_id"],
            action_execution_id=row["action_execution_id"],
            action_assessments=_json_load(row["action_assessments_json"], []),
            event_outcome=EventOutcome(row["event_outcome"] or EventOutcome.UNKNOWN.value),
            effectiveness=FeedbackEffectiveness(
                row["effectiveness"] or FeedbackEffectiveness.UNKNOWN.value
            ),
            reason_code=FeedbackReasonCode(
                row["reason_code"] or FeedbackReasonCode.NONE.value
            ),
            comment=row["comment"] or "",
            reviewer=row["reviewer"] or "",
            lifecycle=FeedbackLifecycle(
                row["lifecycle"] or FeedbackLifecycle.PENDING.value
            ),
            revision=int(row["revision"] or 1),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def feedback_metrics(self) -> Dict[str, Any]:
        """Return a small bounded aggregate for the operations summary."""

        _ensure_case_memory_tables()
        conn = _get_conn()
        try:
            feedback = conn.execute(
                """SELECT COUNT(*) AS total,
                          SUM(CASE WHEN lifecycle='COMPLETE' THEN 1 ELSE 0 END) AS complete,
                          SUM(CASE WHEN lifecycle='PARTIAL' THEN 1 ELSE 0 END) AS partial,
                          SUM(CASE WHEN lifecycle='PENDING' THEN 1 ELSE 0 END) AS pending,
                          SUM(CASE WHEN effectiveness='EFFECTIVE' THEN 1 ELSE 0 END) AS effective,
                          SUM(CASE WHEN effectiveness!='UNKNOWN'
                                        AND event_outcome!='UNKNOWN'
                                   THEN 1 ELSE 0 END) AS assessed,
                          SUM(CASE WHEN effectiveness='EFFECTIVE' AND event_outcome='RESOLVED'
                              THEN 1 ELSE 0 END) AS effective_resolution
                   FROM traffic_event_feedback"""
            ).fetchone()
            qualities = conn.execute(
                """SELECT quality_status, COUNT(*) AS c
                   FROM traffic_case_memories WHERE is_canonical=1
                   GROUP BY quality_status"""
            ).fetchall()
            recommendations = conn.execute(
                """SELECT json_extract(recommendation_feedback_json, '$.status') AS status,
                          COUNT(*) AS c
                   FROM traffic_case_memories WHERE is_canonical=1
                   GROUP BY json_extract(recommendation_feedback_json, '$.status')"""
            ).fetchall()
            action_rows = conn.execute(
                """SELECT action_feedback_json FROM traffic_case_memories
                   WHERE is_canonical=1"""
            ).fetchall()
        finally:
            conn.close()
        quality_counts = {
            str(row["quality_status"]): int(row["c"] or 0) for row in qualities
        }
        recommendation_counts = {
            str(row["status"] or "unknown"): int(row["c"] or 0)
            for row in recommendations
        }
        recommendation_total = sum(
            recommendation_counts.get(key, 0)
            for key in ("accepted", "modified", "rejected")
        )
        assessed = int(feedback["assessed"] or 0)
        action_total = 0
        action_succeeded = 0
        action_failed = 0
        action_business_assessed = 0
        action_business_effective = 0
        for row in action_rows:
            for action in _json_load(row["action_feedback_json"], []):
                if not isinstance(action, dict) or not action.get("executed"):
                    continue
                action_total += 1
                action_succeeded += int(bool(action.get("succeeded")))
                action_failed += int(bool(action.get("failed")))
                business_effectiveness = str(
                    action.get("businessEffectiveness") or "UNKNOWN"
                )
                if business_effectiveness != FeedbackEffectiveness.UNKNOWN.value:
                    action_business_assessed += 1
                    action_business_effective += int(
                        business_effectiveness == FeedbackEffectiveness.EFFECTIVE.value
                    )
        return {
            "total": int(feedback["total"] or 0),
            "lifecycle": {
                "complete": int(feedback["complete"] or 0),
                "partial": int(feedback["partial"] or 0),
                "pending": int(feedback["pending"] or 0),
            },
            "recommendations": {
                "accepted": recommendation_counts.get("accepted", 0),
                "modified": recommendation_counts.get("modified", 0),
                "rejected": recommendation_counts.get("rejected", 0),
                "acceptanceRate": (
                    round(recommendation_counts.get("accepted", 0) / recommendation_total, 4)
                    if recommendation_total else None
                ),
                "modificationRate": (
                    round(recommendation_counts.get("modified", 0) / recommendation_total, 4)
                    if recommendation_total else None
                ),
                "rejectionRate": (
                    round(recommendation_counts.get("rejected", 0) / recommendation_total, 4)
                    if recommendation_total else None
                ),
            },
            "effectiveResolutionRate": (
                round(int(feedback["effective_resolution"] or 0) / assessed, 4)
                if assessed else None
            ),
            "actions": {
                "total": action_total,
                "succeeded": action_succeeded,
                "failed": action_failed,
                "businessAssessed": action_business_assessed,
                "businessEffective": action_business_effective,
                "successRate": (
                    round(action_succeeded / action_total, 4)
                    if action_total else None
                ),
                "businessEffectivenessRate": (
                    round(action_business_effective / action_business_assessed, 4)
                    if action_business_assessed else None
                ),
            },
            "cases": {
                "verified": quality_counts.get(CaseMemoryQuality.VERIFIED_SUCCESS.value, 0),
                "partial": quality_counts.get(CaseMemoryQuality.PARTIAL_SUCCESS.value, 0),
                "failed": quality_counts.get(CaseMemoryQuality.FAILED_OUTCOME.value, 0),
                "incomplete": quality_counts.get(CaseMemoryQuality.INCOMPLETE.value, 0),
                "unverified": quality_counts.get(CaseMemoryQuality.UNVERIFIED.value, 0),
            },
        }
