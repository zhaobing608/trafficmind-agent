"""
Workflow Repository — Phase 12

SQLite 持久化实现。表结构与 PostgreSQL 迁移解耦：
  - workflow_definitions
  - workflow_definition_versions
  - workflow_runs
  - workflow_node_runs
  - workflow_events
  - workflow_approvals
  - workflow_action_records

所有 JSON 字段使用 ensure_ascii=False 序列化。
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import backend.config as _config

from backend.workflow.models import (
    ActionStatus,
    ApprovalDecision,
    ApprovalReasonCode,
    DefinitionStatus,
    NodeConfig,
    NodeStatus,
    NodeType,
    WorkflowActionRecord,
    WorkflowActionAttempt,
    WorkflowApproval,
    WorkflowDefinition,
    WorkflowDefinitionVersion,
    WorkflowEvent,
    WorkflowNodeRun,
    WorkflowRun,
    WorkflowRunStatus,
    compute_action_idempotency_key,
    compute_legacy_action_idempotency_key,
    generate_event_id,
)
from backend.workflow.definition import WorkflowRepository as AbstractWorkflowRepository


# ═══════════════════════════════════════════════════════════════════════════════
# 数据库连接
# ═══════════════════════════════════════════════════════════════════════════════


def _get_conn() -> sqlite3.Connection:
    """获取 SQLite 连接。"""
    os.makedirs(os.path.dirname(_config.DB_PATH), exist_ok=True)
    conn = sqlite3.connect(_config.DB_PATH)
    conn.row_factory = sqlite3.Row
    # Schema upgrades can start concurrently in multiple API/worker
    # processes.  Wait for the current migrator instead of failing on the
    # transient write lock.
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _correlation_payload_tx(
    conn: sqlite3.Connection,
    run_id: str,
    payload: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Attach durable correlation identities to a workflow audit payload.

    Historical callers only supplied node-specific data.  Correlation is
    resolved from the same transaction's run/action rows, never from log text
    or a frontend guess.  Caller-supplied identities cannot override durable
    Run/Action ownership.
    """
    correlated = dict(payload or {})
    correlated["workflowRunId"] = run_id
    run_event_id = ""
    row = conn.execute(
        "SELECT state_json FROM workflow_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    if row is not None:
        try:
            state = json.loads(row["state_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            state = {}
        current = state.get("currentEvent") if isinstance(state, dict) else {}
        if isinstance(current, dict):
            run_event_id = str(current.get("eventId") or "").strip()

    action_id = str(
        correlated.get("actionExecutionId")
        or correlated.get("actionId")
        or ""
    ).strip()
    action_event_id = ""
    if action_id:
        action = conn.execute(
            "SELECT run_id, event_id FROM workflow_action_records WHERE action_id=?",
            (action_id,),
        ).fetchone()
        if action is not None and str(action["run_id"] or "") != run_id:
            # Never let an Action owned by another Run cross-link this audit.
            correlated.pop("actionExecutionId", None)
            correlated.pop("actionId", None)
        else:
            # A not-yet-created deterministic ID is legitimate for a blocked
            # Action; once a row exists, ownership above is mandatory.
            correlated["actionExecutionId"] = action_id
            if action is not None:
                action_event_id = str(action["event_id"] or "").strip()
                if (
                    run_event_id
                    and action_event_id
                    and action_event_id != run_event_id
                ):
                    # A corrupt/legacy Action row can belong to this Run while
                    # carrying another Event identity.  Do not let that row
                    # cross-link either Event's audit graph.
                    correlated.pop("actionExecutionId", None)
                    correlated.pop("actionId", None)
                    action_event_id = ""

    # The Run is the primary Event binding.  An owned Action may fill a legacy
    # missing binding, but an explicit payload can never introduce another
    # Event identity.
    correlated["eventId"] = run_event_id or action_event_id or None
    return correlated


def _append_event_tx(
    conn: sqlite3.Connection,
    run_id: str,
    event_type: str,
    *,
    node_id: str = "",
    payload: Optional[Dict[str, Any]] = None,
) -> WorkflowEvent:
    """Append an immutable audit event inside an existing transaction."""
    row = conn.execute(
        "SELECT MAX(sequence) AS seq FROM workflow_events WHERE run_id=?",
        (run_id,),
    ).fetchone()
    sequence = int(row["seq"]) + 1 if row and row["seq"] is not None else 0
    event = WorkflowEvent(
        event_id=generate_event_id(run_id, sequence),
        run_id=run_id,
        node_id=node_id,
        event_type=event_type,
        payload=_correlation_payload_tx(conn, run_id, payload),
        sequence=sequence,
    )
    conn.execute(
        "INSERT INTO workflow_events VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            event.event_id,
            event.run_id,
            event.node_id,
            event.event_type,
            json.dumps(event.payload, ensure_ascii=False),
            event.sequence,
            event.created_at,
        ),
    )
    return event


def _apply_action_finalization_tx(
    conn: sqlite3.Connection,
    finalization: Dict[str, Any],
) -> bool:
    """Close exactly one claimed action attempt inside a caller transaction."""
    action_id = str(finalization.get("actionExecutionId") or finalization.get("actionId") or "")
    attempt = int(finalization.get("attempt") or 0)
    status = str(finalization.get("status") or "")
    if not action_id or attempt <= 0 or status not in {
        ActionStatus.SUCCEEDED.value,
        ActionStatus.FAILED.value,
        ActionStatus.UNKNOWN.value,
        ActionStatus.CANCELLED.value,
        ActionStatus.BLOCKED.value,
    }:
        return False
    finished_at = str(finalization.get("finishedAt") or _utc_now_iso())
    result = finalization.get("result") if isinstance(finalization.get("result"), dict) else {}
    error = str(finalization.get("error") or "")[:500]
    external_reference = str(finalization.get("externalReference") or "")[:500]
    reconciliation_supported = 1 if finalization.get("reconciliationSupported") else 0
    retryable = 1 if finalization.get("retryable") else 0
    reconciliation_message = str(finalization.get("reconciliationMessage") or "")[:500]
    update = conn.execute(
        """UPDATE workflow_action_records SET
               status=?, result_json=?, error=?, completed_at=?, finished_at=?,
               external_reference=?, reconciliation_supported=?, retryable=?,
               reconciliation_message=?,
               unknown_since=CASE WHEN ?='unknown'
                   THEN COALESCE(NULLIF(unknown_since, ''), ?)
                   ELSE '' END
           WHERE action_id=? AND attempt=? AND status IN ('running','executing')""",
        (
            status,
            json.dumps(result, ensure_ascii=False),
            error,
            finished_at,
            finished_at,
            external_reference,
            reconciliation_supported,
            retryable,
            reconciliation_message,
            status,
            finished_at,
            action_id,
            attempt,
        ),
    )
    if update.rowcount != 1:
        return False
    conn.execute(
        """UPDATE workflow_action_attempts SET
               status=?, result_json=?, error=?, finished_at=?, external_reference=?
           WHERE action_id=? AND attempt=? AND status IN ('running','executing')""",
        (
            status,
            json.dumps(result, ensure_ascii=False),
            error,
            finished_at,
            external_reference,
            action_id,
            attempt,
        ),
    )
    return True


def _project_unknown_action_to_run_tx(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    reason: str,
    project_node: bool = True,
) -> str:
    """Project an UNKNOWN action into a safe run state in the same tx.

    Returns the durable run status after projection.  A cancelled run remains
    cancelled; query-only reconciliation is still possible, but execution is
    never resumed.  Active runs are paused even when no driver owns them.
    """
    action = conn.execute(
        "SELECT * FROM workflow_action_records WHERE action_id=?",
        (action_id,),
    ).fetchone()
    if action is None or action["status"] != ActionStatus.UNKNOWN.value:
        return ""
    run = conn.execute(
        "SELECT * FROM workflow_runs WHERE run_id=?",
        (action["run_id"],),
    ).fetchone()
    if run is None:
        return ""

    run_status = str(run["status"] or "")
    if run_status not in {
        WorkflowRunStatus.PENDING.value,
        WorkflowRunStatus.RUNNING.value,
        WorkflowRunStatus.PAUSED.value,
        WorkflowRunStatus.CANCELLED.value,
    }:
        return run_status

    state = json.loads(run["state_json"] or "{}")
    action_result = json.loads(action["result_json"] or "{}")
    state.setdefault("actionResults", {})[action["action_type"]] = {
        "actionExecutionId": action_id,
        "status": ActionStatus.UNKNOWN.value,
        "result": action_result,
        "error": action["error"] or reason,
        "externalReference": action["external_reference"] or None,
    }
    state["currentNode"] = action["node_id"]
    if run_status == WorkflowRunStatus.CANCELLED.value:
        state["status"] = WorkflowRunStatus.CANCELLED.value
    else:
        state["status"] = WorkflowRunStatus.PAUSED.value
        state["finishedAt"] = ""

    if project_node:
        conn.execute(
            """UPDATE workflow_node_runs SET status='paused', error=?
               WHERE node_run_id=(
                   SELECT node_run_id FROM workflow_node_runs
                   WHERE run_id=? AND node_id=? AND status='running'
                   ORDER BY attempt DESC LIMIT 1
               )""",
            (reason[:500], action["run_id"], action["node_id"]),
        )
    now = _utc_now_iso()
    if run_status == WorkflowRunStatus.CANCELLED.value:
        conn.execute(
            """UPDATE workflow_runs SET state_json=?, updated_at=?
               WHERE run_id=? AND status='cancelled'""",
            (json.dumps(state, ensure_ascii=False), now, action["run_id"]),
        )
        return WorkflowRunStatus.CANCELLED.value

    conn.execute(
        """UPDATE workflow_runs SET status='paused', current_node_id=?,
               state_json=?, updated_at=?, completed_at='',
               driver_owner=NULL, driver_lease_until=NULL,
               driver_heartbeat_at=NULL
           WHERE run_id=? AND status IN ('pending','running','paused')""",
        (
            action["node_id"],
            json.dumps(state, ensure_ascii=False),
            now,
            action["run_id"],
        ),
    )
    return WorkflowRunStatus.PAUSED.value


def _safe_parse_status(status_str: str) -> WorkflowRunStatus:
    """安全解析状态字符串，未知状态按 paused 处理。"""
    try:
        return WorkflowRunStatus(status_str)
    except ValueError:
        return WorkflowRunStatus.PAUSED


def _ensure_wait_columns():
    """非破坏性添加 wait 相关列（幂等）。"""
    conn = _get_conn()
    try:
        for col_def in [
            "wait_type TEXT DEFAULT ''",
            "wake_at TEXT DEFAULT NULL",
            "resumed_at TEXT DEFAULT NULL",
            "resume_reason TEXT DEFAULT ''",
        ]:
            col_name = col_def.split()[0]
            try:
                conn.execute(f"ALTER TABLE workflow_runs ADD COLUMN {col_def}")
            except sqlite3.OperationalError:
                pass
        conn.commit()
    finally:
        conn.close()


def _ensure_driver_columns():
    """非破坏性添加 RunDriver 相关列（幂等，Phase17 Round3）。"""
    conn = _get_conn()
    try:
        for col_def in [
            "driver_managed INTEGER DEFAULT 0",
            "driver_owner TEXT DEFAULT NULL",
            "driver_lease_until TEXT DEFAULT NULL",
            "driver_heartbeat_at TEXT DEFAULT NULL",
            "driver_generation INTEGER DEFAULT 0",
        ]:
            col_name = col_def.split()[0]
            try:
                conn.execute(f"ALTER TABLE workflow_runs ADD COLUMN {col_def}")
            except sqlite3.OperationalError:
                pass
        conn.commit()
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# 表初始化（幂等）
# ═══════════════════════════════════════════════════════════════════════════════


def init_workflow_tables() -> None:
    """初始化 Workflow 相关表（幂等 CREATE TABLE IF NOT EXISTS）。"""
    conn = _get_conn()
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS workflow_definitions (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT DEFAULT '',
            category TEXT DEFAULT '',
            status TEXT DEFAULT 'draft',
            nodes_json TEXT DEFAULT '[]',
            entry_node_id TEXT DEFAULT '',
            metadata_json TEXT DEFAULT '{}',
            created_at TEXT DEFAULT '',
            updated_at TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS workflow_definition_versions (
            id TEXT PRIMARY KEY,
            definition_id TEXT NOT NULL,
            version INTEGER NOT NULL,
            definition_json TEXT DEFAULT '{}',
            changelog TEXT DEFAULT '',
            created_at TEXT DEFAULT '',
            UNIQUE(definition_id, version)
        );

        CREATE TABLE IF NOT EXISTS workflow_runs (
            run_id TEXT PRIMARY KEY,
            definition_id TEXT DEFAULT '',
            version INTEGER DEFAULT 1,
            session_id TEXT DEFAULT '',
            event_thread_id TEXT DEFAULT '',
            status TEXT DEFAULT 'pending',
            current_node_id TEXT DEFAULT '',
            state_json TEXT DEFAULT '{}',
            started_at TEXT DEFAULT '',
            updated_at TEXT DEFAULT '',
            completed_at TEXT DEFAULT '',
            triggered_by TEXT DEFAULT 'system'
        );

        CREATE TABLE IF NOT EXISTS workflow_node_runs (
            node_run_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            node_id TEXT NOT NULL,
            node_type TEXT DEFAULT 'trigger',
            status TEXT DEFAULT 'pending',
            attempt INTEGER DEFAULT 0,
            max_attempts INTEGER DEFAULT 1,
            input_snapshot_json TEXT DEFAULT '{}',
            output_snapshot_json TEXT DEFAULT '{}',
            error TEXT DEFAULT '',
            started_at TEXT DEFAULT '',
            completed_at TEXT DEFAULT '',
            duration_ms INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS workflow_events (
            event_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            node_id TEXT DEFAULT '',
            event_type TEXT DEFAULT '',
            payload_json TEXT DEFAULT '{}',
            sequence INTEGER DEFAULT 0,
            created_at TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS workflow_approvals (
            approval_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            node_id TEXT DEFAULT '',
            proposed_actions_json TEXT DEFAULT '[]',
            edited_actions_json TEXT DEFAULT '[]',
            decision TEXT DEFAULT 'pending',
            reviewer TEXT DEFAULT '',
            comment TEXT DEFAULT '',
            reason_code TEXT DEFAULT 'NONE',
            created_at TEXT DEFAULT '',
            decided_at TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS workflow_action_records (
            action_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            node_id TEXT DEFAULT '',
            action_type TEXT DEFAULT '',
            idempotency_key TEXT NOT NULL UNIQUE,
            params_json TEXT DEFAULT '{}',
            result_json TEXT DEFAULT '{}',
            status TEXT DEFAULT 'pending',
            error TEXT DEFAULT '',
            created_at TEXT DEFAULT '',
            completed_at TEXT DEFAULT '',
            event_id TEXT DEFAULT '',
            semantic_action_version TEXT DEFAULT 'v1',
            attempt INTEGER DEFAULT 0,
            started_at TEXT DEFAULT '',
            finished_at TEXT DEFAULT '',
            request_metadata_json TEXT DEFAULT '{}',
            external_reference TEXT DEFAULT '',
            last_reconciled_at TEXT DEFAULT '',
            unknown_since TEXT DEFAULT '',
            reconciliation_attempts INTEGER DEFAULT 0,
            reconciliation_supported INTEGER DEFAULT 0,
            retryable INTEGER DEFAULT 0,
            reconciliation_message TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS workflow_action_attempts (
            attempt_id TEXT PRIMARY KEY,
            action_id TEXT NOT NULL,
            attempt INTEGER NOT NULL,
            status TEXT DEFAULT 'pending',
            started_at TEXT DEFAULT '',
            finished_at TEXT DEFAULT '',
            request_metadata_json TEXT DEFAULT '{}',
            external_reference TEXT DEFAULT '',
            result_json TEXT DEFAULT '{}',
            error TEXT DEFAULT '',
            last_reconciled_at TEXT DEFAULT '',
            UNIQUE(action_id, attempt)
        );

        CREATE TABLE IF NOT EXISTS workflow_dispatch_tasks (
            dispatch_task_id TEXT PRIMARY KEY,
            event_id TEXT NOT NULL,
            workflow_run_id TEXT NOT NULL,
            action_execution_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL UNIQUE,
            assignee TEXT DEFAULT '',
            target TEXT DEFAULT '',
            instruction TEXT DEFAULT '',
            status TEXT DEFAULT 'created',
            created_at TEXT DEFAULT '',
            updated_at TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS workflow_notification_receipts (
            receipt_id TEXT PRIMARY KEY,
            idempotency_key TEXT NOT NULL UNIQUE,
            channel TEXT DEFAULT 'local',
            target TEXT DEFAULT '',
            message_digest TEXT DEFAULT '',
            delivered_at TEXT DEFAULT ''
        );

        CREATE INDEX IF NOT EXISTS idx_wf_runs_session ON workflow_runs(session_id);
        CREATE INDEX IF NOT EXISTS idx_wf_runs_status ON workflow_runs(status);
        CREATE INDEX IF NOT EXISTS idx_wf_node_runs_run ON workflow_node_runs(run_id);
        CREATE INDEX IF NOT EXISTS idx_wf_events_run ON workflow_events(run_id);
        CREATE INDEX IF NOT EXISTS idx_wf_events_run_sequence ON workflow_events(run_id, sequence);
        CREATE INDEX IF NOT EXISTS idx_wf_approvals_run ON workflow_approvals(run_id);
        CREATE INDEX IF NOT EXISTS idx_wf_approvals_pending_created ON workflow_approvals(decision, created_at);
        CREATE INDEX IF NOT EXISTS idx_wf_actions_run ON workflow_action_records(run_id);
        CREATE INDEX IF NOT EXISTS idx_wf_actions_idem ON workflow_action_records(idempotency_key);
        CREATE INDEX IF NOT EXISTS idx_wf_action_attempts_action ON workflow_action_attempts(action_id, attempt);
        CREATE INDEX IF NOT EXISTS idx_wf_dispatch_event ON workflow_dispatch_tasks(event_id);
        CREATE INDEX IF NOT EXISTS idx_wf_versions_def ON workflow_definition_versions(definition_id, version);
    """)
    # Serialize the inspect/ALTER sequence across processes.  A process-local
    # lock is insufficient here: without an immediate transaction, two fresh
    # workers can both observe a missing column and then race the same ALTER.
    c.execute("BEGIN IMMEDIATE")
    # Non-destructive migration for databases created before Phase 21.3.
    existing_action_columns = {
        row[1] for row in c.execute("PRAGMA table_info(workflow_action_records)").fetchall()
    }
    action_column_defs = {
        "event_id": "TEXT DEFAULT ''",
        "semantic_action_version": "TEXT DEFAULT 'v1'",
        "attempt": "INTEGER DEFAULT 0",
        "started_at": "TEXT DEFAULT ''",
        "finished_at": "TEXT DEFAULT ''",
        "request_metadata_json": "TEXT DEFAULT '{}'",
        "external_reference": "TEXT DEFAULT ''",
        "last_reconciled_at": "TEXT DEFAULT ''",
        "unknown_since": "TEXT DEFAULT ''",
        "reconciliation_attempts": "INTEGER DEFAULT 0",
        "reconciliation_supported": "INTEGER DEFAULT 0",
        "retryable": "INTEGER DEFAULT 0",
        "reconciliation_message": "TEXT DEFAULT ''",
    }
    for column_name, column_def in action_column_defs.items():
        if column_name not in existing_action_columns:
            c.execute(
                f"ALTER TABLE workflow_action_records ADD COLUMN {column_name} {column_def}"
            )
    existing_approval_columns = {
        row[1] for row in c.execute("PRAGMA table_info(workflow_approvals)").fetchall()
    }
    if "reason_code" not in existing_approval_columns:
        c.execute(
            "ALTER TABLE workflow_approvals ADD COLUMN reason_code TEXT DEFAULT 'NONE'"
        )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_wf_actions_event "
        "ON workflow_action_records(event_id)"
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_wf_actions_status "
        "ON workflow_action_records(status, unknown_since)"
    )
    # These expression indexes directly support the exact Event trace lookups
    # already used by list_runs/list_plans.  The partial predicate protects
    # legacy malformed JSON rows.
    c.execute(
        """CREATE INDEX IF NOT EXISTS idx_wf_runs_event_id
           ON workflow_runs(json_extract(state_json, '$.currentEvent.eventId'))
           WHERE json_valid(state_json)"""
    )
    c.execute(
        """CREATE INDEX IF NOT EXISTS idx_wf_definitions_plan_event
           ON workflow_definitions(json_extract(metadata_json, '$.plan.eventId'))
           WHERE json_valid(metadata_json)"""
    )
    # Rows written before 21.3 did not carry event_id.  Backfill only from the
    # same run's persisted canonical event; never infer across runs/events.
    c.execute(
        """UPDATE workflow_action_records
           SET event_id = COALESCE((
               SELECT json_extract(workflow_runs.state_json, '$.currentEvent.eventId')
               FROM workflow_runs
               WHERE workflow_runs.run_id = workflow_action_records.run_id
                 AND json_valid(workflow_runs.state_json)
           ), '')
           WHERE COALESCE(event_id, '') = ''"""
    )
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# SQLiteWorkflowRepository
# ═══════════════════════════════════════════════════════════════════════════════


class SQLiteWorkflowRepository(AbstractWorkflowRepository):
    """Workflow SQLite 持久化实现。

    表结构与 PostgreSQL 迁移解耦：
      - 所有 JSON 字段用单独列（_json 后缀）
      - 不使用 ORM，直接 SQL
      - JSON 序列化统一使用 ensure_ascii=False
    """

    # ── Definition CRUD ──────────────────────────────────────────────────

    def save_definition(self, definition: WorkflowDefinition) -> None:
        init_workflow_tables()
        conn = _get_conn()
        conn.execute(
            """INSERT OR REPLACE INTO workflow_definitions VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )""",
            (
                definition.id,
                definition.name,
                definition.description,
                definition.category,
                definition.status.value,
                json.dumps([n.to_dict() for n in definition.nodes], ensure_ascii=False),
                definition.entry_node_id,
                json.dumps(definition.metadata, ensure_ascii=False),
                definition.created_at,
                definition.updated_at,
            ),
        )
        conn.commit()
        conn.close()

    def get_definition(self, definition_id: str) -> Optional[WorkflowDefinition]:
        init_workflow_tables()
        conn = _get_conn()
        row = conn.execute(
            "SELECT * FROM workflow_definitions WHERE id=?", (definition_id,)
        ).fetchone()
        conn.close()
        if row is None:
            return None
        return self._row_to_definition(dict(row))

    def find_definition_by_template_identity(
        self,
        name: str,
        category: str = "",
    ) -> Optional[WorkflowDefinition]:
        init_workflow_tables()
        conn = _get_conn()
        row = conn.execute(
            """SELECT * FROM workflow_definitions
               WHERE name=? AND category=? AND status IN (?, ?)
               ORDER BY updated_at DESC, id DESC LIMIT 1""",
            (name, category or "", DefinitionStatus.ACTIVE.value, DefinitionStatus.DRAFT.value),
        ).fetchone()
        conn.close()
        if row is None:
            return None
        return self._row_to_definition(dict(row))

    def list_definitions(
        self, status: Optional[str] = None
    ) -> List[WorkflowDefinition]:
        init_workflow_tables()
        conn = _get_conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM workflow_definitions WHERE status=? ORDER BY updated_at DESC",
                (status,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM workflow_definitions ORDER BY updated_at DESC"
            ).fetchall()
        conn.close()
        return [self._row_to_definition(dict(r)) for r in rows]

    def _row_to_definition(self, d: Dict[str, Any]) -> WorkflowDefinition:
        nodes_raw = d.get("nodes_json", "[]")
        if isinstance(nodes_raw, str):
            nodes_list = json.loads(nodes_raw) if nodes_raw else []
        else:
            nodes_list = nodes_raw
        metadata_raw = d.get("metadata_json", "{}")
        if isinstance(metadata_raw, str):
            metadata = json.loads(metadata_raw) if metadata_raw else {}
        else:
            metadata = metadata_raw
        return WorkflowDefinition(
            id=d["id"],
            name=d["name"],
            description=d.get("description", ""),
            category=d.get("category", ""),
            status=DefinitionStatus(d.get("status", "draft")),
            nodes=[NodeConfig.from_dict(n) for n in (nodes_list or [])],
            entry_node_id=d.get("entry_node_id", ""),
            metadata=metadata or {},
            created_at=d.get("created_at", ""),
            updated_at=d.get("updated_at", ""),
        )

    # ── Version CRUD ─────────────────────────────────────────────────────

    def save_definition_version(self, version: WorkflowDefinitionVersion) -> None:
        init_workflow_tables()
        conn = _get_conn()
        conn.execute(
            """INSERT OR REPLACE INTO workflow_definition_versions VALUES (?, ?, ?, ?, ?, ?)""",
            (
                version.id,
                version.definition_id,
                version.version,
                json.dumps(version.definition_json, ensure_ascii=False),
                version.changelog,
                version.created_at,
            ),
        )
        conn.commit()
        conn.close()

    def get_definition_version(
        self, definition_id: str, version: int
    ) -> Optional[WorkflowDefinitionVersion]:
        init_workflow_tables()
        conn = _get_conn()
        row = conn.execute(
            "SELECT * FROM workflow_definition_versions WHERE definition_id=? AND version=?",
            (definition_id, version),
        ).fetchone()
        conn.close()
        if row is None:
            return None
        d = dict(row)
        def_json = d.get("definition_json", "{}")
        if isinstance(def_json, str):
            def_json = json.loads(def_json)
        return WorkflowDefinitionVersion(
            id=d["id"],
            definition_id=d["definition_id"],
            version=d["version"],
            definition_json=def_json,
            changelog=d.get("changelog", ""),
            created_at=d.get("created_at", ""),
        )

    def get_latest_version_number(self, definition_id: str) -> int:
        init_workflow_tables()
        conn = _get_conn()
        row = conn.execute(
            "SELECT MAX(version) as mv FROM workflow_definition_versions WHERE definition_id=?",
            (definition_id,),
        ).fetchone()
        conn.close()
        return row["mv"] if row and row["mv"] is not None else 0

    def list_definition_versions(
        self, definition_id: str
    ) -> List[WorkflowDefinitionVersion]:
        init_workflow_tables()
        conn = _get_conn()
        rows = conn.execute(
            "SELECT * FROM workflow_definition_versions WHERE definition_id=? ORDER BY version DESC",
            (definition_id,),
        ).fetchall()
        conn.close()
        results = []
        for row in rows:
            d = dict(row)
            def_json = d.get("definition_json", "{}")
            if isinstance(def_json, str):
                def_json = json.loads(def_json)
            results.append(WorkflowDefinitionVersion(
                id=d["id"],
                definition_id=d["definition_id"],
                version=d["version"],
                definition_json=def_json,
                changelog=d.get("changelog", ""),
                created_at=d.get("created_at", ""),
            ))
        return results

    # ── Run CRUD ─────────────────────────────────────────────────────────

    def save_run(self, run: WorkflowRun) -> None:
        init_workflow_tables()
        _ensure_wait_columns()
        _ensure_driver_columns()
        conn = _get_conn()
        # upsert：显式 16 业务列，保留 driver_* 列（不 wipe lease/fencing）
        conn.execute(
            """INSERT INTO workflow_runs (run_id, definition_id, version, session_id, event_thread_id,
                   status, current_node_id, state_json, started_at, updated_at, completed_at, triggered_by,
                   wait_type, wake_at, resumed_at, resume_reason)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(run_id) DO UPDATE SET
                   definition_id=excluded.definition_id, version=excluded.version,
                   session_id=excluded.session_id, event_thread_id=excluded.event_thread_id,
                   status=excluded.status, current_node_id=excluded.current_node_id,
                   state_json=excluded.state_json, started_at=excluded.started_at,
                   updated_at=excluded.updated_at, completed_at=excluded.completed_at,
                   triggered_by=excluded.triggered_by
               WHERE workflow_runs.status != 'cancelled'
                  OR excluded.status = 'cancelled'""",
            (
                run.run_id,
                run.definition_id,
                run.version,
                run.session_id,
                run.event_thread_id,
                run.status.value,
                run.current_node_id,
                json.dumps(run.state, ensure_ascii=False),
                run.started_at,
                run.updated_at,
                run.completed_at,
                run.triggered_by,
                "",     # wait_type
                None,   # wake_at
                None,   # resumed_at
                "",     # resume_reason
            ),
        )
        conn.commit()
        conn.close()

    def get_run(self, run_id: str) -> Optional[WorkflowRun]:
        init_workflow_tables()
        conn = _get_conn()
        row = conn.execute(
            "SELECT * FROM workflow_runs WHERE run_id=?", (run_id,)
        ).fetchone()
        conn.close()
        if row is None:
            return None
        return self._row_to_run(dict(row))

    def list_runs(
        self,
        session_id: str = "",
        definition_id: str = "",
        status: Optional[str] = None,
        event_id: str = "",
        limit: int = 50,
        offset: int = 0,
    ) -> List[WorkflowRun]:
        init_workflow_tables()
        conn = _get_conn()
        query = "SELECT * FROM workflow_runs WHERE 1=1"
        params: List[Any] = []
        if session_id:
            query += " AND session_id=?"
            params.append(session_id)
        if definition_id:
            query += " AND definition_id=?"
            params.append(definition_id)
        if status:
            query += " AND status=?"
            params.append(status)
        if event_id:
            # Phase20 R2：按事件 ID 精确匹配（只读，无 schema 变更）。
            # 绑定源是 state_json 内 $.currentEvent.eventId（仅启动方提供时存在）。
            query += " AND json_valid(state_json) AND json_extract(state_json, '$.currentEvent.eventId')=?"
            params.append(event_id)
        query += " ORDER BY updated_at DESC, run_id DESC LIMIT ? OFFSET ?"
        params.append(limit)
        params.append(offset)
        rows = conn.execute(query, params).fetchall()
        conn.close()
        return [self._row_to_run(dict(r)) for r in rows]

    def count_runs(
        self,
        session_id: str = "",
        definition_id: str = "",
        status: Optional[str] = None,
        event_id: str = "",
    ) -> int:
        """统计符合条件的 Run 总数（用于分页）。"""
        init_workflow_tables()
        conn = _get_conn()
        query = "SELECT COUNT(*) as cnt FROM workflow_runs WHERE 1=1"
        params: List[Any] = []
        if session_id:
            query += " AND session_id=?"
            params.append(session_id)
        if definition_id:
            query += " AND definition_id=?"
            params.append(definition_id)
        if status:
            query += " AND status=?"
            params.append(status)
        if event_id:
            query += " AND json_valid(state_json) AND json_extract(state_json, '$.currentEvent.eventId')=?"
            params.append(event_id)
        row = conn.execute(query, params).fetchone()
        conn.close()
        return row["cnt"] if row else 0

    def _row_to_run(self, d: Dict[str, Any]) -> WorkflowRun:
        state_raw = d.get("state_json", "{}")
        if isinstance(state_raw, str):
            state = json.loads(state_raw) if state_raw else {}
        else:
            state = state_raw
        return WorkflowRun(
            run_id=d["run_id"],
            definition_id=d.get("definition_id", ""),
            version=d.get("version", 1),
            session_id=d.get("session_id", ""),
            event_thread_id=d.get("event_thread_id", ""),
            status=_safe_parse_status(d.get("status", "pending")),
            current_node_id=d.get("current_node_id", ""),
            state=state or {},
            started_at=d.get("started_at", ""),
            updated_at=d.get("updated_at", ""),
            completed_at=d.get("completed_at", ""),
            triggered_by=d.get("triggered_by", "system"),
        )

    # ── NodeRun CRUD ─────────────────────────────────────────────────────

    def save_node_run(self, node_run: WorkflowNodeRun) -> None:
        init_workflow_tables()
        conn = _get_conn()
        conn.execute(
            """INSERT OR REPLACE INTO workflow_node_runs VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )""",
            (
                node_run.node_run_id,
                node_run.run_id,
                node_run.node_id,
                node_run.node_type.value,
                node_run.status.value,
                node_run.attempt,
                node_run.max_attempts,
                json.dumps(node_run.input_snapshot, ensure_ascii=False),
                json.dumps(node_run.output_snapshot, ensure_ascii=False),
                node_run.error,
                node_run.started_at,
                node_run.completed_at,
                node_run.duration_ms,
            ),
        )
        conn.commit()
        conn.close()

    def finalize_node_run(
        self,
        node_run: WorkflowNodeRun,
        *,
        driver_owner: str = "",
        driver_generation: int = 0,
        checkpoint_run: Optional[WorkflowRun] = None,
        action_finalization: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Persist a terminal node attempt only while its run may still advance.

        A RUNNING attempt is written before execution.  This conditional update
        closes that same attempt only if cancellation has not won the race.  For
        driver-managed runs the current owner/generation and lease are checked in
        the same SQLite statement, so a stale worker cannot write a late terminal
        result or advance control state.
        """
        init_workflow_tables()
        _ensure_driver_columns()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            run_predicate = "r.run_id=? AND r.status != 'cancelled'"
            predicate_params: List[Any] = [node_run.run_id]
            if driver_owner:
                run_predicate += (
                    " AND r.driver_owner=? AND r.driver_generation=?"
                    " AND r.driver_lease_until IS NOT NULL"
                    " AND r.driver_lease_until >= ?"
                )
                predicate_params.extend([
                    driver_owner,
                    int(driver_generation),
                    _utc_now_iso(),
                ])
            cursor = conn.execute(
                f"""UPDATE workflow_node_runs SET
                        status=?, attempt=?, max_attempts=?,
                        input_snapshot_json=?, output_snapshot_json=?, error=?,
                        started_at=?, completed_at=?, duration_ms=?
                    WHERE node_run_id=? AND status='running' AND EXISTS (
                        SELECT 1 FROM workflow_runs AS r WHERE {run_predicate}
                    )""",
                (
                    node_run.status.value,
                    node_run.attempt,
                    node_run.max_attempts,
                    json.dumps(node_run.input_snapshot, ensure_ascii=False),
                    json.dumps(node_run.output_snapshot, ensure_ascii=False),
                    node_run.error,
                    node_run.started_at,
                    node_run.completed_at,
                    node_run.duration_ms,
                    node_run.node_run_id,
                    *predicate_params,
                ),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                return False

            if action_finalization is not None:
                action_row = conn.execute(
                    """SELECT run_id, node_id, action_type, event_id
                       FROM workflow_action_records WHERE action_id=?""",
                    (str(action_finalization.get("actionExecutionId") or ""),),
                ).fetchone()
                if (
                    action_row is None
                    or action_row["run_id"] != node_run.run_id
                    or action_row["node_id"] != node_run.node_id
                    or not _apply_action_finalization_tx(conn, action_finalization)
                ):
                    conn.rollback()
                    return False
                action_status = str(action_finalization.get("status") or "")
                action_event_type = {
                    ActionStatus.SUCCEEDED.value: "action_succeeded",
                    ActionStatus.FAILED.value: "action_failed",
                    ActionStatus.UNKNOWN.value: "action_unknown",
                    ActionStatus.CANCELLED.value: "action_blocked",
                    ActionStatus.BLOCKED.value: "action_blocked",
                }.get(action_status, "action_blocked")
                _append_event_tx(
                    conn,
                    node_run.run_id,
                    action_event_type,
                    node_id=node_run.node_id,
                    payload={
                        "actionExecutionId": str(action_finalization.get("actionExecutionId") or ""),
                        "workflowRunId": node_run.run_id,
                        "eventId": action_row["event_id"] or None,
                        "actionType": action_row["action_type"],
                        "attempt": int(action_finalization.get("attempt") or 0),
                        "status": action_status,
                        "externalReference": str(action_finalization.get("externalReference") or "") or None,
                        "error": str(action_finalization.get("error") or "")[:500] or None,
                    },
                )

            if checkpoint_run is not None:
                checkpoint_predicate = "run_id=? AND status != 'cancelled'"
                checkpoint_params: List[Any] = [checkpoint_run.run_id]
                if driver_owner:
                    checkpoint_predicate += (
                        " AND driver_owner=? AND driver_generation=?"
                        " AND driver_lease_until IS NOT NULL"
                        " AND driver_lease_until >= ?"
                    )
                    checkpoint_params.extend([
                        driver_owner,
                        int(driver_generation),
                        _utc_now_iso(),
                    ])
                checkpoint = conn.execute(
                    f"""UPDATE workflow_runs SET
                            status=?, current_node_id=?, state_json=?,
                            started_at=?, updated_at=?, completed_at=?
                        WHERE {checkpoint_predicate}""",
                    (
                        checkpoint_run.status.value,
                        checkpoint_run.current_node_id,
                        json.dumps(checkpoint_run.state, ensure_ascii=False),
                        checkpoint_run.started_at,
                        checkpoint_run.updated_at,
                        checkpoint_run.completed_at,
                        *checkpoint_params,
                    ),
                )
                if checkpoint.rowcount != 1:
                    conn.rollback()
                    return False
                if checkpoint_run.status == WorkflowRunStatus.COMPLETED:
                    checkpoint_state = (
                        checkpoint_run.state
                        if isinstance(checkpoint_run.state, dict)
                        else {}
                    )
                    current_event = checkpoint_state.get("currentEvent") or {}
                    if (
                        isinstance(current_event, dict)
                        and current_event.get("actionExecutionAllowed", True) is not False
                    ):
                        event_id = str(
                            current_event.get("eventId")
                            or current_event.get("event_id")
                            or ""
                        ).strip()
                        table_exists = conn.execute(
                            """SELECT 1 FROM sqlite_master
                               WHERE type='table' AND name='event_records'"""
                        ).fetchone()
                        if event_id and table_exists:
                            previous = conn.execute(
                                "SELECT status FROM event_records WHERE eventId=?",
                                (event_id,),
                            ).fetchone()
                            completed_at = _utc_now_iso()
                            advanced = conn.execute(
                                """UPDATE event_records
                                   SET status='已处置', updatedAt=?
                                   WHERE eventId=?
                                     AND status IN ('待研判','待派单','处置中')""",
                                (completed_at, event_id),
                            )
                            lifecycle_exists = conn.execute(
                                """SELECT 1 FROM sqlite_master
                                   WHERE type='table'
                                     AND name='event_lifecycle_audit'"""
                            ).fetchone()
                            if (
                                advanced.rowcount == 1
                                and previous is not None
                                and lifecycle_exists
                            ):
                                conn.execute(
                                    """INSERT INTO event_lifecycle_audit (
                                           event_id, event_type,
                                           previous_status, status, actor,
                                           created_at
                                       ) VALUES (
                                           ?, 'event_status_updated', ?,
                                           '已处置', 'workflow_runtime', ?
                                       )""",
                                    (
                                        event_id,
                                        str(previous["status"] or ""),
                                        completed_at,
                                    ),
                                )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_node_runs(self, run_id: str) -> List[WorkflowNodeRun]:
        init_workflow_tables()
        conn = _get_conn()
        rows = conn.execute(
            "SELECT * FROM workflow_node_runs WHERE run_id=? ORDER BY started_at",
            (run_id,),
        ).fetchall()
        conn.close()
        return [self._row_to_node_run(dict(r)) for r in rows]

    def _row_to_node_run(self, d: Dict[str, Any]) -> WorkflowNodeRun:
        inp = d.get("input_snapshot_json", "{}")
        out = d.get("output_snapshot_json", "{}")
        if isinstance(inp, str):
            inp = json.loads(inp) if inp else {}
        if isinstance(out, str):
            out = json.loads(out) if out else {}
        return WorkflowNodeRun(
            node_run_id=d["node_run_id"],
            run_id=d["run_id"],
            node_id=d["node_id"],
            node_type=NodeType(d.get("node_type", "trigger")),
            status=NodeStatus(d.get("status", "pending")),
            attempt=d.get("attempt", 0),
            max_attempts=d.get("max_attempts", 1),
            input_snapshot=inp or {},
            output_snapshot=out or {},
            error=d.get("error", ""),
            started_at=d.get("started_at", ""),
            completed_at=d.get("completed_at", ""),
            duration_ms=d.get("duration_ms", 0),
        )

    # ── Event CRUD ───────────────────────────────────────────────────────

    def save_event(self, event: WorkflowEvent) -> None:
        """Insert an immutable audit event.

        Historical callers may provide their own event id/sequence.  A replay
        of the exact same event id is idempotent, but an existing audit record
        is never replaced or mutated.
        """
        init_workflow_tables()
        conn = _get_conn()
        try:
            conn.execute(
                """INSERT INTO workflow_events VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(event_id) DO NOTHING""",
                (
                    event.event_id,
                    event.run_id,
                    event.node_id,
                    event.event_type,
                    json.dumps(event.payload, ensure_ascii=False),
                    event.sequence,
                    event.created_at,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def append_event(
        self,
        run_id: str,
        event_type: str,
        *,
        node_id: str = "",
        payload: Optional[Dict[str, Any]] = None,
        event_id: str = "",
        created_at: str = "",
    ) -> WorkflowEvent:
        """Atomically allocate and append one per-run audit sequence.

        ``BEGIN IMMEDIATE`` serialises the MAX+1 allocation with the INSERT,
        preventing two workers from receiving the same sequence.  Runtime
        callers use this method instead of the legacy split
        ``next_event_sequence``/``save_event`` pair.
        """
        init_workflow_tables()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if event_id:
                existing = conn.execute(
                    "SELECT * FROM workflow_events WHERE event_id=?",
                    (event_id,),
                ).fetchone()
                if existing is not None:
                    conn.rollback()
                    raw_payload = existing["payload_json"] or "{}"
                    return WorkflowEvent(
                        event_id=existing["event_id"],
                        run_id=existing["run_id"],
                        node_id=existing["node_id"] or "",
                        event_type=existing["event_type"] or "",
                        payload=json.loads(raw_payload),
                        sequence=int(existing["sequence"] or 0),
                        created_at=existing["created_at"] or "",
                    )
            row = conn.execute(
                "SELECT MAX(sequence) AS seq FROM workflow_events WHERE run_id=?",
                (run_id,),
            ).fetchone()
            sequence = (
                int(row["seq"]) + 1
                if row is not None and row["seq"] is not None
                else 0
            )
            event = WorkflowEvent(
                event_id=event_id or generate_event_id(run_id, sequence),
                run_id=run_id,
                node_id=node_id,
                event_type=event_type,
                payload=_correlation_payload_tx(conn, run_id, payload),
                sequence=sequence,
                created_at=created_at,
            )
            conn.execute(
                "INSERT INTO workflow_events VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    event.event_id,
                    event.run_id,
                    event.node_id,
                    event.event_type,
                    json.dumps(event.payload, ensure_ascii=False),
                    event.sequence,
                    event.created_at,
                ),
            )
            conn.commit()
            return event
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def list_events(self, run_id: str) -> List[WorkflowEvent]:
        init_workflow_tables()
        conn = _get_conn()
        rows = conn.execute(
            "SELECT * FROM workflow_events WHERE run_id=? ORDER BY sequence, rowid",
            (run_id,),
        ).fetchall()
        conn.close()
        results = []
        for row in rows:
            d = dict(row)
            payload = d.get("payload_json", "{}")
            if isinstance(payload, str):
                payload = json.loads(payload) if payload else {}
            results.append(WorkflowEvent(
                event_id=d["event_id"],
                run_id=d["run_id"],
                node_id=d.get("node_id", ""),
                event_type=d.get("event_type", ""),
                payload=payload or {},
                sequence=d.get("sequence", 0),
                created_at=d.get("created_at", ""),
            ))
        return results

    def next_event_sequence(self, run_id: str) -> int:
        """Preview the next sequence (not an allocator; use append_event)."""
        init_workflow_tables()
        conn = _get_conn()
        try:
            row = conn.execute(
                "SELECT MAX(sequence) AS seq FROM workflow_events WHERE run_id=?",
                (run_id,),
            ).fetchone()
            return int(row["seq"] or 0) + 1 if row and row["seq"] is not None else 0
        finally:
            conn.close()

    # ── Approval CRUD ────────────────────────────────────────────────────

    def save_approval(self, approval: WorkflowApproval) -> None:
        init_workflow_tables()
        conn = _get_conn()
        conn.execute(
            """INSERT INTO workflow_approvals (
                   approval_id, run_id, node_id, proposed_actions_json,
                   edited_actions_json, decision, reviewer, comment,
                   reason_code, created_at, decided_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(approval_id) DO UPDATE SET
                   run_id=excluded.run_id,
                   node_id=excluded.node_id,
                   proposed_actions_json=excluded.proposed_actions_json,
                   edited_actions_json=excluded.edited_actions_json,
                   decision=excluded.decision,
                   reviewer=excluded.reviewer,
                   comment=excluded.comment,
                   reason_code=excluded.reason_code,
                   decided_at=excluded.decided_at""",
            (
                approval.approval_id,
                approval.run_id,
                approval.node_id,
                json.dumps(approval.proposed_actions, ensure_ascii=False),
                json.dumps(approval.edited_actions, ensure_ascii=False),
                approval.decision.value,
                approval.reviewer,
                approval.comment,
                approval.reason_code.value,
                approval.created_at,
                approval.decided_at,
            ),
        )
        conn.commit()
        conn.close()

    def decide_approval(self, approval: WorkflowApproval) -> bool:
        """Atomically apply one decision to a still-pending approval."""
        init_workflow_tables()
        conn = _get_conn()
        try:
            cursor = conn.execute(
                """UPDATE workflow_approvals SET
                       edited_actions_json=?, decision=?, reviewer=?, comment=?,
                       reason_code=?, decided_at=?
                   WHERE approval_id=? AND run_id=? AND decision='pending'""",
                (
                    json.dumps(approval.edited_actions, ensure_ascii=False),
                    approval.decision.value,
                    approval.reviewer,
                    approval.comment,
                    approval.reason_code.value,
                    approval.decided_at,
                    approval.approval_id,
                    approval.run_id,
                ),
            )
            conn.commit()
            return cursor.rowcount == 1
        finally:
            conn.close()

    def decide_approval_and_transition(
        self,
        approval: WorkflowApproval,
        run: WorkflowRun,
        *,
        expected_status: str = "awaiting_approval",
        ensure_driver_managed: bool = False,
        audit_events: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        """Commit approval, run checkpoint, and audit events in one transaction.

        Returns a stable result code: ``updated``, ``not_found``,
        ``invalid_status``, ``approval_mismatch``, or
        ``approval_not_pending``.  Serialising the validation and writes under
        ``BEGIN IMMEDIATE`` closes both cancellation and process-crash windows.
        """
        init_workflow_tables()
        _ensure_driver_columns()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status, state_json FROM workflow_runs WHERE run_id=?",
                (run.run_id,),
            ).fetchone()
            if row is None:
                conn.rollback()
                return "not_found"
            if row["status"] != expected_status:
                conn.rollback()
                return "invalid_status"
            persisted_state = json.loads(row["state_json"] or "{}")
            pending = (
                persisted_state.get("pendingApproval")
                or persisted_state.get("pending_approval")
                or {}
            )
            if not isinstance(pending, dict) or pending.get("approvalId") != approval.approval_id:
                conn.rollback()
                return "approval_mismatch"

            existing = conn.execute(
                "SELECT run_id, decision FROM workflow_approvals WHERE approval_id=?",
                (approval.approval_id,),
            ).fetchone()
            if existing is None:
                conn.execute(
                    """INSERT INTO workflow_approvals (
                           approval_id, run_id, node_id, proposed_actions_json,
                           edited_actions_json, decision, reviewer, comment,
                           reason_code, created_at, decided_at
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        approval.approval_id,
                        approval.run_id,
                        approval.node_id,
                        json.dumps(approval.proposed_actions, ensure_ascii=False),
                        "[]",
                        ApprovalDecision.PENDING.value,
                        "",
                        "",
                        ApprovalReasonCode.NONE.value,
                        approval.created_at,
                        "",
                    ),
                )
            elif (
                existing["run_id"] != approval.run_id
                or existing["decision"] != ApprovalDecision.PENDING.value
            ):
                conn.rollback()
                return "approval_not_pending"

            approval_update = conn.execute(
                """UPDATE workflow_approvals SET
                       edited_actions_json=?, decision=?, reviewer=?, comment=?,
                       reason_code=?, decided_at=?
                   WHERE approval_id=? AND run_id=? AND decision='pending'""",
                (
                    json.dumps(approval.edited_actions, ensure_ascii=False),
                    approval.decision.value,
                    approval.reviewer,
                    approval.comment,
                    approval.reason_code.value,
                    approval.decided_at,
                    approval.approval_id,
                    approval.run_id,
                ),
            )
            if approval_update.rowcount != 1:
                conn.rollback()
                return "approval_not_pending"

            run_update = conn.execute(
                """UPDATE workflow_runs SET
                       status=?, current_node_id=?, state_json=?, started_at=?,
                       updated_at=?, completed_at=?, driver_owner=NULL,
                       driver_lease_until=NULL,
                       driver_managed=CASE WHEN ?=1 THEN 1 ELSE driver_managed END
                   WHERE run_id=? AND status=?""",
                (
                    run.status.value,
                    run.current_node_id,
                    json.dumps(run.state, ensure_ascii=False),
                    run.started_at,
                    run.updated_at,
                    run.completed_at,
                    1 if ensure_driver_managed else 0,
                    run.run_id,
                    expected_status,
                ),
            )
            if run_update.rowcount != 1:
                conn.rollback()
                return "invalid_status"

            for event_data in audit_events or []:
                seq_row = conn.execute(
                    "SELECT MAX(sequence) AS seq FROM workflow_events WHERE run_id=?",
                    (run.run_id,),
                ).fetchone()
                sequence = (
                    int(seq_row["seq"]) + 1
                    if seq_row is not None and seq_row["seq"] is not None
                    else 0
                )
                event = WorkflowEvent(
                    event_id=generate_event_id(run.run_id, sequence),
                    run_id=run.run_id,
                    node_id=str(event_data.get("nodeId") or ""),
                    event_type=str(event_data.get("eventType") or ""),
                    payload=dict(event_data.get("payload") or {}),
                    sequence=sequence,
                )
                conn.execute(
                    "INSERT INTO workflow_events VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        event.event_id,
                        event.run_id,
                        event.node_id,
                        event.event_type,
                        json.dumps(event.payload, ensure_ascii=False),
                        event.sequence,
                        event.created_at,
                    ),
                )
            conn.commit()
            return "updated"
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_approval(self, approval_id: str) -> Optional[WorkflowApproval]:
        init_workflow_tables()
        conn = _get_conn()
        row = conn.execute(
            "SELECT * FROM workflow_approvals WHERE approval_id=?",
            (approval_id,),
        ).fetchone()
        conn.close()
        if row is None:
            return None
        return self._row_to_approval(dict(row))

    def list_approvals(self, run_id: str) -> List["WorkflowApproval"]:
        """列出 run 的全部审批记录。"""
        init_workflow_tables()
        conn = _get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM workflow_approvals WHERE run_id=? ORDER BY created_at",
                (run_id,),
            ).fetchall()
            conn.close()
            return [self._row_to_approval(dict(r)) for r in rows]
        except Exception:
            conn.close()
            return []

    def get_pending_approval(
        self, run_id: str, node_id: str
    ) -> Optional[WorkflowApproval]:
        init_workflow_tables()
        conn = _get_conn()
        row = conn.execute(
            "SELECT * FROM workflow_approvals WHERE run_id=? AND node_id=? AND decision='pending' ORDER BY created_at DESC LIMIT 1",
            (run_id, node_id),
        ).fetchone()
        conn.close()
        if row is None:
            return None
        return self._row_to_approval(dict(row))

    def _row_to_approval(self, d: Dict[str, Any]) -> WorkflowApproval:
        proposed = d.get("proposed_actions_json", "[]")
        edited = d.get("edited_actions_json", "[]")
        if isinstance(proposed, str):
            proposed = json.loads(proposed) if proposed else []
        if isinstance(edited, str):
            edited = json.loads(edited) if edited else []
        return WorkflowApproval(
            approval_id=d["approval_id"],
            run_id=d["run_id"],
            node_id=d.get("node_id", ""),
            proposed_actions=proposed or [],
            edited_actions=edited or [],
            decision=ApprovalDecision(d.get("decision", "pending")),
            reviewer=d.get("reviewer", ""),
            comment=d.get("comment", ""),
            reason_code=ApprovalReasonCode(d.get("reason_code", "NONE") or "NONE"),
            created_at=d.get("created_at", ""),
            decided_at=d.get("decided_at", ""),
        )

    # ── ActionRecord CRUD ────────────────────────────────────────────────

    def save_action_record(self, record: WorkflowActionRecord) -> None:
        init_workflow_tables()
        conn = _get_conn()
        conn.execute(
            """INSERT INTO workflow_action_records (
                   action_id, run_id, node_id, action_type, idempotency_key,
                   params_json, result_json, status, error, created_at, completed_at,
                   event_id, semantic_action_version, attempt, started_at, finished_at,
                   request_metadata_json, external_reference, last_reconciled_at,
                   unknown_since, reconciliation_attempts,
                   reconciliation_supported, retryable, reconciliation_message
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(idempotency_key) DO UPDATE SET
                   action_id=excluded.action_id,
                   run_id=excluded.run_id,
                   node_id=excluded.node_id,
                   action_type=excluded.action_type,
                   params_json=excluded.params_json,
                   result_json=excluded.result_json,
                   status=excluded.status,
                   error=excluded.error,
                   created_at=excluded.created_at,
                   completed_at=excluded.completed_at,
                   event_id=excluded.event_id,
                   semantic_action_version=excluded.semantic_action_version,
                   attempt=excluded.attempt,
                   started_at=excluded.started_at,
                   finished_at=excluded.finished_at,
                   request_metadata_json=excluded.request_metadata_json,
                   external_reference=excluded.external_reference,
                   last_reconciled_at=excluded.last_reconciled_at,
                   unknown_since=excluded.unknown_since,
                   reconciliation_attempts=excluded.reconciliation_attempts,
                   reconciliation_supported=excluded.reconciliation_supported,
                   retryable=excluded.retryable,
                   reconciliation_message=excluded.reconciliation_message
               WHERE workflow_action_records.action_id=excluded.action_id
                  OR workflow_action_records.status IN ('pending','failed')""",
            (
                record.action_id,
                record.run_id,
                record.node_id,
                record.action_type,
                record.idempotency_key,
                json.dumps(record.params, ensure_ascii=False),
                json.dumps(record.result, ensure_ascii=False),
                record.status.value,
                record.error,
                record.created_at,
                record.completed_at,
                record.event_id,
                record.semantic_action_version,
                int(record.attempt or 0),
                record.started_at,
                record.finished_at,
                json.dumps(record.request_metadata, ensure_ascii=False),
                record.external_reference,
                record.last_reconciled_at,
                record.unknown_since,
                int(record.reconciliation_attempts or 0),
                1 if record.reconciliation_supported else 0,
                1 if record.retryable else 0,
                record.reconciliation_message,
            ),
        )
        conn.commit()
        conn.close()

    def get_action_record(self, action_id: str) -> Optional[WorkflowActionRecord]:
        init_workflow_tables()
        conn = _get_conn()
        try:
            row = conn.execute(
                "SELECT * FROM workflow_action_records WHERE action_id=?",
                (action_id,),
            ).fetchone()
            return self._row_to_action_record(dict(row)) if row is not None else None
        finally:
            conn.close()

    def get_action_record_by_idempotency_key(
        self, idempotency_key: str
    ) -> Optional[WorkflowActionRecord]:
        init_workflow_tables()
        conn = _get_conn()
        row = conn.execute(
            "SELECT * FROM workflow_action_records WHERE idempotency_key=?",
            (idempotency_key,),
        ).fetchone()
        conn.close()
        if row is None:
            return None
        return self._row_to_action_record(dict(row))

    def list_action_records(self, run_id: str) -> List[WorkflowActionRecord]:
        init_workflow_tables()
        conn = _get_conn()
        rows = conn.execute(
            "SELECT * FROM workflow_action_records WHERE run_id=? ORDER BY created_at",
            (run_id,),
        ).fetchall()
        conn.close()
        return [self._row_to_action_record(dict(r)) for r in rows]

    def list_action_attempts(self, action_id: str) -> List[WorkflowActionAttempt]:
        init_workflow_tables()
        conn = _get_conn()
        try:
            rows = conn.execute(
                """SELECT * FROM workflow_action_attempts
                   WHERE action_id=? ORDER BY attempt""",
                (action_id,),
            ).fetchall()
            attempts: List[WorkflowActionAttempt] = []
            for row in rows:
                data = dict(row)
                request_metadata = data.get("request_metadata_json") or "{}"
                result = data.get("result_json") or "{}"
                if isinstance(request_metadata, str):
                    request_metadata = json.loads(request_metadata)
                if isinstance(result, str):
                    result = json.loads(result)
                attempts.append(WorkflowActionAttempt(
                    attempt_id=data["attempt_id"],
                    action_id=data["action_id"],
                    attempt=int(data.get("attempt") or 0),
                    status=ActionStatus(data.get("status") or "pending"),
                    started_at=data.get("started_at") or "",
                    finished_at=data.get("finished_at") or "",
                    request_metadata=request_metadata or {},
                    external_reference=data.get("external_reference") or "",
                    result=result or {},
                    error=data.get("error") or "",
                    last_reconciled_at=data.get("last_reconciled_at") or "",
                ))
            return attempts
        finally:
            conn.close()

    def _row_to_action_record(self, d: Dict[str, Any]) -> WorkflowActionRecord:
        params = d.get("params_json", "{}")
        result = d.get("result_json", "{}")
        request_metadata = d.get("request_metadata_json", "{}")
        if isinstance(params, str):
            params = json.loads(params) if params else {}
        if isinstance(result, str):
            result = json.loads(result) if result else {}
        if isinstance(request_metadata, str):
            request_metadata = json.loads(request_metadata) if request_metadata else {}
        return WorkflowActionRecord(
            action_id=d["action_id"],
            run_id=d["run_id"],
            node_id=d.get("node_id", ""),
            action_type=d.get("action_type", ""),
            idempotency_key=d.get("idempotency_key", ""),
            params=params or {},
            result=result or {},
            status=ActionStatus(d.get("status", "pending")),
            error=d.get("error", ""),
            created_at=d.get("created_at", ""),
            completed_at=d.get("completed_at", ""),
            event_id=d.get("event_id", "") or "",
            semantic_action_version=d.get("semantic_action_version", "v1") or "v1",
            attempt=int(d.get("attempt", 0) or 0),
            started_at=d.get("started_at", "") or "",
            finished_at=d.get("finished_at", "") or "",
            request_metadata=request_metadata or {},
            external_reference=d.get("external_reference", "") or "",
            last_reconciled_at=d.get("last_reconciled_at", "") or "",
            unknown_since=d.get("unknown_since", "") or "",
            reconciliation_attempts=int(d.get("reconciliation_attempts", 0) or 0),
            reconciliation_supported=bool(d.get("reconciliation_supported", 0)),
            retryable=bool(d.get("retryable", 0)),
            reconciliation_message=d.get("reconciliation_message", "") or "",
        )

    def claim_action_execution(
        self,
        record: WorkflowActionRecord,
    ) -> Dict[str, Any]:
        """Claim exactly one side-effect attempt for an idempotency key.

        Initial execution inserts the parent record and attempt atomically.
        A retry is claimable only after an explicit API transition changed a
        known FAILED execution back to PENDING.  RUNNING/UNKNOWN/SUCCEEDED are
        never dispatched again.
        """
        init_workflow_tables()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            durable_run = conn.execute(
                "SELECT status FROM workflow_runs WHERE run_id=?",
                (record.run_id,),
            ).fetchone()
            if durable_run is not None and durable_run["status"] == WorkflowRunStatus.CANCELLED.value:
                conn.rollback()
                return {"claimed": False, "reason": "run_cancelled", "record": None}

            versioned_key = compute_action_idempotency_key(
                record.run_id,
                record.node_id,
                record.action_type,
                record.semantic_action_version,
            )
            candidate_keys = {record.idempotency_key, versioned_key}
            if record.semantic_action_version == "v1":
                candidate_keys.add(compute_legacy_action_idempotency_key(
                    record.run_id,
                    record.node_id,
                    record.action_type,
                ))
            placeholders = ",".join("?" for _ in candidate_keys)
            existing_rows = conn.execute(
                f"""SELECT * FROM workflow_action_records
                    WHERE idempotency_key IN ({placeholders})
                    ORDER BY CASE status
                        WHEN 'succeeded' THEN 0
                        WHEN 'unknown' THEN 1
                        WHEN 'running' THEN 2
                        WHEN 'executing' THEN 2
                        WHEN 'pending' THEN 3
                        ELSE 4
                    END, created_at ASC""",
                tuple(candidate_keys),
            ).fetchall()
            existing_row = existing_rows[0] if existing_rows else None
            if len(existing_rows) > 1:
                existing = self._row_to_action_record(dict(existing_row))
                conn.rollback()
                return {
                    "claimed": False,
                    "reason": "duplicate_semantic_identity",
                    "record": existing,
                }
            now = _utc_now_iso()
            if existing_row is None:
                attempt = 1
                record.attempt = attempt
                record.status = ActionStatus.RUNNING
                record.started_at = now
                record.created_at = record.created_at or now
                conn.execute(
                    """INSERT INTO workflow_action_records (
                           action_id, run_id, node_id, action_type, idempotency_key,
                           params_json, result_json, status, error, created_at, completed_at,
                           event_id, semantic_action_version, attempt, started_at, finished_at,
                           request_metadata_json, external_reference, last_reconciled_at,
                           reconciliation_supported, retryable, reconciliation_message
                       ) VALUES (?, ?, ?, ?, ?, ?, '{}', 'running', '', ?, '', ?, ?, ?, ?, '', ?, '', '', ?, 0, '')""",
                    (
                        record.action_id,
                        record.run_id,
                        record.node_id,
                        record.action_type,
                        record.idempotency_key,
                        json.dumps(record.params, ensure_ascii=False),
                        record.created_at,
                        record.event_id,
                        record.semantic_action_version,
                        attempt,
                        now,
                        json.dumps(record.request_metadata, ensure_ascii=False),
                        1 if record.reconciliation_supported else 0,
                    ),
                )
                conn.execute(
                    """INSERT INTO workflow_action_attempts (
                           attempt_id, action_id, attempt, status, started_at,
                           request_metadata_json
                       ) VALUES (?, ?, ?, 'running', ?, ?)""",
                    (
                        f"{record.action_id}:attempt:{attempt}",
                        record.action_id,
                        attempt,
                        now,
                        json.dumps(record.request_metadata, ensure_ascii=False),
                    ),
                )
                _append_event_tx(conn, record.run_id, "action_created", node_id=record.node_id, payload={
                    "actionExecutionId": record.action_id,
                    "workflowRunId": record.run_id,
                    "actionType": record.action_type,
                    "eventId": record.event_id,
                    "attempt": attempt,
                })
                _append_event_tx(conn, record.run_id, "action_started", node_id=record.node_id, payload={
                    "actionExecutionId": record.action_id,
                    "workflowRunId": record.run_id,
                    "eventId": record.event_id or None,
                    "actionType": record.action_type,
                    "attempt": attempt,
                })
                conn.commit()
                claimed = conn.execute(
                    "SELECT * FROM workflow_action_records WHERE action_id=?",
                    (record.action_id,),
                ).fetchone()
                return {
                    "claimed": True,
                    "reason": "created",
                    "record": self._row_to_action_record(dict(claimed)),
                }

            existing = self._row_to_action_record(dict(existing_row))
            if (
                existing.run_id != record.run_id
                or existing.node_id != record.node_id
                or existing.event_id != record.event_id
                or existing.action_type != record.action_type
                or existing.semantic_action_version
                != record.semantic_action_version
            ):
                conn.rollback()
                return {
                    "claimed": False,
                    "reason": "identity_mismatch",
                    "record": existing,
                }
            if existing.status != ActionStatus.PENDING:
                conn.rollback()
                return {
                    "claimed": False,
                    "reason": existing.status.value,
                    "record": existing,
                }

            next_attempt = max(0, int(existing.attempt or 0)) + 1
            claim_request_metadata = dict(record.request_metadata or {})
            claim_request_metadata["idempotencyKey"] = existing.idempotency_key
            claimed = conn.execute(
                """UPDATE workflow_action_records SET
                       status='running', attempt=?, started_at=?, finished_at='',
                       completed_at='', result_json='{}', error='', retryable=0,
                       request_metadata_json=?, external_reference='',
                       last_reconciled_at='', reconciliation_message=''
                   WHERE action_id=? AND status='pending'""",
                (
                    next_attempt,
                    now,
                    json.dumps(claim_request_metadata, ensure_ascii=False),
                    existing.action_id,
                ),
            )
            if claimed.rowcount != 1:
                conn.rollback()
                return {"claimed": False, "reason": "claim_lost", "record": existing}
            conn.execute(
                """INSERT INTO workflow_action_attempts (
                       attempt_id, action_id, attempt, status, started_at,
                       request_metadata_json
                   ) VALUES (?, ?, ?, 'running', ?, ?)""",
                (
                    f"{existing.action_id}:attempt:{next_attempt}",
                    existing.action_id,
                    next_attempt,
                    now,
                    json.dumps(claim_request_metadata, ensure_ascii=False),
                ),
            )
            _append_event_tx(conn, existing.run_id, "action_started", node_id=existing.node_id, payload={
                "actionExecutionId": existing.action_id,
                "workflowRunId": existing.run_id,
                "eventId": existing.event_id or None,
                "actionType": existing.action_type,
                "attempt": next_attempt,
            })
            conn.commit()
            row = conn.execute(
                "SELECT * FROM workflow_action_records WHERE action_id=?",
                (existing.action_id,),
            ).fetchone()
            return {
                "claimed": True,
                "reason": "retry",
                "record": self._row_to_action_record(dict(row)),
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def finalize_action_execution(self, finalization: Dict[str, Any]) -> bool:
        """Persist one terminal attempt plus its immutable audit event."""
        init_workflow_tables()
        _ensure_driver_columns()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT run_id, node_id, action_type, event_id FROM workflow_action_records WHERE action_id=?",
                (str(finalization.get("actionExecutionId") or ""),),
            ).fetchone()
            if row is None or not _apply_action_finalization_tx(conn, finalization):
                conn.rollback()
                return False
            status = str(finalization.get("status") or "")
            event_type = {
                ActionStatus.SUCCEEDED.value: "action_succeeded",
                ActionStatus.FAILED.value: "action_failed",
                ActionStatus.UNKNOWN.value: "action_unknown",
                ActionStatus.CANCELLED.value: "action_blocked",
                ActionStatus.BLOCKED.value: "action_blocked",
            }[status]
            _append_event_tx(conn, row["run_id"], event_type, node_id=row["node_id"], payload={
                "actionExecutionId": str(finalization.get("actionExecutionId") or ""),
                "workflowRunId": row["run_id"],
                "eventId": row["event_id"] or None,
                "actionType": row["action_type"],
                "attempt": int(finalization.get("attempt") or 0),
                "status": status,
                "externalReference": str(finalization.get("externalReference") or "") or None,
                "error": str(finalization.get("error") or "")[:500] or None,
            })
            if status == ActionStatus.UNKNOWN.value:
                _project_unknown_action_to_run_tx(
                    conn,
                    str(finalization.get("actionExecutionId") or ""),
                    reason=(
                        str(finalization.get("error") or "")
                        or "external outcome unknown; reconciliation required"
                    ),
                    project_node=False,
                )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def create_dispatch_task(
        self,
        *,
        event_id: str,
        run_id: str,
        action_execution_id: str,
        idempotency_key: str,
        assignee: str,
        target: str,
        instruction: str,
    ) -> Tuple[Dict[str, Any], bool]:
        """Create one real internal dispatch task, deduplicated by action key."""
        init_workflow_tables()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT * FROM workflow_dispatch_tasks WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                conn.rollback()
                return dict(existing), False
            task_id = f"dispatch_{action_execution_id}"
            now = _utc_now_iso()
            conn.execute(
                """INSERT INTO workflow_dispatch_tasks (
                       dispatch_task_id, event_id, workflow_run_id,
                       action_execution_id, idempotency_key, assignee, target,
                       instruction, status, created_at, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'created', ?, ?)""",
                (
                    task_id,
                    event_id,
                    run_id,
                    action_execution_id,
                    idempotency_key,
                    assignee,
                    target,
                    instruction,
                    now,
                    now,
                ),
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM workflow_dispatch_tasks WHERE dispatch_task_id=?",
                (task_id,),
            ).fetchone()
            return dict(row), True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_dispatch_task_by_idempotency_key(self, idempotency_key: str) -> Optional[Dict[str, Any]]:
        init_workflow_tables()
        conn = _get_conn()
        try:
            row = conn.execute(
                "SELECT * FROM workflow_dispatch_tasks WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
            return dict(row) if row is not None else None
        finally:
            conn.close()

    def record_local_notification_receipt(
        self,
        *,
        receipt_id: str,
        idempotency_key: str,
        channel: str,
        target: str,
        message_digest: str,
    ) -> Tuple[Dict[str, Any], bool]:
        """Controlled provider boundary used by development and acceptance tests."""
        init_workflow_tables()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT * FROM workflow_notification_receipts WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                conn.rollback()
                return dict(existing), False
            conn.execute(
                """INSERT INTO workflow_notification_receipts (
                       receipt_id, idempotency_key, channel, target,
                       message_digest, delivered_at
                   ) VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    receipt_id,
                    idempotency_key,
                    channel,
                    target,
                    message_digest,
                    _utc_now_iso(),
                ),
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM workflow_notification_receipts WHERE receipt_id=?",
                (receipt_id,),
            ).fetchone()
            return dict(row), True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_local_notification_receipt(
        self,
        *,
        external_reference: str = "",
        idempotency_key: str = "",
    ) -> Optional[Dict[str, Any]]:
        init_workflow_tables()
        conn = _get_conn()
        try:
            if external_reference:
                row = conn.execute(
                    "SELECT * FROM workflow_notification_receipts WHERE receipt_id=?",
                    (external_reference,),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM workflow_notification_receipts WHERE idempotency_key=?",
                    (idempotency_key,),
                ).fetchone()
            return dict(row) if row is not None else None
        finally:
            conn.close()

    def request_action_retry(self, action_id: str) -> Dict[str, Any]:
        """Atomically make a known FAILED action and its run runnable again."""
        init_workflow_tables()
        _ensure_driver_columns()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            action = conn.execute(
                "SELECT * FROM workflow_action_records WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if action is None:
                conn.rollback()
                return {"updated": False, "reason": "not_found"}
            run = conn.execute(
                "SELECT * FROM workflow_runs WHERE run_id=?",
                (action["run_id"],),
            ).fetchone()
            if run is None:
                conn.rollback()
                return {"updated": False, "reason": "run_not_found"}
            if run["status"] == WorkflowRunStatus.CANCELLED.value:
                conn.rollback()
                return {"updated": False, "reason": "run_cancelled"}
            if action["status"] != ActionStatus.FAILED.value or not bool(action["retryable"]):
                conn.rollback()
                return {"updated": False, "reason": "invalid_status"}
            if run["status"] not in {
                WorkflowRunStatus.FAILED.value,
                WorkflowRunStatus.PAUSED.value,
            }:
                conn.rollback()
                return {"updated": False, "reason": "invalid_run_status"}

            state = json.loads(run["state_json"] or "{}")
            state["status"] = WorkflowRunStatus.PENDING.value
            state["currentNode"] = action["node_id"]
            state["finishedAt"] = ""
            state["retryCount"] = int(state.get("retryCount", 0) or 0) + 1
            update_action = conn.execute(
                """UPDATE workflow_action_records SET
                       status='pending', retryable=0, error='', result_json='{}',
                       started_at='', finished_at='', completed_at='',
                       external_reference='', last_reconciled_at='',
                       reconciliation_message=''
                   WHERE action_id=? AND status='failed' AND retryable=1""",
                (action_id,),
            )
            update_run = conn.execute(
                """UPDATE workflow_runs SET
                       status='pending', current_node_id=?, state_json=?,
                       updated_at=?, completed_at='', driver_managed=1,
                       driver_owner=NULL, driver_lease_until=NULL,
                       driver_heartbeat_at=NULL
                   WHERE run_id=? AND status IN ('failed','paused')""",
                (
                    action["node_id"],
                    json.dumps(state, ensure_ascii=False),
                    _utc_now_iso(),
                    action["run_id"],
                ),
            )
            if update_action.rowcount != 1 or update_run.rowcount != 1:
                conn.rollback()
                return {"updated": False, "reason": "concurrent_change"}
            _append_event_tx(conn, action["run_id"], "action_retry_requested", node_id=action["node_id"], payload={
                "actionExecutionId": action_id,
                "workflowRunId": action["run_id"],
                "eventId": action["event_id"] or None,
                "actionType": action["action_type"],
                "previousAttempt": int(action["attempt"] or 0),
                "attempt": int(action["attempt"] or 0) + 1,
            })
            conn.commit()
            return {
                "updated": True,
                "runId": action["run_id"],
                "nodeId": action["node_id"],
                "actionExecutionId": action_id,
                "nextAttempt": int(action["attempt"] or 0) + 1,
                "retryCount": int(state.get("retryCount", 0) or 0),
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def apply_action_reconciliation(
        self,
        action_id: str,
        *,
        status: ActionStatus,
        result: Optional[Dict[str, Any]] = None,
        error: str = "",
        external_reference: str = "",
        supported: bool = True,
        retryable: bool = False,
        message: str = "",
    ) -> Dict[str, Any]:
        """Apply a query-only reconciliation result without re-executing."""
        if status not in {ActionStatus.SUCCEEDED, ActionStatus.FAILED, ActionStatus.UNKNOWN}:
            raise ValueError(f"invalid reconciliation status: {status.value}")
        init_workflow_tables()
        _ensure_driver_columns()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            action = conn.execute(
                "SELECT * FROM workflow_action_records WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if action is None:
                conn.rollback()
                return {"updated": False, "reason": "not_found"}
            if action["status"] != ActionStatus.UNKNOWN.value:
                conn.rollback()
                return {"updated": False, "reason": "invalid_status"}
            run = conn.execute(
                "SELECT * FROM workflow_runs WHERE run_id=?",
                (action["run_id"],),
            ).fetchone()
            if run is None:
                conn.rollback()
                return {"updated": False, "reason": "run_not_found"}
            if run["status"] not in {
                WorkflowRunStatus.PAUSED.value,
                WorkflowRunStatus.RUNNING.value,
                WorkflowRunStatus.FAILED.value,
                WorkflowRunStatus.CANCELLED.value,
            }:
                conn.rollback()
                return {"updated": False, "reason": "invalid_run_status"}

            now = _utc_now_iso()
            safe_result = result if isinstance(result, dict) else {}
            run_cancelled = run["status"] == WorkflowRunStatus.CANCELLED.value
            next_retryable = (
                1
                if status == ActionStatus.FAILED and retryable and not run_cancelled
                else 0
            )
            finished_at = now if status != ActionStatus.UNKNOWN else ""
            action_update = conn.execute(
                """UPDATE workflow_action_records SET
                       status=?, result_json=?, error=?, external_reference=?,
                       last_reconciled_at=?, reconciliation_supported=?,
                       retryable=?, reconciliation_message=?,
                       reconciliation_attempts=reconciliation_attempts + 1,
                       unknown_since=CASE WHEN ?='unknown'
                           THEN COALESCE(NULLIF(unknown_since, ''), ?)
                           ELSE '' END,
                       finished_at=CASE WHEN ?='' THEN finished_at ELSE ? END,
                       completed_at=CASE WHEN ?='' THEN completed_at ELSE ? END
                   WHERE action_id=? AND status='unknown'""",
                (
                    status.value,
                    json.dumps(safe_result, ensure_ascii=False),
                    str(error or "")[:500],
                    external_reference or action["external_reference"],
                    now,
                    1 if supported else 0,
                    next_retryable,
                    str(message or "")[:500],
                    status.value,
                    now,
                    finished_at,
                    finished_at,
                    finished_at,
                    finished_at,
                    action_id,
                ),
            )
            if action_update.rowcount != 1:
                conn.rollback()
                return {"updated": False, "reason": "concurrent_change"}
            conn.execute(
                """UPDATE workflow_action_attempts SET
                       status=?, result_json=?, error=?, external_reference=?,
                       last_reconciled_at=?,
                       finished_at=CASE WHEN ?='' THEN finished_at ELSE ? END
                   WHERE action_id=? AND attempt=?""",
                (
                    status.value,
                    json.dumps(safe_result, ensure_ascii=False),
                    str(error or "")[:500],
                    external_reference or action["external_reference"],
                    now,
                    finished_at,
                    finished_at,
                    action_id,
                    int(action["attempt"] or 0),
                ),
            )
            state = json.loads(run["state_json"] or "{}")
            resolved_external_reference = (
                external_reference or action["external_reference"]
            )
            action_output = {
                "action_id": action_id,
                "actionExecutionId": action_id,
                "action_type": action["action_type"],
                "actionType": action["action_type"],
                "attempt": int(action["attempt"] or 0),
                "status": status.value,
                "result": safe_result,
                "error": str(error or "")[:500],
                "externalReference": resolved_external_reference or None,
                "reconciliationSupported": bool(supported),
                "retryable": bool(next_retryable),
            }
            state.setdefault("actionResults", {})[action["action_type"]] = {
                "actionExecutionId": action_id,
                "status": status.value,
                "result": safe_result,
                "error": str(error or "")[:500],
                "externalReference": resolved_external_reference or None,
            }
            if run_cancelled:
                # Reconciliation is query-only.  It may improve the durable
                # Action fact, but must never revive or advance a cancelled
                # Workflow, even when the provider confirms success.
                state["status"] = WorkflowRunStatus.CANCELLED.value
                run_update = conn.execute(
                    """UPDATE workflow_runs SET state_json=?, updated_at=?
                       WHERE run_id=? AND status='cancelled'""",
                    (
                        json.dumps(state, ensure_ascii=False),
                        now,
                        action["run_id"],
                    ),
                )
                if run_update.rowcount != 1:
                    conn.rollback()
                    return {"updated": False, "reason": "concurrent_change"}
                if status in {ActionStatus.SUCCEEDED, ActionStatus.FAILED}:
                    _append_event_tx(
                        conn,
                        action["run_id"],
                        (
                            "action_succeeded"
                            if status == ActionStatus.SUCCEEDED
                            else "action_failed"
                        ),
                        node_id=action["node_id"],
                        payload={
                            "actionExecutionId": action_id,
                            "workflowRunId": action["run_id"],
                            "eventId": action["event_id"] or None,
                            "actionType": action["action_type"],
                            "attempt": int(action["attempt"] or 0),
                            "source": "reconciliation",
                            "runCancelled": True,
                            "retryable": False,
                        },
                    )
            elif status == ActionStatus.SUCCEEDED:
                completed = state.get("completedSteps") or []
                if action["node_id"] not in completed:
                    completed.append(action["node_id"])
                state["completedSteps"] = completed
                state["currentNode"] = action["node_id"]
                state["status"] = WorkflowRunStatus.PENDING.value
                state["finishedAt"] = ""
                state.setdefault("nodeOutputs", {})[action["node_id"]] = action_output
                conn.execute(
                    """UPDATE workflow_node_runs SET status='succeeded', error='',
                           completed_at=?, output_snapshot_json=?
                       WHERE node_run_id=(
                           SELECT node_run_id FROM workflow_node_runs
                           WHERE run_id=? AND node_id=? AND status IN ('running','paused')
                           ORDER BY attempt DESC LIMIT 1
                       )""",
                    (
                        now,
                        json.dumps(action_output, ensure_ascii=False),
                        action["run_id"],
                        action["node_id"],
                    ),
                )
                run_update = conn.execute(
                    """UPDATE workflow_runs SET
                           status='pending', current_node_id=?, state_json=?,
                           updated_at=?, completed_at='', driver_managed=1,
                           driver_owner=NULL, driver_lease_until=NULL,
                           driver_heartbeat_at=NULL
                       WHERE run_id=? AND status IN ('paused','running','failed')""",
                    (
                        action["node_id"],
                        json.dumps(state, ensure_ascii=False),
                        now,
                        action["run_id"],
                    ),
                )
                if run_update.rowcount != 1:
                    conn.rollback()
                    return {"updated": False, "reason": "concurrent_change"}
                _append_event_tx(conn, action["run_id"], "action_succeeded", node_id=action["node_id"], payload={
                    "actionExecutionId": action_id,
                    "workflowRunId": action["run_id"],
                    "eventId": action["event_id"] or None,
                    "actionType": action["action_type"],
                    "attempt": int(action["attempt"] or 0),
                    "source": "reconciliation",
                })
            elif status == ActionStatus.FAILED:
                state["status"] = WorkflowRunStatus.FAILED.value
                state["finishedAt"] = now
                state.setdefault("errors", []).append({
                    "nodeId": action["node_id"],
                    "error": str(error or message or "reconciliation confirmed not executed")[:500],
                    "attempt": int(action["attempt"] or 0),
                    "timestamp": now,
                })
                conn.execute(
                    """UPDATE workflow_node_runs SET status='failed', error=?, completed_at=?
                       WHERE node_run_id=(
                           SELECT node_run_id FROM workflow_node_runs
                           WHERE run_id=? AND node_id=? AND status IN ('running','paused')
                           ORDER BY attempt DESC LIMIT 1
                       )""",
                    (str(error or message or "not executed")[:500], now, action["run_id"], action["node_id"]),
                )
                run_update = conn.execute(
                    """UPDATE workflow_runs SET status='failed', state_json=?, updated_at=?, completed_at=?
                       WHERE run_id=? AND status IN ('paused','running','failed')""",
                    (json.dumps(state, ensure_ascii=False), now, now, action["run_id"]),
                )
                if run_update.rowcount != 1:
                    conn.rollback()
                    return {"updated": False, "reason": "concurrent_change"}
                _append_event_tx(conn, action["run_id"], "action_failed", node_id=action["node_id"], payload={
                    "actionExecutionId": action_id,
                    "workflowRunId": action["run_id"],
                    "eventId": action["event_id"] or None,
                    "actionType": action["action_type"],
                    "attempt": int(action["attempt"] or 0),
                    "source": "reconciliation",
                    "retryable": bool(retryable),
                })
            elif status == ActionStatus.UNKNOWN:
                _project_unknown_action_to_run_tx(
                    conn,
                    action_id,
                    reason=(
                        str(error or message or "external outcome remains unknown")[:500]
                    ),
                )
            _append_event_tx(conn, action["run_id"], "action_reconciled", node_id=action["node_id"], payload={
                "actionExecutionId": action_id,
                "workflowRunId": action["run_id"],
                "eventId": action["event_id"] or None,
                "actionType": action["action_type"],
                "attempt": int(action["attempt"] or 0),
                "outcome": status.value,
                "supported": bool(supported),
                "message": str(message or "")[:500] or None,
            })
            conn.commit()
            return {
                "updated": True,
                "runId": action["run_id"],
                "nodeId": action["node_id"],
                "actionExecutionId": action_id,
                "status": status.value,
                "runStatus": run["status"] if run_cancelled else (
                    WorkflowRunStatus.PENDING.value
                    if status == ActionStatus.SUCCEEDED
                    else (
                        WorkflowRunStatus.FAILED.value
                        if status == ActionStatus.FAILED
                        else run["status"]
                    )
                ),
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def mark_running_action_unknown_and_pause(
        self,
        action_id: str,
        *,
        reason: str = "runtime restarted after dispatch; outcome unknown",
    ) -> bool:
        """Crash recovery fence: RUNNING action becomes UNKNOWN, never replayed."""
        init_workflow_tables()
        _ensure_driver_columns()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            action = conn.execute(
                "SELECT * FROM workflow_action_records WHERE action_id=?",
                (action_id,),
            ).fetchone()
            if action is None or action["status"] not in {"running", "executing"}:
                conn.rollback()
                return False
            run = conn.execute(
                "SELECT * FROM workflow_runs WHERE run_id=?",
                (action["run_id"],),
            ).fetchone()
            if run is None:
                conn.rollback()
                return False
            now = _utc_now_iso()
            conn.execute(
                """UPDATE workflow_action_records SET
                       status='unknown', error=?, reconciliation_message=?,
                       unknown_since=COALESCE(NULLIF(unknown_since, ''), ?)
                   WHERE action_id=? AND status IN ('running','executing')""",
                (
                    reason[:500],
                    "reconciliation required before retry",
                    now,
                    action_id,
                ),
            )
            conn.execute(
                """UPDATE workflow_action_attempts SET status='unknown', error=?
                   WHERE action_id=? AND attempt=? AND status IN ('running','executing')""",
                (reason[:500], action_id, int(action["attempt"] or 0)),
            )
            _project_unknown_action_to_run_tx(
                conn,
                action_id,
                reason=reason,
            )
            _append_event_tx(conn, action["run_id"], "action_unknown", node_id=action["node_id"], payload={
                "actionExecutionId": action_id,
                "workflowRunId": action["run_id"],
                "eventId": action["event_id"] or None,
                "actionType": action["action_type"],
                "attempt": int(action["attempt"] or 0),
                "reason": reason,
                "recovery": True,
            })
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ── Batch Read Helpers (Workflow Center V2 Round 1) ──────────────────

    def batch_get_node_counts(
        self, run_ids: List[str]
    ) -> Dict[str, Dict[str, int]]:
        """批量获取每个 Run 的节点执行统计。

        返回: {run_id: {"total": N, "succeeded": N, "failed": N, "unknown": N}}
        不存在的 run_id 不出现在结果中。
        """
        if not run_ids:
            return {}
        init_workflow_tables()
        conn = _get_conn()
        placeholders = ",".join(["?" for _ in run_ids])
        rows = conn.execute(
            f"""SELECT run_id, status, COUNT(*) as cnt
                FROM workflow_node_runs
                WHERE run_id IN ({placeholders})
                GROUP BY run_id, status""",
            run_ids,
        ).fetchall()
        conn.close()

        result: Dict[str, Dict[str, int]] = {}
        for row in rows:
            rid = row["run_id"]
            st = row["status"]
            cnt = row["cnt"]
            if rid not in result:
                result[rid] = {"total": 0, "succeeded": 0, "failed": 0,
                               "running": 0, "pending": 0}
            result[rid]["total"] += cnt
            if st in ("succeeded",):
                result[rid]["succeeded"] += cnt
            elif st in ("failed", "timed_out"):
                result[rid]["failed"] += cnt
            elif st in ("running", "retrying", "awaiting_approval"):
                result[rid]["running"] += cnt
            elif st in ("pending",):
                result[rid]["pending"] += cnt
        return result

    def batch_get_action_counts(
        self, run_ids: List[str]
    ) -> Dict[str, Dict[str, int]]:
        """批量获取每个 Run 的 Action 执行统计。

        返回: {run_id: {"total": N, "succeeded": N, "failed": N}}
        不存在的 run_id 不出现在结果中。
        """
        if not run_ids:
            return {}
        init_workflow_tables()
        conn = _get_conn()
        placeholders = ",".join(["?" for _ in run_ids])
        rows = conn.execute(
            f"""SELECT run_id, status, COUNT(*) as cnt
                FROM workflow_action_records
                WHERE run_id IN ({placeholders})
                GROUP BY run_id, status""",
            run_ids,
        ).fetchall()
        conn.close()

        result: Dict[str, Dict[str, int]] = {}
        for row in rows:
            rid = row["run_id"]
            st = row["status"]
            cnt = row["cnt"]
            if rid not in result:
                result[rid] = {"total": 0, "succeeded": 0, "failed": 0, "unknown": 0}
            result[rid]["total"] += cnt
            if st in ("succeeded",):
                result[rid]["succeeded"] += cnt
            elif st in ("failed",):
                result[rid]["failed"] += cnt
            elif st == "unknown":
                result[rid]["unknown"] += cnt
        return result

    def batch_get_definition_summaries(
        self, definition_ids: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """批量获取 Definition ID → {name, nodeCount} 映射。

        返回: {definition_id: {"name": str, "nodeCount": int}}
        不存在的 definition_id 不出现在结果中。
        """
        if not definition_ids:
            return {}
        init_workflow_tables()
        conn = _get_conn()
        placeholders = ",".join(["?" for _ in definition_ids])
        rows = conn.execute(
            f"""SELECT id, name, nodes_json FROM workflow_definitions
                WHERE id IN ({placeholders})""",
            definition_ids,
        ).fetchall()
        conn.close()

        result: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            nodes_raw = row["nodes_json"]
            if isinstance(nodes_raw, str):
                try:
                    nodes_list = json.loads(nodes_raw) if nodes_raw else []
                except json.JSONDecodeError:
                    nodes_list = []
            else:
                nodes_list = nodes_raw or []
            node_count = len(nodes_list) if isinstance(nodes_list, list) else 0
            result[row["id"]] = {
                "name": row["name"],
                "nodeCount": node_count,
            }
        return result

    def batch_get_approval_decisions(
        self, run_ids: List[str]
    ) -> Dict[str, List[str]]:
        """批量获取每个 Run 的审批决策列表。

        返回: {run_id: [decision, ...]}
        用于判断 completed run 是否历史上经过审批。
        """
        if not run_ids:
            return {}
        init_workflow_tables()
        conn = _get_conn()
        placeholders = ",".join(["?" for _ in run_ids])
        rows = conn.execute(
            f"""SELECT run_id, decision FROM workflow_approvals
                WHERE run_id IN ({placeholders})
                ORDER BY created_at""",
            run_ids,
        ).fetchall()
        conn.close()

        result: Dict[str, List[str]] = {}
        for row in rows:
            rid = row["run_id"]
            if rid not in result:
                result[rid] = []
            result[rid].append(row["decision"])
        return result

    # ── Phase 17 Round 2: atomic child continuation ──────────────────────

    def create_child_continuation_tx(
        self,
        child_run: "WorkflowRun",
        parent_run_id: str,
        parent_status: str,
        parent_state: Dict[str, Any],
        definition_json: Dict[str, Any],
        changelog: str = "replan",
    ) -> int:
        """原子 child cutover（单一 BEGIN IMMEDIATE 事务）。

        顺序：version allocation → insert version → insert child run → update parent。
        任何异常 rollback（parent 不被半写 / child 不 orphan / version 不覆盖）。
        返回分配的 version。
        """
        import uuid as _uuid

        init_workflow_tables()
        _ensure_wait_columns()
        _ensure_driver_columns()
        now = _utc_now_iso()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            # 1. version allocation（definition-level global monotonic，事务内无 race）
            row = conn.execute(
                "SELECT MAX(version) as mv FROM workflow_definition_versions WHERE definition_id=?",
                (child_run.definition_id,),
            ).fetchone()
            next_version = (row["mv"] if row and row["mv"] is not None else 0) + 1
            child_run.version = next_version

            # 2. insert version snapshot（UNIQUE(definition_id, version)，碰撞即 rollback）
            conn.execute(
                "INSERT INTO workflow_definition_versions (id, definition_id, version, definition_json, changelog, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (f"wfver_{_uuid.uuid4().hex[:12]}", child_run.definition_id, next_version,
                 json.dumps(definition_json, ensure_ascii=False), changelog, now),
            )

            # 3. insert child run record（确定性 run_id，PK 碰撞即 rollback → 幂等）
            #    driver_managed=1 在同一事务内落库（COMMIT 后即 driver 可发现，无 post-commit mark 窗口）
            conn.execute(
                """INSERT INTO workflow_runs (run_id, definition_id, version, session_id, event_thread_id,
                       status, current_node_id, state_json, started_at, updated_at, completed_at, triggered_by,
                       wait_type, wake_at, resumed_at, resume_reason, driver_managed)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
                (child_run.run_id, child_run.definition_id, next_version,
                 child_run.session_id, child_run.event_thread_id, child_run.status.value,
                 child_run.current_node_id, json.dumps(child_run.state, ensure_ascii=False),
                 child_run.started_at, child_run.updated_at, child_run.completed_at,
                 child_run.triggered_by, "", None, None, ""),
            )

            # 4. update parent run（terminal + lineage/termination metadata）
            #    replannedToVersion 必须 = 事务内实际分配的 next_version（非 parent.version+1）
            parent_state["replannedToVersion"] = next_version
            conn.execute(
                "UPDATE workflow_runs SET status=?, state_json=?, updated_at=? WHERE run_id=?",
                (parent_status, json.dumps(parent_state, ensure_ascii=False), now, parent_run_id),
            )

            conn.commit()
            return next_version
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ── Phase18 Round2: atomic critic/assessment invocation claim ─────────────

    def claim_critic_invocation_tx(self, run_id: str, invocation_key: str) -> Dict[str, Any]:
        """原子 critic claim：BEGIN IMMEDIATE 内 compound budget reserve + STARTED marker。

        result ∈ {claimed, already_completed, already_started, budget_exhausted, not_eligible}。
        budget 检查 + 两个 counter 递增 + registry STARTED 写回同一事务（全成功或零变化）。
        """
        if not invocation_key or not run_id:
            return {"result": "not_eligible"}
        init_workflow_tables()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT state_json FROM workflow_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                conn.rollback()
                return {"result": "not_eligible"}
            state = json.loads(row["state_json"] or "{}")
            registry = state.get("criticInvocations", {}) or {}
            existing = registry.get(invocation_key)
            if isinstance(existing, dict):
                if existing.get("status") == "COMPLETED":
                    conn.rollback()
                    return {"result": "already_completed", "recommendation": existing.get("recommendation", {})}
                conn.rollback()
                return {"result": "already_started"}

            lineage = state.get("executionLineage", {}) or {}
            usage = lineage.get("budgetUsage", {}) or {}
            limits = lineage.get("budgetLimits", {}) or {}
            llm_used = int(usage.get("llmCallsUsed", 0))
            critic_used = int(usage.get("criticCallsUsed", 0))
            if llm_used >= int(limits.get("maxLlmCalls", 5)) or critic_used >= int(limits.get("maxCriticCalls", 3)):
                conn.rollback()
                return {"result": "budget_exhausted"}

            usage["llmCallsUsed"] = llm_used + 1
            usage["criticCallsUsed"] = critic_used + 1
            lineage["budgetUsage"] = usage
            state["executionLineage"] = lineage
            registry[invocation_key] = {"status": "STARTED"}
            state["criticInvocations"] = registry
            conn.execute(
                "UPDATE workflow_runs SET state_json=?, updated_at=? WHERE run_id=?",
                (json.dumps(state, ensure_ascii=False), _utc_now_iso(), run_id),
            )
            conn.commit()
            return {"result": "claimed"}
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def complete_critic_invocation_tx(self, run_id: str, invocation_key: str, recommendation: Dict[str, Any]) -> None:
        """原子 critic 完成：仅 STARTED → COMPLETED（reload latest state，不覆盖其它 keys）。"""
        init_workflow_tables()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT state_json FROM workflow_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                conn.rollback()
                return
            state = json.loads(row["state_json"] or "{}")
            registry = state.get("criticInvocations", {}) or {}
            if registry.get(invocation_key, {}).get("status") == "STARTED":
                registry[invocation_key] = {"status": "COMPLETED", "recommendation": recommendation or {}}
                state["criticInvocations"] = registry
                conn.execute(
                    "UPDATE workflow_runs SET state_json=?, updated_at=? WHERE run_id=?",
                    (json.dumps(state, ensure_ascii=False), _utc_now_iso(), run_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def claim_assessment_tx(self, run_id: str, assessment_key: str) -> Dict[str, Any]:
        """原子 assessment claim（compound reserve llmCallsUsed + assessmentCallsUsed + STARTED）。"""
        if not assessment_key or not run_id:
            return {"result": "not_eligible"}
        init_workflow_tables()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT state_json FROM workflow_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                conn.rollback()
                return {"result": "not_eligible"}
            state = json.loads(row["state_json"] or "{}")
            reg = state.get("assessment", {}) or {}
            existing = reg.get(assessment_key)
            if isinstance(existing, dict):
                if existing.get("status") == "COMPLETED":
                    conn.rollback()
                    return {"result": "already_completed", "assessment": existing.get("result", {})}
                conn.rollback()
                return {"result": "already_started"}

            lineage = state.get("executionLineage", {}) or {}
            usage = lineage.get("budgetUsage", {}) or {}
            limits = lineage.get("budgetLimits", {}) or {}
            llm_used = int(usage.get("llmCallsUsed", 0))
            assess_used = int(usage.get("assessmentCallsUsed", 0))
            if llm_used >= int(limits.get("maxLlmCalls", 5)) or assess_used >= int(limits.get("maxAssessments", 1)):
                conn.rollback()
                return {"result": "budget_exhausted"}

            usage["llmCallsUsed"] = llm_used + 1
            usage["assessmentCallsUsed"] = assess_used + 1
            lineage["budgetUsage"] = usage
            state["executionLineage"] = lineage
            reg[assessment_key] = {"status": "STARTED"}
            state["assessment"] = reg
            conn.execute(
                "UPDATE workflow_runs SET state_json=?, updated_at=? WHERE run_id=?",
                (json.dumps(state, ensure_ascii=False), _utc_now_iso(), run_id),
            )
            conn.commit()
            return {"result": "claimed"}
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def complete_assessment_tx(self, run_id: str, assessment_key: str, result: Dict[str, Any]) -> None:
        """原子 assessment 完成：upsert COMPLETED result（幂等）。

        兼容两类路径：
          - provider 路径：claim STARTED → complete → COMPLETED。
          - deterministic/fallback 路径（无 claim）：直接写 COMPLETED。
        """
        init_workflow_tables()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT state_json FROM workflow_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                conn.rollback()
                return
            state = json.loads(row["state_json"] or "{}")
            reg = state.get("assessment", {}) or {}
            reg[assessment_key] = {"status": "COMPLETED", "result": result or {}}
            state["assessment"] = reg
            conn.execute(
                "UPDATE workflow_runs SET state_json=?, updated_at=? WHERE run_id=?",
                (json.dumps(state, ensure_ascii=False), _utc_now_iso(), run_id),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def claim_semantic_replan_tx(self, run_id: str, invocation_key: str) -> Dict[str, Any]:
        """原子 semantic-replan claim：BEGIN IMMEDIATE 内 check invocation + reserve llmCallsUsed + STARTED。"""
        if not invocation_key or not run_id:
            return {"result": "not_eligible"}
        init_workflow_tables()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT state_json FROM workflow_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                conn.rollback()
                return {"result": "not_eligible"}
            state = json.loads(row["state_json"] or "{}")
            registry = state.get("semanticReplanInvocations", {}) or {}
            existing = registry.get(invocation_key)
            if isinstance(existing, dict):
                if existing.get("status") == "COMPLETED":
                    conn.rollback()
                    return {"result": "already_completed", "proposal": existing.get("proposal", {})}
                conn.rollback()
                return {"result": "already_started"}

            lineage = state.get("executionLineage", {}) or {}
            usage = lineage.get("budgetUsage", {}) or {}
            limits = lineage.get("budgetLimits", {}) or {}
            llm_used = int(usage.get("llmCallsUsed", 0))
            if llm_used >= int(limits.get("maxLlmCalls", 5)):
                conn.rollback()
                return {"result": "budget_exhausted"}

            usage["llmCallsUsed"] = llm_used + 1
            lineage["budgetUsage"] = usage
            state["executionLineage"] = lineage
            registry[invocation_key] = {"status": "STARTED"}
            state["semanticReplanInvocations"] = registry
            conn.execute(
                "UPDATE workflow_runs SET state_json=?, updated_at=? WHERE run_id=?",
                (json.dumps(state, ensure_ascii=False), _utc_now_iso(), run_id),
            )
            conn.commit()
            return {"result": "claimed"}
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def complete_semantic_replan_tx(self, run_id: str, invocation_key: str, proposal: Dict[str, Any]) -> None:
        """原子 semantic-replan 完成：仅 STARTED → COMPLETED（不覆盖其它 keys）。"""
        init_workflow_tables()
        conn = _get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT state_json FROM workflow_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                conn.rollback()
                return
            state = json.loads(row["state_json"] or "{}")
            registry = state.get("semanticReplanInvocations", {}) or {}
            if registry.get(invocation_key, {}).get("status") == "STARTED":
                registry[invocation_key] = {"status": "COMPLETED", "proposal": proposal or {}}
                state["semanticReplanInvocations"] = registry
                conn.execute(
                    "UPDATE workflow_runs SET state_json=?, updated_at=? WHERE run_id=?",
                    (json.dumps(state, ensure_ascii=False), _utc_now_iso(), run_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def list_observations(self, run_id: str) -> List["WorkflowEvent"]:
        """列出 run 的 observation 事件（event_type=observation_recorded）。"""
        events = self.list_events(run_id)
        return [e for e in events if e.event_type == "observation_recorded"]

    # ── Phase17 Round3: RunDriver claim / lease / fencing ────────────────

    def mark_driver_managed(self, run_id: str) -> None:
        """标记为 planning driver-managed run。"""
        init_workflow_tables()
        _ensure_driver_columns()
        conn = _get_conn()
        try:
            conn.execute("UPDATE workflow_runs SET driver_managed=1 WHERE run_id=?", (run_id,))
            conn.commit()
        finally:
            conn.close()

    def save_driver_managed_run(self, run: "WorkflowRun") -> None:
        """原子创建 driver-managed run（单次 INSERT，driver_managed=1，无 post-save mark 窗口）。"""
        init_workflow_tables()
        _ensure_wait_columns()
        _ensure_driver_columns()
        conn = _get_conn()
        try:
            conn.execute(
                """INSERT INTO workflow_runs (run_id, definition_id, version, session_id, event_thread_id,
                       status, current_node_id, state_json, started_at, updated_at, completed_at, triggered_by,
                       wait_type, wake_at, resumed_at, resume_reason, driver_managed)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                   ON CONFLICT(run_id) DO UPDATE SET
                       definition_id=excluded.definition_id, version=excluded.version,
                       session_id=excluded.session_id, event_thread_id=excluded.event_thread_id,
                       status=excluded.status, current_node_id=excluded.current_node_id,
                       state_json=excluded.state_json, started_at=excluded.started_at,
                       updated_at=excluded.updated_at, completed_at=excluded.completed_at,
                       triggered_by=excluded.triggered_by,
                       driver_managed=1""",
                (run.run_id, run.definition_id, run.version, run.session_id, run.event_thread_id,
                 run.status.value, run.current_node_id, json.dumps(run.state, ensure_ascii=False),
                 run.started_at, run.updated_at, run.completed_at, run.triggered_by, "", None, None, ""),
            )
            conn.commit()
        finally:
            conn.close()

    def is_driver_managed(self, run_id: str) -> bool:
        """查询 run 是否 driver-managed（planning）。"""
        init_workflow_tables()
        _ensure_driver_columns()
        conn = _get_conn()
        try:
            row = conn.execute(
                "SELECT driver_managed FROM workflow_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            conn.close()
            return bool(row and row["driver_managed"])
        except Exception:
            conn.close()
            return False

    def set_run_status_managed(
        self,
        run_id: str,
        status: str,
        state_dict: Dict[str, Any] = None,
        *,
        expected_status: Optional[str] = None,
        current_node_id: Optional[str] = None,
        ensure_driver_managed: bool = False,
        expected_failed_node_run_id: Optional[str] = None,
        expected_resume_reason: Optional[str] = None,
    ) -> bool:
        """Atomically schedule/wake a driver-managed run.

        ``expected_status`` is a lifecycle CAS guard.  Approval, retry, and
        recovery callers use it so stale commands cannot resurrect a run that
        concurrently became CANCELLED or COMPLETED.
        """
        init_workflow_tables()
        _ensure_wait_columns()
        _ensure_driver_columns()
        conn = _get_conn()
        try:
            status_guard = " AND status=?" if expected_status is not None else ""
            suffix: Tuple[Any, ...] = (
                (expected_status,) if expected_status is not None else ()
            )
            if expected_failed_node_run_id:
                status_guard += (
                    " AND EXISTS (SELECT 1 FROM workflow_node_runs AS nr"
                    " WHERE nr.node_run_id=? AND nr.run_id=workflow_runs.run_id"
                    " AND nr.status IN ('failed','timed_out'))"
                )
                suffix = (*suffix, expected_failed_node_run_id)
            if expected_resume_reason is not None:
                status_guard += " AND resume_reason=?"
                suffix = (*suffix, expected_resume_reason)
            if state_dict is not None:
                cursor = conn.execute(
                    f"""UPDATE workflow_runs SET status=?, state_json=?, driver_owner=NULL,
                           driver_lease_until=NULL, updated_at=?,
                           completed_at=CASE WHEN ?='pending' THEN '' ELSE completed_at END,
                           current_node_id=CASE WHEN ? IS NULL THEN current_node_id ELSE ? END,
                           driver_managed=CASE WHEN ?=1 THEN 1 ELSE driver_managed END
                       WHERE run_id=?{status_guard}""",
                    (
                        status, json.dumps(state_dict, ensure_ascii=False),
                        _utc_now_iso(), status,
                        current_node_id, current_node_id,
                        1 if ensure_driver_managed else 0,
                        run_id, *suffix,
                    ),
                )
            else:
                cursor = conn.execute(
                    f"""UPDATE workflow_runs SET status=?, driver_owner=NULL,
                           driver_lease_until=NULL, updated_at=?,
                           completed_at=CASE WHEN ?='pending' THEN '' ELSE completed_at END,
                           current_node_id=CASE WHEN ? IS NULL THEN current_node_id ELSE ? END,
                           driver_managed=CASE WHEN ?=1 THEN 1 ELSE driver_managed END
                       WHERE run_id=?{status_guard}""",
                    (
                        status, _utc_now_iso(), status,
                        current_node_id, current_node_id,
                        1 if ensure_driver_managed else 0,
                        run_id, *suffix,
                    ),
                )
            conn.commit()
            return cursor.rowcount == 1
        finally:
            conn.close()

    def claim_driver_run(self, run_id: str, owner: str, lease_until_iso: str) -> Dict[str, Any]:
        """原子 CAS claim。返回 {claimed, generation}。rowcount==1 才成功。"""
        init_workflow_tables()
        _ensure_driver_columns()
        now = _utc_now_iso()
        conn = _get_conn()
        try:
            cur = conn.execute(
                """UPDATE workflow_runs
                   SET driver_owner=?, driver_lease_until=?, driver_heartbeat_at=?,
                       driver_generation = driver_generation + 1
                   WHERE run_id=? AND driver_managed=1
                     AND status IN ('pending','running')
                     AND (driver_lease_until IS NULL OR driver_lease_until < ?)""",
                (owner, lease_until_iso, now, run_id, now),
            )
            conn.commit()
            if cur.rowcount != 1:
                return {"claimed": False, "generation": 0}
            row = conn.execute(
                "SELECT driver_generation FROM workflow_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            return {"claimed": True, "generation": row["driver_generation"] if row else 0}
        finally:
            conn.close()

    def heartbeat_driver_lease(self, run_id: str, owner: str, generation: int, lease_until_iso: str) -> bool:
        """CAS heartbeat：owner/generation 匹配且 lease 尚未过期且 run 可执行时续租。

        lease 已过期 → False（不能复活过期 lease；RunDriver 停止旧 worker）。
        """
        init_workflow_tables()
        _ensure_driver_columns()
        now = _utc_now_iso()
        conn = _get_conn()
        try:
            cur = conn.execute(
                """UPDATE workflow_runs SET driver_lease_until=?, driver_heartbeat_at=?
                   WHERE run_id=? AND driver_owner=? AND driver_generation=?
                     AND driver_lease_until IS NOT NULL AND driver_lease_until >= ?
                     AND status IN ('pending','running')""",
                (lease_until_iso, now, run_id, owner, generation, now),
            )
            conn.commit()
            return cur.rowcount == 1
        finally:
            conn.close()

    def release_driver_lease(self, run_id: str, owner: str, generation: int) -> bool:
        """释放 lease（仅 owner/generation 匹配）。"""
        init_workflow_tables()
        _ensure_driver_columns()
        conn = _get_conn()
        try:
            cur = conn.execute(
                """UPDATE workflow_runs SET driver_owner=NULL, driver_lease_until=NULL
                   WHERE run_id=? AND driver_owner=? AND driver_generation=?""",
                (run_id, owner, generation),
            )
            conn.commit()
            return cur.rowcount == 1
        finally:
            conn.close()

    def is_driver_owner(self, run_id: str, owner: str, generation: int) -> bool:
        """检查当前 owner/generation 是否匹配（identity helper，不检查 lease）。"""
        init_workflow_tables()
        _ensure_driver_columns()
        conn = _get_conn()
        try:
            row = conn.execute(
                "SELECT driver_owner, driver_generation FROM workflow_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
            conn.close()
            return bool(row and row["driver_owner"] == owner and row["driver_generation"] == generation)
        except Exception:
            conn.close()
            return False

    def is_driver_execution_valid(self, run_id: str, owner: str, generation: int) -> bool:
        """driver-managed execution gate：identity + lease 未过期 + status 非 CANCELLED。

        与 is_driver_owner（纯 identity）区分：lease 已过期即使尚未被 takeover，
        旧 worker 也不得继续执行 / dispatch / 写 control state。
        """
        init_workflow_tables()
        _ensure_driver_columns()
        now = _utc_now_iso()
        conn = _get_conn()
        try:
            row = conn.execute(
                "SELECT driver_owner, driver_generation, driver_lease_until, status FROM workflow_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
            conn.close()
            if not row:
                return False
            if row["driver_owner"] != owner or row["driver_generation"] != generation:
                return False
            if row["status"] == WorkflowRunStatus.CANCELLED.value:
                return False
            lease = row["driver_lease_until"]
            if not lease:
                return False
            return lease >= now
        except Exception:
            conn.close()
            return False

    def list_driver_candidates(self, limit: int = 50) -> List["WorkflowRun"]:
        """发现 driver-managed runnable runs（PENDING / RUNNING lease-expired）。"""
        init_workflow_tables()
        _ensure_driver_columns()
        now = _utc_now_iso()
        conn = _get_conn()
        try:
            rows = conn.execute(
                """SELECT * FROM workflow_runs
                   WHERE driver_managed=1
                     AND status IN ('pending','running')
                     AND (driver_lease_until IS NULL OR driver_lease_until < ?)
                   ORDER BY updated_at ASC LIMIT ?""",
                (now, limit),
            ).fetchall()
            conn.close()
            return [self._row_to_run(dict(r)) for r in rows]
        except Exception:
            conn.close()
            raise

    def fenced_update_run(self, run_id: str, owner: str, generation: int,
                          status: str, current_node_id: str, state_dict: Dict[str, Any],
                          started_at: Optional[str] = None,
                          completed_at: Optional[str] = None) -> bool:
        """原子 fenced 控制状态写入（owner/generation + lease 有效 CAS）。rowcount==1 才成功。

        不覆盖 CANCELLED（terminal-preserving）；lease 已过期不得写（expired worker 停写）。
        """
        init_workflow_tables()
        _ensure_driver_columns()
        now = _utc_now_iso()
        conn = _get_conn()
        try:
            cur = conn.execute(
                """UPDATE workflow_runs SET status=?, current_node_id=?, state_json=?, updated_at=?,
                       started_at=CASE WHEN ? IS NULL THEN started_at ELSE ? END,
                       completed_at=CASE WHEN ? IS NULL THEN completed_at ELSE ? END
                   WHERE run_id=? AND driver_owner=? AND driver_generation=? AND status != 'cancelled'
                     AND driver_lease_until IS NOT NULL AND driver_lease_until >= ?""",
                (status, current_node_id, json.dumps(state_dict, ensure_ascii=False),
                 _utc_now_iso(), started_at, started_at, completed_at, completed_at,
                 run_id, owner, generation, now),
            )
            conn.commit()
            return cur.rowcount == 1
        finally:
            conn.close()

    def list_executing_action_records(self, run_id: str) -> List["WorkflowActionRecord"]:
        """列出 dispatch marker 已写但尚无 terminal result 的 Action。"""
        return [
            action for action in self.list_action_records(run_id)
            if action.status in {ActionStatus.RUNNING, ActionStatus.EXECUTING}
        ]

    def recover_action_runtime_invariants(self) -> Dict[str, int]:
        """Repair crash windows without dispatching any side effect.

        * RUNNING/EXECUTING under a cancelled Run becomes UNKNOWN while the
          Run remains terminal.
        * An already UNKNOWN Action under an active Run projects the node/Run
          to PAUSED, including legacy non-driver-managed Runs.
        """
        init_workflow_tables()
        _ensure_driver_columns()
        conn = _get_conn()
        cancelled_markers = 0
        active_projections = 0
        try:
            conn.execute("BEGIN IMMEDIATE")
            stranded = conn.execute(
                """SELECT a.* FROM workflow_action_records a
                   JOIN workflow_runs r ON r.run_id=a.run_id
                   WHERE r.status='cancelled'
                     AND a.status IN ('running','executing')"""
            ).fetchall()
            for action in stranded:
                reason = "run cancelled after dispatch; external outcome unknown"
                changed = conn.execute(
                    """UPDATE workflow_action_records SET
                           status='unknown', error=?, retryable=0,
                           reconciliation_message='reconciliation required before retry',
                           unknown_since=COALESCE(NULLIF(unknown_since, ''), ?)
                       WHERE action_id=? AND status IN ('running','executing')""",
                    (reason, _utc_now_iso(), action["action_id"]),
                )
                if changed.rowcount != 1:
                    continue
                conn.execute(
                    """UPDATE workflow_action_attempts SET status='unknown', error=?
                       WHERE action_id=? AND attempt=?
                         AND status IN ('running','executing')""",
                    (reason, action["action_id"], int(action["attempt"] or 0)),
                )
                _project_unknown_action_to_run_tx(
                    conn,
                    action["action_id"],
                    reason=reason,
                )
                _append_event_tx(
                    conn,
                    action["run_id"],
                    "action_unknown",
                    node_id=action["node_id"],
                    payload={
                        "actionExecutionId": action["action_id"],
                        "workflowRunId": action["run_id"],
                        "eventId": action["event_id"] or None,
                        "actionType": action["action_type"],
                        "attempt": int(action["attempt"] or 0),
                        "reason": reason,
                        "recovery": True,
                        "runCancelled": True,
                    },
                )
                cancelled_markers += 1

            unprojected = conn.execute(
                """SELECT a.* FROM workflow_action_records a
                   JOIN workflow_runs r ON r.run_id=a.run_id
                   WHERE a.status='unknown'
                     AND r.status IN ('pending','running')"""
            ).fetchall()
            for action in unprojected:
                reason = action["error"] or "external outcome unknown; reconciliation required"
                if not action["unknown_since"]:
                    conn.execute(
                        "UPDATE workflow_action_records SET unknown_since=? WHERE action_id=?",
                        (
                            action["finished_at"]
                            or action["completed_at"]
                            or action["started_at"]
                            or action["created_at"]
                            or _utc_now_iso(),
                            action["action_id"],
                        ),
                    )
                projected = _project_unknown_action_to_run_tx(
                    conn,
                    action["action_id"],
                    reason=reason,
                )
                if projected == WorkflowRunStatus.PAUSED.value:
                    _append_event_tx(
                        conn,
                        action["run_id"],
                        "workflow_paused",
                        node_id=action["node_id"],
                        payload={
                            "actionExecutionId": action["action_id"],
                            "reason": "UNKNOWN Action recovered after restart",
                            "recovery": True,
                        },
                    )
                    active_projections += 1
            conn.commit()
            return {
                "cancelledMarkers": cancelled_markers,
                "activeProjections": active_projections,
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ── Phase17 P1: plan discovery ────────────────────────────────────────

    def list_planning_definitions(self, limit: int = 50, offset: int = 0) -> List["WorkflowDefinition"]:
        """列出 planning definitions（metadata 含 planFingerprint marker），分页。"""
        init_workflow_tables()
        conn = _get_conn()
        try:
            rows = conn.execute(
                """SELECT * FROM workflow_definitions
                   WHERE metadata_json LIKE '%"planFingerprint"%'
                   ORDER BY updated_at DESC, id DESC LIMIT ? OFFSET ?""",
                (limit, offset),
            ).fetchall()
            conn.close()
            return [self._row_to_definition(dict(r)) for r in rows]
        except Exception:
            conn.close()
            raise

    def count_planning_definitions(self) -> int:
        init_workflow_tables()
        conn = _get_conn()
        try:
            row = conn.execute(
                "SELECT COUNT(*) as c FROM workflow_definitions WHERE metadata_json LIKE '%\"planFingerprint\"%'"
            ).fetchone()
            conn.close()
            return row["c"] if row else 0
        except Exception:
            conn.close()
            return 0

    def list_planning_definitions_filtered(
        self,
        goal_type: Optional[str] = None,
        status: Optional[str] = None,
        search: Optional[str] = None,
        event_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Tuple[int, List["WorkflowDefinition"]]:
        """SQL 侧过滤 planning definitions（无硬上限）。

        filter（goalType/search/status）在 count 与 pagination 之前生效：
          - goalType / search 用 json_extract 读取 metadata_json 的 frozen plan
          - status 用 latest-run 子查询（updated_at DESC, rowid DESC 取最新）
        返回 (filtered_total, page_definitions)。
        """
        init_workflow_tables()
        conn = _get_conn()
        try:
            where = ['metadata_json LIKE \'%"planFingerprint"%\'']
            params: List[Any] = []
            if goal_type:
                where.append("json_extract(metadata_json, '$.plan.goalType') = ?")
                params.append(goal_type)
            if search:
                where.append(
                    "LOWER(COALESCE(json_extract(metadata_json, '$.plan.goal'), '')) LIKE ?"
                )
                params.append(f"%{search.lower()}%")
            if event_id:
                where.append(
                    "json_valid(metadata_json) AND json_extract(metadata_json, '$.plan.eventId') = ?"
                )
                params.append(event_id)
            if status:
                where.append(
                    """(SELECT r.status FROM workflow_runs r
                         WHERE r.definition_id = d.id
                         ORDER BY r.updated_at DESC, r.rowid DESC LIMIT 1) = ?"""
                )
                params.append(status)
            where_sql = " AND ".join(where)

            total = conn.execute(
                f"SELECT COUNT(*) AS c FROM workflow_definitions d WHERE {where_sql}",
                params,
            ).fetchone()["c"]

            rows = conn.execute(
                f"""SELECT d.* FROM workflow_definitions d WHERE {where_sql}
                    ORDER BY d.updated_at DESC, d.id DESC LIMIT ? OFFSET ?""",
                params + [limit, offset],
            ).fetchall()
            conn.close()
            return total, [self._row_to_definition(dict(r)) for r in rows]
        except Exception:
            conn.close()
            raise

    def batch_get_run_aggregates(self, definition_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """每个 definition_id 的 run 聚合（executionCount/replanCount/latest run summary）。"""
        if not definition_ids:
            return {}
        init_workflow_tables()
        conn = _get_conn()
        try:
            placeholders = ",".join(["?" for _ in definition_ids])
            rows = conn.execute(
                f"SELECT run_id, definition_id, version, status, updated_at, state_json "
                f"FROM workflow_runs WHERE definition_id IN ({placeholders}) ORDER BY updated_at ASC",
                definition_ids,
            ).fetchall()
            conn.close()
        except Exception:
            conn.close()
            return {}
        agg: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            did = row["definition_id"]
            a = agg.setdefault(did, {"executionCount": 0, "replanCount": 0, "latest": None})
            a["executionCount"] += 1
            state = row["state_json"]
            if isinstance(state, str):
                import json as _j
                try:
                    state = _j.loads(state)
                except Exception:
                    state = {}
            if isinstance(state, dict) and state.get("replannedFromRunId"):
                a["replanCount"] += 1
            # latest by updated_at（循环按 ASC，最后一条即最新）
            a["latest"] = {
                "runId": row["run_id"], "version": row["version"],
                "status": row["status"], "updatedAt": row["updated_at"],
                "rootRunId": (state.get("executionLineage") or {}).get("rootRunId") if isinstance(state, dict) else None,
            }
        return agg
