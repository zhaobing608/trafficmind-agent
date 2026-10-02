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
    DefinitionStatus,
    NodeConfig,
    NodeStatus,
    NodeType,
    WorkflowActionRecord,
    WorkflowApproval,
    WorkflowDefinition,
    WorkflowDefinitionVersion,
    WorkflowEvent,
    WorkflowNodeRun,
    WorkflowRun,
    WorkflowRunStatus,
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
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
            completed_at TEXT DEFAULT ''
        );

        CREATE INDEX IF NOT EXISTS idx_wf_runs_session ON workflow_runs(session_id);
        CREATE INDEX IF NOT EXISTS idx_wf_runs_status ON workflow_runs(status);
        CREATE INDEX IF NOT EXISTS idx_wf_node_runs_run ON workflow_node_runs(run_id);
        CREATE INDEX IF NOT EXISTS idx_wf_events_run ON workflow_events(run_id);
        CREATE INDEX IF NOT EXISTS idx_wf_approvals_run ON workflow_approvals(run_id);
        CREATE INDEX IF NOT EXISTS idx_wf_actions_run ON workflow_action_records(run_id);
        CREATE INDEX IF NOT EXISTS idx_wf_actions_idem ON workflow_action_records(idempotency_key);
        CREATE INDEX IF NOT EXISTS idx_wf_versions_def ON workflow_definition_versions(definition_id, version);
    """)
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
            query += " AND CASE WHEN json_valid(state_json) THEN json_extract(state_json, '$.currentEvent.eventId') END=?"
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
            query += " AND CASE WHEN json_valid(state_json) THEN json_extract(state_json, '$.currentEvent.eventId') END=?"
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
                            conn.execute(
                                """UPDATE event_records
                                   SET status='已处置', updatedAt=?
                                   WHERE eventId=?
                                     AND status IN ('待研判','待派单','处置中')""",
                                (_utc_now_iso(), event_id),
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
                payload=dict(payload or {}),
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
            """INSERT INTO workflow_approvals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(approval_id) DO UPDATE SET
                   run_id=excluded.run_id,
                   node_id=excluded.node_id,
                   proposed_actions_json=excluded.proposed_actions_json,
                   edited_actions_json=excluded.edited_actions_json,
                   decision=excluded.decision,
                   reviewer=excluded.reviewer,
                   comment=excluded.comment,
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
                       edited_actions_json=?, decision=?, reviewer=?, comment=?, decided_at=?
                   WHERE approval_id=? AND run_id=? AND decision='pending'""",
                (
                    json.dumps(approval.edited_actions, ensure_ascii=False),
                    approval.decision.value,
                    approval.reviewer,
                    approval.comment,
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
                    """INSERT INTO workflow_approvals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        approval.approval_id,
                        approval.run_id,
                        approval.node_id,
                        json.dumps(approval.proposed_actions, ensure_ascii=False),
                        "[]",
                        ApprovalDecision.PENDING.value,
                        "",
                        "",
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
                       edited_actions_json=?, decision=?, reviewer=?, comment=?, decided_at=?
                   WHERE approval_id=? AND run_id=? AND decision='pending'""",
                (
                    json.dumps(approval.edited_actions, ensure_ascii=False),
                    approval.decision.value,
                    approval.reviewer,
                    approval.comment,
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
            created_at=d.get("created_at", ""),
            decided_at=d.get("decided_at", ""),
        )

    # ── ActionRecord CRUD ────────────────────────────────────────────────

    def save_action_record(self, record: WorkflowActionRecord) -> None:
        init_workflow_tables()
        conn = _get_conn()
        conn.execute(
            """INSERT INTO workflow_action_records VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                   completed_at=excluded.completed_at
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
            ),
        )
        conn.commit()
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

    def _row_to_action_record(self, d: Dict[str, Any]) -> WorkflowActionRecord:
        params = d.get("params_json", "{}")
        result = d.get("result_json", "{}")
        if isinstance(params, str):
            params = json.loads(params) if params else {}
        if isinstance(result, str):
            result = json.loads(result) if result else {}
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
        )

    # ── Batch Read Helpers (Workflow Center V2 Round 1) ──────────────────

    def batch_get_node_counts(
        self, run_ids: List[str]
    ) -> Dict[str, Dict[str, int]]:
        """批量获取每个 Run 的节点执行统计。

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
                result[rid] = {"total": 0, "succeeded": 0, "failed": 0}
            result[rid]["total"] += cnt
            if st in ("succeeded",):
                result[rid]["succeeded"] += cnt
            elif st in ("failed",):
                result[rid]["failed"] += cnt
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
        """列出 run 中 status=EXECUTING 的 action record（dispatch started 但未完成）。"""
        return [a for a in self.list_action_records(run_id) if a.status == ActionStatus.EXECUTING]

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
                    "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.plan.eventId') END = ?"
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
