"""
SQLite 协作持久化 — Phase 9.3
5 tables: collaboration_runs/tasks/messages/conflicts/events
"""
import sqlite3, json, os
from datetime import datetime
from typing import Any, Dict, List, Optional
from backend.config import DB_PATH


def get_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_collaboration_tables():
    conn = get_conn()
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS collaboration_runs (
            run_id TEXT PRIMARY KEY, session_id TEXT, trace_id TEXT,
            status TEXT, protocol_version TEXT DEFAULT '1.0',
            normalized_event TEXT DEFAULT '{}',
            selected_agents TEXT DEFAULT '[]', skipped_agents TEXT DEFAULT '[]',
            failed_agents TEXT DEFAULT '[]', budget_usage TEXT DEFAULT '{}',
            final_decision TEXT DEFAULT '', started_at TEXT, updated_at TEXT, completed_at TEXT,
            previous_run_context TEXT DEFAULT '{}',
            grounding_context TEXT DEFAULT '{}'
        );
        CREATE TABLE IF NOT EXISTS collaboration_tasks (
            task_id TEXT NOT NULL, run_id TEXT NOT NULL,
            agent_name TEXT, task_type TEXT, status TEXT DEFAULT 'pending',
            depends_on TEXT DEFAULT '[]', priority INTEGER DEFAULT 5,
            attempt INTEGER DEFAULT 0, max_retries INTEGER DEFAULT 1,
            timeout_seconds INTEGER DEFAULT 30,
            input_snapshot TEXT DEFAULT '{}', output_snapshot TEXT DEFAULT '{}',
            error_code TEXT DEFAULT '', error_message TEXT DEFAULT '',
            started_at TEXT, completed_at TEXT,
            PRIMARY KEY (run_id, task_id)
        );
        CREATE TABLE IF NOT EXISTS collaboration_messages (
            message_id TEXT PRIMARY KEY, run_id TEXT, trace_id TEXT, task_id TEXT,
            sender TEXT, receiver TEXT, message_type TEXT, phase TEXT DEFAULT '',
            attempt INTEGER DEFAULT 1, payload TEXT DEFAULT '{}',
            evidence_refs TEXT DEFAULT '[]', created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS collaboration_conflicts (
            conflict_id TEXT NOT NULL, run_id TEXT NOT NULL,
            conflict_type TEXT DEFAULT '', field TEXT DEFAULT '',
            participants TEXT DEFAULT '[]', proposals TEXT DEFAULT '[]',
            severity TEXT DEFAULT 'low', status TEXT DEFAULT 'open',
            resolution TEXT DEFAULT '', resolved_by TEXT DEFAULT '',
            requires_human_review INTEGER DEFAULT 0,
            created_at TEXT, resolved_at TEXT,
            PRIMARY KEY (run_id, conflict_id)
        );
        CREATE TABLE IF NOT EXISTS collaboration_events (
            event_id TEXT NOT NULL, run_id TEXT NOT NULL,
            event_type TEXT, payload TEXT DEFAULT '{}',
            sequence_number INTEGER DEFAULT 0, created_at TEXT,
            PRIMARY KEY (run_id, event_id)
        );
        CREATE INDEX IF NOT EXISTS idx_collab_msgs_run ON collaboration_messages(run_id);
        CREATE INDEX IF NOT EXISTS idx_collab_tasks_run ON collaboration_tasks(run_id);
        CREATE INDEX IF NOT EXISTS idx_collab_events_run ON collaboration_events(run_id);
    """)
    conn.commit(); conn.close()


# ===== SQLite Repository =====

class SQLiteCollaborationRepository:
    def save_run(self, state):
        init_collaboration_tables()
        conn = get_conn(); now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        def _json_value(value, default):
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except Exception:
                    return default
            return default if value is None else value

        normalized_event = _json_value(state.get("normalized_event", {}), {})
        selected_agents = _json_value(state.get("selected_agents", []), [])
        skipped_agents = _json_value(state.get("skipped_agents", []), [])
        failed_agents = _json_value(state.get("failed_agents", []), [])
        budget_usage = _json_value(state.get("budget_usage", {}), {})
        raw_final_decision = state.get("final_decision", "")
        final_decision = _json_value(raw_final_decision, raw_final_decision)
        previous_run_context = _json_value(state.get("previous_run_context"), None)
        grounding_context = _json_value(state.get("grounding_context", {}), {})
        # Ensure previous_run_context column exists (non-destructive migration)
        try:
            conn.execute("ALTER TABLE collaboration_runs ADD COLUMN previous_run_context TEXT DEFAULT '{}'")
        except sqlite3.OperationalError:
            pass  # column already exists
        try:
            conn.execute("ALTER TABLE collaboration_runs ADD COLUMN grounding_context TEXT DEFAULT '{}'")
        except sqlite3.OperationalError:
            pass  # column already exists
        conn.execute("""
            INSERT OR REPLACE INTO collaboration_runs (
                run_id, session_id, trace_id, status, protocol_version,
                normalized_event, selected_agents, skipped_agents, failed_agents,
                budget_usage, final_decision, started_at, updated_at, completed_at,
                previous_run_context, grounding_context
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
            (state["run_id"], state.get("session_id",""), state.get("trace_id",""),
             state["status"], state.get("protocol_version","1.0"),
             json.dumps(normalized_event, ensure_ascii=False),
             json.dumps(selected_agents, ensure_ascii=False),
             json.dumps(skipped_agents, ensure_ascii=False),
             json.dumps(failed_agents, ensure_ascii=False),
             json.dumps(budget_usage, ensure_ascii=False),
             json.dumps(final_decision, ensure_ascii=False),
             state.get("started_at",""), now, state.get("completed_at",""),
             json.dumps(previous_run_context, ensure_ascii=False),
             json.dumps(grounding_context, ensure_ascii=False)))
        # Canonical Agent completion and event lifecycle advancement share the
        # same SQLite transaction.  A completed live Agent run therefore cannot
        # be durable while its canonical event is left behind in 待研判.  Replay
        # runs are analysis-only and never mutate the original event lifecycle.
        if (
            state.get("status") in ("completed", "partial_success")
            and isinstance(normalized_event, dict)
            and normalized_event.get("runKind", "live") != "replay"
            and normalized_event.get("actionExecutionAllowed", True) is not False
        ):
            event_id = str(
                normalized_event.get("eventId")
                or normalized_event.get("event_id")
                or ""
            ).strip()
            table_exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_records'"
            ).fetchone()
            if event_id and table_exists:
                conn.execute(
                    """UPDATE event_records SET status='待派单', updatedAt=?
                       WHERE eventId=? AND status='待研判'""",
                    (now, event_id),
                )
        conn.commit(); conn.close()

    def get_run(self, run_id: str) -> Optional[Dict]:
        init_collaboration_tables(); conn = get_conn()
        row = conn.execute("SELECT * FROM collaboration_runs WHERE run_id=?",(run_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def update_run(self, state): self.save_run(state)

    def save_message(self, msg: Dict):
        init_collaboration_tables(); conn = get_conn()
        try:
            conn.execute("""INSERT OR IGNORE INTO collaboration_messages VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (msg["message_id"], msg.get("run_id",""), msg.get("trace_id",""), msg.get("task_id",""),
                 msg.get("sender",""), msg.get("receiver",""), msg.get("message_type",""),
                 msg.get("phase",""), msg.get("attempt",1),
                 json.dumps(msg.get("payload",{}), ensure_ascii=False),
                 json.dumps(msg.get("evidence_refs",[]), ensure_ascii=False),
                 msg.get("created_at",datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))))
            conn.commit()
        except sqlite3.IntegrityError: pass
        finally: conn.close()

    def list_messages(self, run_id: str) -> List[Dict]:
        init_collaboration_tables(); conn = get_conn()
        rows = conn.execute("SELECT * FROM collaboration_messages WHERE run_id=? ORDER BY created_at",(run_id,)).fetchall()
        conn.close(); return [dict(r) for r in rows]

    def save_task(self, run_id: str, task_dict: Dict):
        init_collaboration_tables(); conn = get_conn(); now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        conn.execute("""INSERT OR REPLACE INTO collaboration_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (task_dict.get("task_id",""), run_id, task_dict.get("agent_name",""),
             task_dict.get("task_type",""), task_dict.get("status","pending"),
             json.dumps(task_dict.get("depends_on",[]), ensure_ascii=False),
             task_dict.get("priority",5), task_dict.get("attempt",0),
             task_dict.get("max_retries",1), task_dict.get("timeout_seconds",30),
             json.dumps(task_dict.get("input_snapshot",{}), ensure_ascii=False),
             json.dumps(task_dict.get("output_snapshot",{}), ensure_ascii=False),
             task_dict.get("error_code",""), task_dict.get("error_message",""),
             now, task_dict.get("completed_at","")))
        conn.commit(); conn.close()

    def update_task(self, run_id: str, task_dict: Dict): self.save_task(run_id, task_dict)

    def list_tasks(self, run_id: str) -> List[Dict]:
        init_collaboration_tables(); conn = get_conn()
        rows = conn.execute("SELECT * FROM collaboration_tasks WHERE run_id=? ORDER BY started_at, task_id", (run_id,)).fetchall()
        conn.close(); return [dict(r) for r in rows]

    def get_session_bound_event_ids(self, session_id: str) -> List[str]:
        """Return distinct persisted eventId bindings for a collaboration session."""
        if not session_id:
            return []
        init_collaboration_tables(); conn = get_conn()
        rows = conn.execute(
            "SELECT normalized_event FROM collaboration_runs WHERE session_id=?",
            (session_id,),
        ).fetchall()
        conn.close()
        event_ids = []
        for row in rows:
            raw = row["normalized_event"] if isinstance(row, sqlite3.Row) else row[0]
            try:
                data = json.loads(raw or "{}") if isinstance(raw, str) else (raw or {})
            except Exception:
                data = {}
            event_id = str(data.get("eventId") or data.get("event_id") or "").strip()
            if event_id and event_id not in event_ids:
                event_ids.append(event_id)
        return event_ids

    def list_runs_by_event_id(self, event_id: str, limit: int = 20, offset: int = 0) -> List[Dict]:
        """Return collaboration runs with an exact persisted normalized_event.eventId binding."""
        event_id = str(event_id or "").strip()
        if not event_id:
            return []
        init_collaboration_tables(); conn = get_conn()
        rows = conn.execute(
            """
            SELECT * FROM collaboration_runs
            WHERE CASE WHEN json_valid(normalized_event)
                THEN json_extract(normalized_event, '$.eventId')
            END = ?
            ORDER BY updated_at DESC, started_at DESC, run_id DESC
            LIMIT ? OFFSET ?
            """,
            (event_id, limit, offset),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def count_runs_by_event_id(self, event_id: str) -> int:
        """Count collaboration runs with an exact persisted normalized_event.eventId binding."""
        event_id = str(event_id or "").strip()
        if not event_id:
            return 0
        init_collaboration_tables(); conn = get_conn()
        row = conn.execute(
            """
            SELECT COUNT(*) AS c FROM collaboration_runs
            WHERE CASE WHEN json_valid(normalized_event)
                THEN json_extract(normalized_event, '$.eventId')
            END = ?
            """,
            (event_id,),
        ).fetchone()
        conn.close()
        return row["c"] if row else 0

    def save_conflict(self, conflict: Dict):
        init_collaboration_tables(); conn = get_conn()
        conn.execute("""INSERT OR REPLACE INTO collaboration_conflicts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (conflict["conflict_id"], conflict.get("run_id",""), conflict.get("type",""),
             conflict.get("field",""), json.dumps(conflict.get("participants",[]), ensure_ascii=False),
             json.dumps(conflict.get("proposals",[]), ensure_ascii=False),
             conflict.get("severity","low"), conflict.get("status","open"),
             conflict.get("resolution",""), conflict.get("resolved_by",""),
             int(conflict.get("requires_human_review", False)),
             conflict.get("created_at", datetime.now().strftime("%Y-%m-%dT%H:%M:%S")),
             conflict.get("resolved_at","")))
        conn.commit(); conn.close()

    def list_conflicts(self, run_id: str) -> List[Dict]:
        init_collaboration_tables(); conn = get_conn()
        rows = conn.execute("SELECT * FROM collaboration_conflicts WHERE run_id=?",(run_id,)).fetchall()
        conn.close(); return [dict(r) for r in rows]

    def save_event(self, run_id: str, event: Dict, seq: int):
        init_collaboration_tables(); conn = get_conn()
        conn.execute("""INSERT OR REPLACE INTO collaboration_events VALUES (?,?,?,?,?,?)""",
            (event.get("event_id",f"evt_{seq}"), run_id, event.get("event_type",""),
             json.dumps(event, ensure_ascii=False), seq, datetime.now().strftime("%Y-%m-%dT%H:%M:%S")))
        conn.commit(); conn.close()

    def list_events(self, run_id: str) -> List[Dict]:
        init_collaboration_tables(); conn = get_conn()
        rows = conn.execute("SELECT * FROM collaboration_events WHERE run_id=? ORDER BY sequence_number",(run_id,)).fetchall()
        conn.close(); return [dict(r) for r in rows]


def load_previous_run_context(session_id: str) -> Optional[Dict[str, Any]]:
    """
    从 SQLite 加载会话上一次运行的上下文摘要。
    只返回摘要和关键字段，不返回完整事件——确保 currentEvent 完全独立。

    Returns:
        None (no previous run) 或 {
            "runId": str,
            "summary": str,
            "status": str,
            "event": {   # ONLY stable + key fields for context
                "avgSpeed": float|None,
                "queueLength": float|None,
                "roadName": str,
                "eventTypeCn": str,
                "nearbySchool": bool,
                "nearbyHospital": bool,
                "isMainRoad": bool,
            },
            "updatedAt": str,
        }
    """
    init_collaboration_tables()
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM collaboration_runs WHERE session_id=? "
            "ORDER BY updated_at DESC LIMIT 1",
            (session_id,)
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        # Parse JSON fields
        import json as _json
        normalized_event = {}
        try:
            normalized_event = _json.loads(d.get("normalized_event", "{}"))
        except Exception:
            pass
        final_decision = d.get("final_decision", "")
        # Extract a brief summary from final_decision
        summary = ""
        if isinstance(final_decision, str) and final_decision:
            summary = final_decision[:200]
        elif isinstance(final_decision, dict):
            summary = str(final_decision.get("fusionSummary", ""))[:200]
        elif isinstance(normalized_event, dict):
            summary = f"{normalized_event.get('roadName', '')}{normalized_event.get('eventTypeCn', '')}研判"
        return {
            "runId": d.get("run_id", ""),
            "summary": summary or f"{normalized_event.get('roadName', '')}研判",
            "status": d.get("status", ""),
            "event": {
                "avgSpeed": normalized_event.get("avgSpeed"),
                "queueLength": normalized_event.get("queueLength"),
                "roadName": normalized_event.get("roadName", ""),
                "eventTypeCn": normalized_event.get("eventTypeCn", ""),
                "nearbySchool": normalized_event.get("nearbySchool", False),
                "nearbyHospital": normalized_event.get("nearbyHospital", False),
                "isMainRoad": normalized_event.get("isMainRoad", False),
            },
            "updatedAt": d.get("updated_at", ""),
        }
    finally:
        conn.close()
