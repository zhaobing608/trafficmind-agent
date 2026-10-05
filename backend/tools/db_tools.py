"""
数据库工具模块
------------
使用 SQLite 持久化存储事件分析结果。
表结构：event_records

第二阶段扩展字段：
  avgSpeed, queueLength, duration, weather, timePeriod,
  isMainRoad, nearbySchool, nearbyHospital
  通过 ALTER TABLE 自动迁移，兼容旧数据库。
"""

import sqlite3
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from backend.config import DB_PATH
from backend.tools.event_tools import safe_float


def get_connection() -> sqlite3.Connection:
    """获取数据库连接（自动创建目录）。"""
    import os
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # 让查询结果支持按列名访问
    return conn


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# 第二阶段新增字段迁移列表
_MIGRATION_COLUMNS = [
    ("avgSpeed", "REAL DEFAULT 0"),
    ("queueLength", "REAL DEFAULT 0"),
    ("duration", "REAL DEFAULT 0"),
    ("weather", "TEXT DEFAULT 'clear'"),
    ("timePeriod", "TEXT DEFAULT 'off_peak'"),
    ("isMainRoad", "INTEGER DEFAULT 0"),
    ("nearbySchool", "INTEGER DEFAULT 0"),
    ("nearbyHospital", "INTEGER DEFAULT 0"),
    # Phase 21: canonical ingestion identity and audit metadata.  These are
    # additive columns so databases created by earlier phases remain valid.
    ("source", "TEXT DEFAULT 'legacy'"),
    ("sourceEventId", "TEXT DEFAULT ''"),
    ("occurredAt", "TEXT DEFAULT ''"),
    ("receivedAt", "TEXT DEFAULT ''"),
    ("lastReceivedAt", "TEXT DEFAULT ''"),
    ("sourceMetadata", "TEXT DEFAULT '{}'"),
    ("payloadFingerprint", "TEXT DEFAULT ''"),
    ("revision", "INTEGER DEFAULT 1"),
]


def _migrate_schema(cursor):
    """兼容迁移：逐个尝试添加新字段，已存在的跳过。"""
    for col_name, col_def in _MIGRATION_COLUMNS:
        try:
            cursor.execute(f"ALTER TABLE event_records ADD COLUMN {col_name} {col_def}")
        except sqlite3.OperationalError:
            pass  # 字段已存在，跳过


def init_db() -> None:
    """初始化数据库表结构，并执行兼容迁移。"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS event_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            eventId TEXT UNIQUE NOT NULL,
            eventType TEXT NOT NULL,
            eventTypeCn TEXT NOT NULL,
            roadName TEXT NOT NULL,
            direction TEXT DEFAULT '',
            riskScore INTEGER NOT NULL,
            riskLevel TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT '待派单',
            report TEXT DEFAULT '',
            rawEvent TEXT DEFAULT '{}',
            fullResult TEXT DEFAULT '{}',
            createdAt TEXT NOT NULL,
            updatedAt TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS event_ingestion_audit (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            source TEXT DEFAULT '',
            source_event_id TEXT DEFAULT '',
            outcome TEXT NOT NULL,
            revision INTEGER DEFAULT 1,
            occurred_at TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS event_lifecycle_audit (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            previous_status TEXT DEFAULT '',
            status TEXT DEFAULT '',
            actor TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    _migrate_schema(cursor)
    # Empty sourceEventId values are legacy rows and deliberately excluded.
    # Real ingested events are uniquely identified by their upstream source.
    cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_event_records_source_identity
        ON event_records(source, sourceEventId)
        WHERE sourceEventId IS NOT NULL AND sourceEventId <> ''
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_event_ingestion_event_sequence
        ON event_ingestion_audit(event_id, sequence)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_event_ingestion_outcome_created
        ON event_ingestion_audit(outcome, created_at)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_event_lifecycle_event_sequence
        ON event_lifecycle_audit(event_id, sequence)
    """)
    conn.commit()
    conn.close()


def save_event_analysis(result: Dict[str, Any]) -> bool:
    """
    保存事件分析结果到数据库。

    Args:
        result: 完整的分析结果字典（与 /analyze_event 返回结构一致）

    Returns:
        是否保存成功
    """
    conn: Optional[sqlite3.Connection] = None
    try:
        init_db()  # 确保表存在 + 迁移
        conn = get_connection()
        cursor = conn.cursor()
        now = _utc_now_iso()

        standard_event = result.get("standardEvent", {})
        event_id = result.get("eventId", standard_event.get("eventId", ""))
        next_status = result.get("status", "待派单")

        # Read and write under one lock so a legacy analysis upsert cannot
        # change Event status without the matching durable lifecycle fact.
        cursor.execute("BEGIN IMMEDIATE")
        previous = cursor.execute(
            "SELECT status, sourceEventId FROM event_records WHERE eventId=?",
            (event_id,),
        ).fetchone()

        cursor.execute("""
            INSERT INTO event_records
                (eventId, eventType, eventTypeCn, roadName, direction,
                 avgSpeed, queueLength, duration,
                 weather, timePeriod, isMainRoad, nearbySchool, nearbyHospital,
                 riskScore, riskLevel, status, report,
                 rawEvent, fullResult, createdAt, updatedAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(eventId) DO UPDATE SET
                eventType=excluded.eventType,
                eventTypeCn=excluded.eventTypeCn,
                roadName=excluded.roadName,
                direction=excluded.direction,
                avgSpeed=excluded.avgSpeed,
                queueLength=excluded.queueLength,
                duration=excluded.duration,
                weather=excluded.weather,
                timePeriod=excluded.timePeriod,
                isMainRoad=excluded.isMainRoad,
                nearbySchool=excluded.nearbySchool,
                nearbyHospital=excluded.nearbyHospital,
                riskScore=excluded.riskScore,
                riskLevel=excluded.riskLevel,
                status=CASE
                    -- A canonical ingestion row owns a durable lifecycle.  A
                    -- delayed analysis result may refresh its payload/risk,
                    -- but must not move an already-advanced event backwards.
                    -- The runtime advances these rows explicitly through
                    -- compare-and-set transitions in ``advance_event_status``.
                    WHEN COALESCE(event_records.sourceEventId, '') <> ''
                        THEN event_records.status
                    ELSE excluded.status
                END,
                report=excluded.report,
                rawEvent=excluded.rawEvent,
                fullResult=excluded.fullResult,
                updatedAt=excluded.updatedAt
        """, (
            event_id,
            standard_event.get("eventType", ""),
            standard_event.get("eventTypeCn", ""),
            standard_event.get("roadName", ""),
            standard_event.get("direction", ""),
            safe_float(standard_event.get("avgSpeed"), 0.0),
            safe_float(standard_event.get("queueLength"), 0.0),
            safe_float(standard_event.get("duration"), 0.0),
            standard_event.get("weather", "clear"),
            standard_event.get("timePeriod", "off_peak"),
            1 if standard_event.get("isMainRoad") else 0,
            1 if standard_event.get("nearbySchool") else 0,
            1 if standard_event.get("nearbyHospital") else 0,
            result.get("riskScore", 0),
            result.get("riskLevel", ""),
            next_status,
            result.get("report", ""),
            json.dumps(standard_event, ensure_ascii=False),
            json.dumps(result, ensure_ascii=False),
            result.get("analyzedAt", now),
            now,
        ))
        if (
            previous is not None
            and not str(previous["sourceEventId"] or "").strip()
            and str(previous["status"] or "") != str(next_status or "")
        ):
            cursor.execute(
                """INSERT INTO event_lifecycle_audit (
                       event_id, event_type, previous_status, status, actor,
                       created_at
                   ) VALUES (?, 'event_status_updated', ?, ?,
                             'analysis_upsert', ?)""",
                (
                    event_id,
                    str(previous["status"] or ""),
                    str(next_status or ""),
                    now,
                ),
            )
        conn.commit()
        return True
    except Exception as e:
        if conn is not None:
            conn.rollback()
        print(f"[DB] 保存失败: {e}")
        return False
    finally:
        if conn is not None:
            conn.close()


def get_history(limit: int = 50) -> List[Dict[str, Any]]:
    """
    查询历史事件分析记录。

    Args:
        limit: 最大返回条数

    Returns:
        历史记录列表
    """
    init_db()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT eventId, eventType, eventTypeCn, roadName, riskScore, "
        "riskLevel, status, createdAt, updatedAt, rawEvent, "
        "source, sourceEventId, occurredAt, receivedAt, lastReceivedAt, revision "
        "FROM event_records ORDER BY updatedAt DESC LIMIT ?",
        (limit,),
    )
    rows = cursor.fetchall()
    conn.close()
    records = []
    for row in rows:
        record = dict(row)
        raw_event = record.pop("rawEvent", None)
        try:
            event = json.loads(raw_event) if isinstance(raw_event, str) else raw_event
        except json.JSONDecodeError:
            event = None
        provenance = event.get("provenance") if isinstance(event, dict) else None
        if isinstance(provenance, dict):
            for key in ("sourceType", "datasetId", "datasetReality"):
                value = provenance.get(key)
                if value is not None:
                    record[key] = value
        records.append(record)
    return records


def get_event_by_id(event_id: str) -> Optional[Dict[str, Any]]:
    """
    根据 eventId 查询单条事件详情。

    Args:
        event_id: 事件编号

    Returns:
        事件详情字典，不存在则返回 None
    """
    init_db()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM event_records WHERE eventId = ?",
        (event_id,),
    )
    row = cursor.fetchone()
    conn.close()
    if row is None:
        return None

    record = dict(row)
    # 将 JSON 字符串还原为对象
    for field in ("rawEvent", "fullResult", "sourceMetadata"):
        if field in record and isinstance(record[field], str):
            try:
                record[field] = json.loads(record[field])
            except json.JSONDecodeError:
                pass
    return record


def update_event_status(event_id: str, status: str) -> bool:
    """
    更新事件状态。

    Args:
        event_id: 事件编号
        status: 新状态（待研判/待派单/处置中/已处置/待复盘/已归档）

    Returns:
        是否更新成功
    """
    from backend.config import EVENT_STATUSES

    if status not in EVENT_STATUSES:
        return False

    init_db()
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            "SELECT status FROM event_records WHERE eventId=?",
            (event_id,),
        ).fetchone()
        if existing is None:
            conn.rollback()
            return False
        now = _utc_now_iso()
        cursor = conn.execute(
            "UPDATE event_records SET status = ?, updatedAt = ? WHERE eventId = ?",
            (status, now, event_id),
        )
        if cursor.rowcount == 1 and str(existing["status"] or "") != status:
            conn.execute(
                """INSERT INTO event_lifecycle_audit (
                       event_id, event_type, previous_status, status, actor, created_at
                   ) VALUES (?, 'event_status_updated', ?, ?, 'status_api', ?)""",
                (event_id, str(existing["status"] or ""), status, now),
            )
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


def advance_event_status(
    event_id: str,
    status: str,
    *,
    allowed_from: Optional[List[str]] = None,
) -> bool:
    """Advance a canonical event lifecycle without overwriting newer truth.

    ``allowed_from`` makes runtime callbacks compare-and-set operations.  A
    delayed Agent/Workflow callback therefore cannot regress an event that has
    already moved further through the existing Chinese business lifecycle.
    """
    from backend.config import EVENT_STATUSES

    if status not in EVENT_STATUSES:
        return False
    init_db()
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            "SELECT status FROM event_records WHERE eventId=?",
            (event_id,),
        ).fetchone()
        if existing is None:
            conn.rollback()
            return False
        previous = str(existing["status"] or "")
        if allowed_from and previous not in allowed_from:
            conn.rollback()
            return False
        now = _utc_now_iso()
        cursor = conn.execute(
            """UPDATE event_records SET status=?, updatedAt=?
               WHERE eventId=? AND status=?""",
            (status, now, event_id, previous),
        )
        if cursor.rowcount == 1 and previous != status:
            conn.execute(
                """INSERT INTO event_lifecycle_audit (
                       event_id, event_type, previous_status, status, actor, created_at
                   ) VALUES (?, 'event_status_updated', ?, ?, 'runtime', ?)""",
                (event_id, previous, status, now),
            )
        conn.commit()
        return cursor.rowcount == 1
    finally:
        conn.close()


def get_stats() -> Dict[str, Any]:
    """
    获取仪表盘统计数据。

    Returns:
        {
            "totalEvents": int,
            "highRiskCount": int,
            "avgRiskScore": float,
            "pendingDispatch": int,
            "riskDistribution": [{"level": str, "count": int}, ...],
            "eventTypeDistribution": [{"type": str, "count": int}, ...],
            "statusDistribution": [{"status": str, "count": int}, ...],
            "dailyTrend": [{"date": str, "count": int}, ...],
        }
    """
    init_db()
    conn = get_connection()
    cursor = conn.cursor()

    # 总事件数
    cursor.execute("SELECT COUNT(*) FROM event_records")
    total_events = cursor.fetchone()[0]

    # 高风险及以上事件数
    cursor.execute(
        "SELECT COUNT(*) FROM event_records WHERE riskLevel IN ('高风险', '重大风险')"
    )
    high_risk_count = cursor.fetchone()[0]

    # 平均风险分数
    cursor.execute("SELECT AVG(riskScore) FROM event_records")
    avg_row = cursor.fetchone()
    avg_risk_score = round(avg_row[0], 1) if avg_row[0] else 0.0

    # 待派单数
    cursor.execute("SELECT COUNT(*) FROM event_records WHERE status = '待派单'")
    pending_dispatch = cursor.fetchone()[0]

    # 风险等级分布
    cursor.execute(
        "SELECT riskLevel, COUNT(*) as cnt FROM event_records GROUP BY riskLevel ORDER BY cnt DESC"
    )
    risk_distribution = [{"level": row["riskLevel"], "count": row["cnt"]} for row in cursor.fetchall()]

    # 事件类型分布
    cursor.execute(
        "SELECT eventTypeCn as type, COUNT(*) as cnt FROM event_records GROUP BY eventTypeCn ORDER BY cnt DESC"
    )
    event_type_distribution = [{"type": row["type"], "count": row["cnt"]} for row in cursor.fetchall()]

    # 状态分布
    cursor.execute(
        "SELECT status, COUNT(*) as cnt FROM event_records GROUP BY status ORDER BY cnt DESC"
    )
    status_distribution = [{"status": row["status"], "count": row["cnt"]} for row in cursor.fetchall()]

    # 近 7 天每日趋势
    cursor.execute("""
        SELECT DATE(createdAt) as date, COUNT(*) as cnt
        FROM event_records
        WHERE createdAt >= DATE('now', '-6 days')
        GROUP BY DATE(createdAt)
        ORDER BY date ASC
    """)
    daily_trend = [{"date": row["date"], "count": row["cnt"]} for row in cursor.fetchall()]

    conn.close()

    return {
        "totalEvents": total_events,
        "highRiskCount": high_risk_count,
        "avgRiskScore": avg_risk_score,
        "pendingDispatch": pending_dispatch,
        "riskDistribution": risk_distribution,
        "eventTypeDistribution": event_type_distribution,
        "statusDistribution": status_distribution,
        "dailyTrend": daily_trend,
    }


def get_all_events_for_similarity() -> List[Dict[str, Any]]:
    """
    获取所有事件的完整信息，用于相似度检索。

    Returns:
        包含所有字段的事件列表（rawEvent 和 fullResult 也解析为对象）
    """
    init_db()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM event_records ORDER BY createdAt DESC")
    rows = cursor.fetchall()
    conn.close()

    events = []
    for row in rows:
        event = dict(row)
        # 将 JSON 字符串还原为对象
        for field in ("rawEvent", "fullResult"):
            if field in event and isinstance(event[field], str):
                try:
                    event[field] = json.loads(event[field])
                except json.JSONDecodeError:
                    pass
        events.append(event)
    return events
