"""
Workflow V1 Wait Scheduler — Phase 12

后台定时扫描器：恢复到期的 waiting Workflow Run。

设计：
  - FastAPI lifespan 启动/停止
  - 固定周期扫描已到期但尚未恢复的 waiting runs
  - DB 级别的 claim/lease 避免多 worker 重复恢复
  - claim 通过 UPDATE ... WHERE wait_status='waiting' AND wake_at <= now 实现
  - 恢复后更新 wait_status='resumed' + resumed_at
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import time
import uuid
from datetime import datetime, timezone

import backend.config as _config

from backend.workflow.models import WorkflowRunStatus


# ── 配置 ─────────────────────────────────────────────────────────────────

# 扫描周期（秒）。测试环境可用短周期；生产环境建议 5-15 秒。
_WAIT_SCAN_INTERVAL_SECONDS = float(
    os.getenv("WORKFLOW_WAIT_SCAN_INTERVAL", "5")
)

# claim lease 超时（秒）。防止崩溃后 claim 被永久占用。
_CLAIM_LEASE_SECONDS = 120


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_now_unix() -> float:
    return datetime.now(timezone.utc).timestamp()


# ── Scheduler ────────────────────────────────────────────────────────────


class WaitScheduler:
    """Workflow Wait 后台调度器。

    在 FastAPI lifespan 中作为 asyncio.Task 运行。
    安全停止：设置 _stop_event，等待当前扫描完成。
    """

    def __init__(self):
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    async def start(self):
        """启动后台扫描循环。"""
        if self._running:
            return
        self._running = True
        # A process-level singleton can be restarted by a new ASGI event loop;
        # never reuse the loop-bound Event from a previous lifespan.
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._scan_loop())
        print(f"[WaitScheduler] 启动，扫描间隔 {_WAIT_SCAN_INTERVAL_SECONDS}s")

    async def stop(self):
        """安全停止。"""
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            finally:
                self._task = None
        print("[WaitScheduler] 已停止")

    async def _scan_loop(self):
        """主扫描循环。"""
        while not self._stop_event.is_set():
            try:
                await self._scan_once()
            except Exception as e:
                print(f"[WaitScheduler] 扫描异常: {e}")

            # 等待下一次扫描或停止信号
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=_WAIT_SCAN_INTERVAL_SECONDS,
                )
                # stop_event 被设置 → 退出
                break
            except asyncio.TimeoutError:
                # 超时 → 正常进行下一次扫描
                continue

    async def _scan_once(self):
        """执行一次扫描：找到到期 waiting runs 并恢复。"""
        conn = _get_conn()
        try:
            now_iso = _utc_now_iso()
            now_unix = _utc_now_unix()
            claim_token = f"wait_claim_{uuid.uuid4().hex}"

            # ── Claim 到期 runs ──────────────────────────────────────
            # 原子 UPDATE：将 waiting 状态改为 claimed，设置 claim 时间
            cursor = conn.execute(
                """UPDATE workflow_runs
                   SET status = 'claimed',
                       updated_at = ?,
                       resume_reason = ?
                   WHERE status = 'paused'
                     AND wait_type IS NOT NULL
                     AND wait_type != ''
                     AND wake_at IS NOT NULL
                     AND wake_at <= ?""",
                (now_iso, claim_token, now_iso),
            )
            conn.commit()
            claimed_count = cursor.rowcount

            if claimed_count > 0:
                print(f"[WaitScheduler] claim 到 {claimed_count} 个到期等待 Run")

            # ── 获取已 claim 的 runs ─────────────────────────────────
            rows = conn.execute(
                """SELECT run_id FROM workflow_runs
                   WHERE status = 'claimed' AND resume_reason = ?
                   ORDER BY wake_at ASC
                   LIMIT 50""",
                (claim_token,),
            ).fetchall()

            for row in rows:
                run_id = row["run_id"]
                try:
                    print(f"[WaitScheduler] Resuming {run_id}...")
                    await self._resume_waiting_run(
                        run_id,
                        require_claimed=True,
                        claim_token=claim_token,
                    )
                    print(f"[WaitScheduler] Resumed {run_id} OK")
                except Exception as e:
                    import traceback
                    print(f"[WaitScheduler] 恢复 {run_id} 失败: {e}")
                    traceback.print_exc()
                    # 标记为 failed 避免死循环
                    conn.execute(
                        """UPDATE workflow_runs
                           SET status = 'failed',
                               updated_at = ?
                           WHERE run_id = ? AND status = 'claimed'""",
                        (now_iso, run_id),
                    )
                    conn.commit()

            # ── 释放超时的 claim ──────────────────────────────────────
            # 防止崩溃后 claim 永久占用
            lease_deadline = datetime.now(timezone.utc).timestamp() - _CLAIM_LEASE_SECONDS
            deadline_iso = datetime.fromtimestamp(lease_deadline, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            conn.execute(
                """UPDATE workflow_runs
                   SET status = 'paused',
                       updated_at = ?
                   WHERE status = 'claimed'
                     AND updated_at < ?""",
                (now_iso, deadline_iso),
            )
            conn.commit()

        finally:
            conn.close()

    async def _resume_waiting_run(
        self,
        run_id: str,
        *,
        require_claimed: bool = False,
        claim_token: str = "",
    ):
        """恢复一个到期等待的 Run。

        注入已等待标记到 state，使 wait 节点识别为恢复场景直接跳过。
        """
        from backend.workflow.repository import SQLiteWorkflowRepository
        from backend.workflow.executor import WorkflowExecutor
        from backend.workflow.state import TrafficWorkflowState

        repo = SQLiteWorkflowRepository()
        run = repo.get_run(run_id)
        if run is None:
            return

        if run.status.value not in ("paused", "claimed"):
            return

        # ── 注入已等待标记到 state ──────────────────────────────────
        state = TrafficWorkflowState.from_dict(
            run.state if isinstance(run.state, dict) else {}
        )
        # 标记当前 wait 节点已完成等待（使 execute_wait 识别恢复场景）
        wait_node_id = state.current_node
        if not hasattr(state, '_node_outputs'):
            state.node_outputs = {}
        state.node_outputs[wait_node_id] = {
            "wait_skipped": True, "status": "already_waited",
            "resumed_by": "scheduler", "resumed_at": _utc_now_iso(),
        }
        source_status = "claimed" if require_claimed else "paused"
        persisted_state = run.state if isinstance(run.state, dict) else {}
        from backend.planning.budget import LINEAGE_KEY

        # ── 恢复运行 ──────────────────────────────────────────────
        # Phase17 Round3: planning driver-managed run → wake-only（PENDING + release lease），
        # 由 RunDriver pickup + execute；不在此 request 内长期执行。
        if repo.is_driver_managed(run_id):
            state.status = WorkflowRunStatus("pending")
            next_state = state.to_dict()
            if persisted_state.get(LINEAGE_KEY):
                next_state[LINEAGE_KEY] = persisted_state[LINEAGE_KEY]
            updated = repo.set_run_status_managed(
                run_id,
                "pending",
                next_state,
                expected_status=source_status,
                current_node_id=state.current_node,
                ensure_driver_managed=True,
                expected_resume_reason=claim_token if require_claimed else None,
            )
            if not updated:
                return
            return

        # Legacy runs still resume in-process, but the scheduler first performs
        # a status CAS.  A concurrent cancellation therefore wins and no call
        # to executor.resume() is made.
        state.status = WorkflowRunStatus("paused")
        next_state = state.to_dict()
        if persisted_state.get(LINEAGE_KEY):
            next_state[LINEAGE_KEY] = persisted_state[LINEAGE_KEY]
        updated = repo.set_run_status_managed(
            run_id,
            "paused",
            next_state,
            expected_status=source_status,
            current_node_id=state.current_node,
            expected_resume_reason=claim_token if require_claimed else None,
        )
        if not updated:
            return
        executor = WorkflowExecutor(repo)
        async for _ in executor.resume(run_id):
            pass  # SSE 事件通过 executor 持久化到 Event 表


# ── 数据库列迁移 ─────────────────────────────────────────────────────────


def _migrate_wait_columns():
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
                pass  # 列已存在
        conn.commit()
    finally:
        conn.close()


def _get_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(_config.DB_PATH), exist_ok=True)
    conn = sqlite3.connect(_config.DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── 全局单例 ────────────────────────────────────────────────────────────

_scheduler: WaitScheduler | None = None


def get_wait_scheduler() -> WaitScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = WaitScheduler()
    return _scheduler
