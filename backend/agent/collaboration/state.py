"""
共享运行状态 — Phase 9.1
多 Agent 协作的单一运行实例状态管理。
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

VALID_STATUSES = {
    "created", "routing", "running", "arbitrating",
    "fusing", "completed", "partial_success", "failed", "requires_human_review",
    "interrupted",
}

INTERRUPTIBLE_STATUSES = {"routing", "running", "arbitrating", "fusing"}
TERMINAL_STATUSES = {"completed", "partial_success", "failed", "requires_human_review", "interrupted"}

VALID_TRANSITIONS = {
    "created": {"routing"},
    "routing": {"running", "failed", "interrupted"},
    "running": {"arbitrating", "fusing", "partial_success", "failed", "interrupted", "completed"},
    "arbitrating": {"fusing", "requires_human_review", "failed", "interrupted"},
    "fusing": {"completed", "partial_success", "failed", "interrupted"},
}


class CollaborationRunState:
    """一次多 Agent 协作运行的完整状态。"""

    def __init__(self, run_id: str, session_id: str, trace_id: str = ""):
        self.run_id = run_id
        self.session_id = session_id
        self.trace_id = trace_id
        self.status: str = "created"

        # Input — currentEvent (当前消息解析结果，不含上一轮数据)
        self.original_input: Dict[str, Any] = {}
        self.normalized_event: Dict[str, Any] = {}

        # Previous run context — 独立存储，不合并到 normalized_event
        self.previous_run_context: Optional[Dict[str, Any]] = None

        # Analysis-time grounding snapshot — shared by all Agents in this run.
        self.grounding_context: Dict[str, Any] = {}

        # Routing
        self.selected_agents: List[str] = []
        self.skipped_agents: List[str] = []

        # Execution
        self.task_graph: Dict[str, Any] = {}
        self.task_results: Dict[str, Any] = {}
        self.evidence_pool: List[Dict[str, Any]] = []
        self.conflicts: List[Dict[str, Any]] = []
        self.arbitration_results: List[Dict[str, Any]] = []

        # Result
        self.final_decision: str = ""

        # Error tracking
        self.failed_agents: List[str] = []
        self.retry_counts: Dict[str, int] = {}

        # Budget
        self.execution_budget: Dict[str, int] = {"max_agents": 6, "max_retries": 2, "timeout_seconds": 120}
        self.budget_usage: Dict[str, Any] = {}

        # Timestamps
        self.started_at: str = ""
        self.updated_at: str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.completed_at: str = ""

    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def transition(self, new_status: str):
        if new_status not in VALID_STATUSES:
            raise ValueError(f"非法状态 '{new_status}'。合法值: {sorted(VALID_STATUSES)}")
        if self.status in TERMINAL_STATUSES:
            raise ValueError(f"当前状态 '{self.status}' 为终止状态，不能再转换")
        allowed = VALID_TRANSITIONS.get(self.status, set())
        if new_status not in allowed:
            raise ValueError(f"非法状态转换: '{self.status}' → '{new_status}'。允许: {sorted(allowed)}")
        self.status = new_status
        self.updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if new_status == "running" and not self.started_at:
            self.started_at = self.updated_at
        if new_status in TERMINAL_STATUSES:
            self.completed_at = self.updated_at

    def record_agent_result(self, agent_name: str, result: Dict[str, Any]):
        self.task_results[agent_name] = result

    def record_conflict(self, conflict: Dict[str, Any]):
        self.conflicts.append(conflict)

    def record_failure(self, agent_name: str):
        if agent_name not in self.failed_agents:
            self.failed_agents.append(agent_name)
        self.retry_counts[agent_name] = self.retry_counts.get(agent_name, 0) + 1

    def is_agent_retryable(self, agent_name: str) -> bool:
        max_retries = self.execution_budget.get("max_retries", 2)
        return self.retry_counts.get(agent_name, 0) < max_retries

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "session_id": self.session_id,
            "trace_id": self.trace_id,
            "status": self.status,
            "normalized_event": self.normalized_event,
            "previous_run_context": self.previous_run_context,
            "grounding_context": self.grounding_context,
            "original_input": self.original_input,
            "selected_agents": self.selected_agents,
            "skipped_agents": self.skipped_agents,
            "task_results": self.task_results,
            "conflicts": self.conflicts,
            "arbitration_results": self.arbitration_results,
            "final_decision": self.final_decision,
            "failed_agents": self.failed_agents,
            "budget_usage": self.budget_usage,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "completed_at": self.completed_at,
        }
