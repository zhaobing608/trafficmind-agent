"""
工具注册表 — Phase 16 Round 3

中央管理工具元数据（不重新实现工具本身）。

每个工具元数据：
  - name / description / category
  - sideEffect: 是否有外部副作用
  - riskLevel: read_only / write / high_risk
  - approvalRequired: 是否要求人工审批
  - idempotent: 是否幂等
  - timeoutSeconds: 建议超时
  - retryPolicy: 重试策略（max_retries, backoff）
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class ToolRisk(str, Enum):
    READ_ONLY = "read_only"
    WRITE = "write"
    HIGH_RISK = "high_risk"


@dataclass
class ToolMetadata:
    """工具元数据。"""
    name: str
    description: str = ""
    category: str = "general"
    sideEffect: bool = False
    riskLevel: ToolRisk = ToolRisk.READ_ONLY
    approvalRequired: bool = False
    idempotent: bool = False
    timeoutSeconds: float = 30.0
    retryPolicy: Dict[str, Any] = field(default_factory=lambda: {"maxRetries": 0})


class ToolRegistry:
    """中央工具注册表。"""

    def __init__(self):
        self._tools: Dict[str, ToolMetadata] = {}
        self._register_defaults()

    def _register_defaults(self) -> None:
        """注册项目内已存在的工具（按真实代码分类）。"""
        # ── 事件校验 / 标准化（READ_ONLY，无副作用） ──
        self.register(ToolMetadata(
            name="validate_event", category="event", description="校验事件字段完整性",
            riskLevel=ToolRisk.READ_ONLY,
        ))
        self.register(ToolMetadata(
            name="standardize_event", category="event", description="标准化事件对象",
            riskLevel=ToolRisk.READ_ONLY,
        ))
        self.register(ToolMetadata(
            name="normalize_event_type", category="event", description="事件类型中英文归一化",
            riskLevel=ToolRisk.READ_ONLY,
        ))

        # ── 风险评分 / 调度（READ_ONLY，纯计算） ──
        self.register(ToolMetadata(
            name="calculate_risk_score", category="risk", description="确定性风险评分",
            riskLevel=ToolRisk.READ_ONLY,
        ))
        self.register(ToolMetadata(
            name="dispatch_departments", category="dispatch", description="联动部门映射",
            riskLevel=ToolRisk.READ_ONLY,
        ))

        # ── 查询 / 统计 / 规则检索（READ_ONLY） ──
        self.register(ToolMetadata(
            name="get_stats", category="analytics", description="聚合统计",
            riskLevel=ToolRisk.READ_ONLY,
        ))
        self.register(ToolMetadata(
            name="get_event_by_id", category="analytics", description="查询单条事件",
            riskLevel=ToolRisk.READ_ONLY,
        ))
        self.register(ToolMetadata(
            name="get_all_events_for_similarity", category="analytics", description="全量事件（相似度用）",
            riskLevel=ToolRisk.READ_ONLY,
        ))
        self.register(ToolMetadata(
            name="get_unclosed_events", category="analytics", description="未闭环事件提醒",
            riskLevel=ToolRisk.READ_ONLY,
        ))
        self.register(ToolMetadata(
            name="get_high_risk_roads", category="analytics", description="高风险路口 TopN",
            riskLevel=ToolRisk.READ_ONLY,
        ))
        self.register(ToolMetadata(
            name="search_rules", category="knowledge", description="规则库检索",
            riskLevel=ToolRisk.READ_ONLY,
        ))
        self.register(ToolMetadata(
            name="search_similar_cases", category="knowledge", description="相似案例检索",
            riskLevel=ToolRisk.READ_ONLY,
        ))
        self.register(ToolMetadata(
            name="generate_report", category="report", description="生成处置报告",
            riskLevel=ToolRisk.READ_ONLY,
        ))
        self.register(ToolMetadata(
            name="generate_daily_report", category="report", description="生成日报",
            riskLevel=ToolRisk.READ_ONLY,
        ))
        self.register(ToolMetadata(
            name="generate_weekly_report", category="report", description="生成周报",
            riskLevel=ToolRisk.READ_ONLY,
        ))

        # ── 数据库写操作（WRITE） ──
        self.register(ToolMetadata(
            name="save_event_analysis", category="persistence", description="保存事件分析结果",
            sideEffect=True, riskLevel=ToolRisk.WRITE, idempotent=True,
        ))
        self.register(ToolMetadata(
            name="update_event_status", category="persistence", description="更新事件处置状态",
            sideEffect=True, riskLevel=ToolRisk.WRITE, idempotent=True,
        ))
        self.register(ToolMetadata(
            name="create_dispatch_task", category="dispatch", description="创建内部处置派单任务",
            sideEffect=True, riskLevel=ToolRisk.HIGH_RISK, approvalRequired=True,
            idempotent=True,
        ))
        self.register(ToolMetadata(
            name="send_notification", category="notification", description="通过服务端通知 Provider 发送消息",
            sideEffect=True, riskLevel=ToolRisk.HIGH_RISK, approvalRequired=True,
            idempotent=True, timeoutSeconds=15.0,
            retryPolicy={"maxRetries": 0},
        ))

        # ── 外部通知（HIGH_RISK，有外部副作用） ──
        self.register(ToolMetadata(
            name="send_wechat_work", category="notification", description="企业微信通知",
            sideEffect=True, riskLevel=ToolRisk.HIGH_RISK, approvalRequired=True,
            timeoutSeconds=10.0, retryPolicy={"maxRetries": 1},
        ))
        self.register(ToolMetadata(
            name="send_dingtalk", category="notification", description="钉钉通知",
            sideEffect=True, riskLevel=ToolRisk.HIGH_RISK, approvalRequired=True,
            timeoutSeconds=10.0, retryPolicy={"maxRetries": 1},
        ))
        self.register(ToolMetadata(
            name="send_email", category="notification", description="邮件通知",
            sideEffect=True, riskLevel=ToolRisk.HIGH_RISK, approvalRequired=True,
            timeoutSeconds=15.0, retryPolicy={"maxRetries": 1},
        ))
        self.register(ToolMetadata(
            name="notify_high_risk_event", category="notification", description="高风险事件自动告警",
            sideEffect=True, riskLevel=ToolRisk.HIGH_RISK, approvalRequired=True,
            timeoutSeconds=15.0, retryPolicy={"maxRetries": 1},
        ))

        # ── 仿真动作（HIGH_RISK） ──
        self.register(ToolMetadata(
            name="simulation_traffic_diversion", category="simulation", description="仿真交通分流动作",
            sideEffect=True, riskLevel=ToolRisk.HIGH_RISK, approvalRequired=True,
        ))
        self.register(ToolMetadata(
            name="simulation_signal_adjust", category="simulation", description="仿真信号配时调整",
            sideEffect=True, riskLevel=ToolRisk.HIGH_RISK, approvalRequired=True,
        ))
        self.register(ToolMetadata(
            name="simulation_signal_adjustment", category="simulation", description="仿真信号配时调整",
            sideEffect=True, riskLevel=ToolRisk.HIGH_RISK, approvalRequired=True,
        ))
        self.register(ToolMetadata(
            name="simulation_lane_control", category="simulation", description="仿真车道控制",
            sideEffect=True, riskLevel=ToolRisk.HIGH_RISK, approvalRequired=True,
        ))
        self.register(ToolMetadata(
            name="simulation_dispatch_coordination", category="simulation", description="仿真调度协调",
            sideEffect=True, riskLevel=ToolRisk.HIGH_RISK, approvalRequired=True,
        ))
        self.register(ToolMetadata(
            name="simulation_monitor", category="simulation", description="仿真监控检查",
            sideEffect=True, riskLevel=ToolRisk.HIGH_RISK, approvalRequired=True,
        ))
        self.register(ToolMetadata(
            name="simulation_close", category="simulation", description="仿真事件关闭",
            sideEffect=True, riskLevel=ToolRisk.HIGH_RISK, approvalRequired=True,
        ))

        # ── Workflow action_type 名称（execute_action / _dispatch_action 实际调度） ──
        self.register(ToolMetadata(
            name="notify_wechat", category="notification", description="企业微信通知（workflow 调度）",
            sideEffect=True, riskLevel=ToolRisk.HIGH_RISK, approvalRequired=True,
            timeoutSeconds=10.0, retryPolicy={"maxRetries": 1},
        ))
        self.register(ToolMetadata(
            name="notify_dingtalk", category="notification", description="钉钉通知（workflow 调度）",
            sideEffect=True, riskLevel=ToolRisk.HIGH_RISK, approvalRequired=True,
            timeoutSeconds=10.0, retryPolicy={"maxRetries": 1},
        ))
        self.register(ToolMetadata(
            name="save_result", category="persistence", description="持久化分析结果（workflow 调度）",
            sideEffect=True, riskLevel=ToolRisk.WRITE, idempotent=True,
        ))

        # ── 其他业务写操作（WRITE） ──
        self.register(ToolMetadata(
            name="build_knowledge_index", category="knowledge", description="重建知识库向量索引",
            sideEffect=True, riskLevel=ToolRisk.WRITE,
            timeoutSeconds=120.0,
        ))

    def register(self, meta: ToolMetadata) -> None:
        """注册工具元数据。"""
        self._tools[meta.name] = meta

    def get(self, name: str) -> Optional[ToolMetadata]:
        """获取工具元数据。未注册返回 None。"""
        return self._tools.get(name)

    def is_registered(self, name: str) -> bool:
        return name in self._tools

    def all(self) -> List[ToolMetadata]:
        return list(self._tools.values())


# 全局单例
_registry: Optional[ToolRegistry] = None


def get_tool_registry() -> ToolRegistry:
    """获取全局 ToolRegistry（懒加载）。"""
    global _registry
    if _registry is None:
        _registry = ToolRegistry()
    return _registry


def reset_tool_registry() -> None:
    """重置（测试用）。"""
    global _registry
    _registry = None
