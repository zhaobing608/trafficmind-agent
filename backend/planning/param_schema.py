"""
Planner-eligible Business Param Schema — Phase 18 Round 1

只给 Round1 真实 planner-eligible 且端到端有业务语义的 action 补 schema。

schema 只表达 business params（LLM 的 parameterHints 仅能填充这些字段）。

禁止 LLM 控制：
  risk / approval / runtime IDs / trace / timestamp / dispatchAttemptId /
  requestId / driver info / runId / stepId。

compiler 只接受 schema 声明字段，并丢弃 FORBIDDEN_PARAM_KEYS。
"""

from __future__ import annotations

from typing import Any, Dict, List

# 永远丢弃的参数 key（runtime / 权限 / 追踪字段，LLM 不得控制）
FORBIDDEN_PARAM_KEYS = frozenset({
    "timestamp", "ts", "requestId", "request_id", "traceId", "trace_id",
    "dispatchAttemptId", "dispatch_attempt_id", "approvalId", "approval_id",
    "approvalIdentity", "approval_identity", "riskLevel", "risk_level",
    "riskScore", "risk_score", "approvalRequired", "approval_required",
    "runId", "run_id", "stepId", "step_id", "nodeId", "node_id",
    "driverOwner", "driver_owner", "driverGeneration", "driver_generation",
    "idempotencyKey", "idempotency_key",
})

# 类型 token → python type（compiler 类型校验用）
_TYPE_MAP = {
    "str": str,
    "int": int,
    "float": (int, float),
    "bool": bool,
    "list[str]": list,
}


def _f(type_name: str, required: bool, description: str = "") -> Dict[str, Any]:
    """构造单个字段 schema。"""
    return {"type": type_name, "required": required, "description": description}


# businessParamSchema：每个 planner-eligible action 的 business 参数 schema
PLANNER_PARAM_SCHEMAS: Dict[str, Dict[str, Any]] = {
    # Phase 21.3 reliable actions.  Only business parameters cross the
    # Agent → Plan boundary; credentials and runtime identity fields remain
    # server-owned and are removed by FORBIDDEN_PARAM_KEYS / schema filtering.
    "update_event_status": {
        "description": "将 canonical Event 推进到合法的目标状态",
        "fields": {
            "status": _f("str", True, "目标 Event 状态"),
        },
        "required": ["status"],
    },
    "create_dispatch_task": {
        "description": "创建幂等的内部处置任务",
        "fields": {
            "assignee": _f("str", False, "处置责任人或单位"),
            "target": _f("str", False, "处置目标"),
            "instruction": _f("str", True, "处置指令"),
        },
        "required": ["instruction"],
    },
    "send_notification": {
        "description": "通过服务端 NotificationProvider 发送幂等通知",
        "fields": {
            "channel": _f("str", False, "服务端允许的通知通道"),
            "target": _f("str", True, "通知目标"),
            "message": _f("str", True, "通知内容"),
        },
        "required": ["target", "message"],
    },
    # 通知类：无 business 参数（channel/内容由 system/event 上下文决定）
    "notify_wechat": {
        "description": "企业微信通知（高风险事件），无额外 business 参数",
        "fields": {},
        "required": [],
    },
    "notify_dingtalk": {
        "description": "钉钉通知（高风险事件），无额外 business 参数",
        "fields": {},
        "required": [],
    },
    # 分流：需指定源道路与目标道路
    "simulation_traffic_diversion": {
        "description": "仿真交通分流动作（有端到端业务语义）",
        "fields": {
            "source_road_id": _f("str", True, "分流源道路 ID"),
            "target_road_ids": _f("list[str]", True, "分流目标道路 ID 列表"),
            "diversion_ratio": _f("float", False, "分流比例（0-1）"),
        },
        "required": ["source_road_id", "target_road_ids"],
    },
    # 信号调整：需指定路口
    "simulation_signal_adjustment": {
        "description": "仿真信号配时调整动作（有端到端业务语义）",
        "fields": {
            "intersection_id": _f("str", True, "目标路口 ID"),
            "cycle_length": _f("float", False, "周期长度（秒）"),
        },
        "required": ["intersection_id"],
    },
    # 持久化：无 business 参数（compiler 结构性插入，LLM 不可 propose）
    "save_result": {
        "description": "闭环持久化分析结果（compiler 结构性插入）",
        "fields": {},
        "required": [],
    },
}


def get_param_schema(action_type: str) -> Dict[str, Any]:
    """获取 action 的 business param schema（无则 None）。"""
    return PLANNER_PARAM_SCHEMAS.get(action_type)


def _check_type(value: Any, type_name: str) -> bool:
    """business param 类型校验。"""
    expected = _TYPE_MAP[type_name]
    if type_name == "list[str]":
        return isinstance(value, list) and all(isinstance(x, str) for x in value)
    return isinstance(value, expected)


def normalize_parameter_hints(action_type: str, hints: Dict[str, Any]) -> Dict[str, Any]:
    """将 untrusted parameterHints 归一化为 canonical business params。

    规则（fail-closed）：
      - 只接受 schema 声明字段；unknown business param → 明确 drop（不透传）
      - FORBIDDEN_PARAM_KEYS（risk/runtime/trace/身份字段）→ drop
      - 已知字段但类型不符 → raise INVALID_PARAMETER_HINTS（compile fail）
      - required 字段缺失 → raise INVALID_PARAMETER_HINTS（compile fail）

    Returns:
        normalized params dict。

    Raises:
        PlannerFailure(INVALID_PARAMETER_HINTS)：类型不符 / required 缺失。
    """
    from backend.planning.proposal import PlannerFailure, PlannerFailureCode

    schema = get_param_schema(action_type)
    if schema is None:
        return {}
    fields = schema.get("fields", {})
    required = schema.get("required", [])

    normalized: Dict[str, Any] = {}
    for k, v in (hints or {}).items():
        if k in FORBIDDEN_PARAM_KEYS:
            continue  # 丢弃权限/追踪字段（injection 尝试）
        if k not in fields:
            continue  # unknown business param → 明确 drop（不透传）
        if not _check_type(v, fields[k]["type"]):
            raise PlannerFailure(
                PlannerFailureCode.INVALID_PARAMETER_HINTS,
                f"action '{action_type}' 的 business param '{k}' 类型不符（期望 {fields[k]['type']}）",
            )
        normalized[k] = v

    for r in required:
        if r not in normalized:
            raise PlannerFailure(
                PlannerFailureCode.INVALID_PARAMETER_HINTS,
                f"action '{action_type}' 缺少 required business param '{r}'",
            )

    return normalized
