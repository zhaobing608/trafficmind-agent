"""
WorkflowDefinition 管理 — Phase 12

管理 Workflow 定义的创建、版本控制、查询和快照冻结。

设计原则：
  - 每次修改 Definition 递增版本号
  - Run 绑定到特定版本，不受后续修改影响
  - 版本快照是不可变的完整 DAG JSON
  - 通过 Repository 持久化
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from backend.workflow.models import (
    DefinitionStatus,
    NodeConfig,
    NodeType,
    WorkflowDefinition,
    WorkflowDefinitionVersion,
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_definition_id() -> str:
    """生成 Definition ID。"""
    return f"wfdef_{uuid.uuid4().hex[:12]}"


def generate_version_id() -> str:
    """生成 Version ID。"""
    return f"wfver_{uuid.uuid4().hex[:12]}"


# ═══════════════════════════════════════════════════════════════════════════════
# DefinitionManager
# ═══════════════════════════════════════════════════════════════════════════════


class DefinitionManager:
    """Workflow 定义管理器。

    负责定义的 CRUD、版本创建和校验。
    持久化委托给 WorkflowRepository。
    """

    def __init__(self, repository: "WorkflowRepository" = None):
        """
        Args:
            repository: Workflow 持久化仓库。若未提供，后续调用需自行注入。
        """
        self._repo = repository

    def set_repository(self, repository: "WorkflowRepository") -> None:
        """注入 Repository（延迟绑定）。"""
        self._repo = repository

    @property
    def repo(self) -> "WorkflowRepository":
        if self._repo is None:
            raise RuntimeError("DefinitionManager 未绑定 Repository")
        return self._repo

    # ── 创建 ──────────────────────────────────────────────────────────

    def create_definition(
        self,
        name: str,
        description: str = "",
        category: str = "",
        nodes: List[NodeConfig] = None,
        entry_node_id: str = "",
    ) -> WorkflowDefinition:
        """创建新的 Workflow 定义。

        Args:
            name: 定义名称
            description: 描述
            category: 分类
            nodes: 节点配置列表
            entry_node_id: 入口节点 ID

        Returns:
            创建的 WorkflowDefinition（含 ID 和时间戳）

        Raises:
            ValueError: 定义校验失败
        """
        now = _utc_now_iso()
        definition = WorkflowDefinition(
            id=generate_definition_id(),
            name=name,
            description=description,
            category=category,
            status=DefinitionStatus.DRAFT,
            nodes=nodes or [],
            entry_node_id=entry_node_id,
            created_at=now,
            updated_at=now,
        )
        # 校验
        issues = definition.validate()
        if issues:
            raise ValueError(f"Definition 校验失败: {'; '.join(issues)}")

        self.repo.save_definition(definition)
        return definition

    # ── 版本管理 ──────────────────────────────────────────────────────

    def create_version(
        self,
        definition: WorkflowDefinition,
        changelog: str = "",
    ) -> WorkflowDefinitionVersion:
        """为 Definition 创建不可变版本快照。

        自动递增版本号，冻结当前 nodes 为 JSON。

        Args:
            definition: 要创建版本的 Definition
            changelog: 变更说明

        Returns:
            创建的版本快照
        """
        # 获取当前最大版本号
        current_max = self.repo.get_latest_version_number(definition.id)
        new_version = current_max + 1

        version = WorkflowDefinitionVersion(
            id=generate_version_id(),
            definition_id=definition.id,
            version=new_version,
            definition_json=definition.to_dict(),
            changelog=changelog,
        )

        self.repo.save_definition_version(version)
        return version

    def get_definition_at_version(
        self,
        definition_id: str,
        version: int,
    ) -> Optional[WorkflowDefinition]:
        """获取指定版本的 Definition（冻结快照还原）。"""
        ver = self.repo.get_definition_version(definition_id, version)
        if ver is None:
            return None
        return WorkflowDefinition.from_dict(ver.definition_json)

    def get_latest_definition(self, definition_id: str) -> Optional[WorkflowDefinition]:
        """获取当前最新 Definition。"""
        return self.repo.get_definition(definition_id)

    # ── 激活 / 废弃 ───────────────────────────────────────────────────

    def activate_definition(self, definition_id: str) -> WorkflowDefinition:
        """激活 Definition（将状态设为 active）。

        激活前自动创建版本快照。
        """
        definition = self.repo.get_definition(definition_id)
        if definition is None:
            raise ValueError(f"Definition '{definition_id}' 不存在")
        if definition.status == DefinitionStatus.ACTIVE:
            return definition  # 已激活

        # 激活前创建版本快照
        self.create_version(definition, changelog="激活")

        definition.status = DefinitionStatus.ACTIVE
        definition.updated_at = _utc_now_iso()
        self.repo.save_definition(definition)
        return definition

    def deprecate_definition(self, definition_id: str) -> WorkflowDefinition:
        """废弃 Definition。"""
        definition = self.repo.get_definition(definition_id)
        if definition is None:
            raise ValueError(f"Definition '{definition_id}' 不存在")

        definition.status = DefinitionStatus.DEPRECATED
        definition.updated_at = _utc_now_iso()
        self.repo.save_definition(definition)
        return definition

    # ── 校验 ──────────────────────────────────────────────────────────

    def validate_for_execution(
        self,
        definition: WorkflowDefinition,
    ) -> List[str]:
        """校验 Definition 是否可执行。

        返回问题列表，空列表表示可执行。
        """
        issues = definition.validate()
        from backend.workflow.action_execution import contains_sensitive_key
        for node in definition.nodes:
            if node.node_type == NodeType.ACTION and contains_sensitive_key(
                node.config.get("action_params", {})
            ):
                issues.append(
                    f"Action 节点 '{node.node_id}' 参数包含禁止持久化的 credential/secret 字段"
                )
        if definition.status == DefinitionStatus.DEPRECATED:
            issues.append("Definition 已废弃，不建议执行")
        return issues


# ═══════════════════════════════════════════════════════════════════════════════
# Repository 抽象接口（前向声明，实现在 repository.py）
# ═══════════════════════════════════════════════════════════════════════════════


class WorkflowRepository:
    """Workflow 持久化仓库抽象接口。

    实现在 backend.workflow.repository 中。"""

    def save_definition(self, definition: WorkflowDefinition) -> None:
        raise NotImplementedError

    def get_definition(self, definition_id: str) -> Optional[WorkflowDefinition]:
        raise NotImplementedError

    def list_definitions(
        self,
        status: Optional[str] = None,
    ) -> List[WorkflowDefinition]:
        raise NotImplementedError

    def save_definition_version(self, version: WorkflowDefinitionVersion) -> None:
        raise NotImplementedError

    def get_definition_version(
        self, definition_id: str, version: int
    ) -> Optional[WorkflowDefinitionVersion]:
        raise NotImplementedError

    def get_latest_version_number(self, definition_id: str) -> int:
        raise NotImplementedError

    def list_definition_versions(
        self, definition_id: str
    ) -> List[WorkflowDefinitionVersion]:
        raise NotImplementedError

    def save_run(self, run) -> None:  # WorkflowRun
        raise NotImplementedError

    def get_run(self, run_id: str):  # -> Optional[WorkflowRun]
        raise NotImplementedError

    def list_runs(
        self,
        session_id: str = "",
        definition_id: str = "",
        status: Optional[str] = None,
        limit: int = 50,
    ):  # -> List[WorkflowRun]
        raise NotImplementedError

    def save_node_run(self, node_run) -> None:  # WorkflowNodeRun
        raise NotImplementedError

    def get_node_runs(self, run_id: str):  # -> List[WorkflowNodeRun]
        raise NotImplementedError

    def save_event(self, event) -> None:  # WorkflowEvent
        raise NotImplementedError

    def list_events(self, run_id: str):  # -> List[WorkflowEvent]
        raise NotImplementedError

    def save_approval(self, approval) -> None:  # WorkflowApproval
        raise NotImplementedError

    def get_approval(self, approval_id: str):  # -> Optional[WorkflowApproval]
        raise NotImplementedError

    def get_pending_approval(self, run_id: str, node_id: str):  # -> Optional[WorkflowApproval]
        raise NotImplementedError

    def save_action_record(self, record) -> None:  # WorkflowActionRecord
        raise NotImplementedError

    def get_action_record_by_idempotency_key(
        self, idempotency_key: str
    ):  # -> Optional[WorkflowActionRecord]
        raise NotImplementedError

    def list_action_records(self, run_id: str):  # -> List[WorkflowActionRecord]
        raise NotImplementedError
