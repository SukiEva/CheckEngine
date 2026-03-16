"""ExecDSL 内部数据模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class ConsumeSpec:
    """步骤间结果消费定义。"""

    from_path: str
    alias: str


@dataclass(frozen=True)
class FailPolicy:
    """失败判定与消息渲染配置。"""

    decision: str
    mode: str
    message_cn: str
    message_en: str
    divider: Optional[str] = None
    divider_cn: Optional[str] = None
    divider_en: Optional[str] = None


@dataclass(frozen=True)
class SqlNode:
    """SQL 类型节点的通用字段。"""

    type: str
    datasource: str
    result_mode: str
    sql_template: str
    sql_params: dict[str, Any] = field(default_factory=dict)
    outputs: list[str] = field(default_factory=list)
    description: Optional[str] = None


@dataclass(frozen=True)
class ContextNode(SqlNode):
    """执行上下文节点。"""


@dataclass(frozen=True)
class VariableCondition:
    """变量条件分支。"""

    condition: str
    value: Any


@dataclass(frozen=True)
class VariableDefinition:
    """变量定义。"""

    type: str
    when: list[VariableCondition] = field(default_factory=list)
    default: Any = None


@dataclass(frozen=True)
class PrecheckNode(SqlNode):
    """前置检查节点。"""

    name: str = ""
    on_fail: Optional[FailPolicy] = None


@dataclass(frozen=True)
class StepNode(SqlNode):
    """主执行步骤节点。"""

    name: str = ""
    consumes: list[ConsumeSpec] = field(default_factory=list)


@dataclass(frozen=True)
class DslDocument:
    """完整 DSL 文档。"""

    context: ContextNode
    variables: dict[str, VariableDefinition]
    prechecks: list[PrecheckNode]
    steps: list[StepNode]
    on_fail: FailPolicy
    raw: dict[str, Any]
