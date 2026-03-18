"""ExecDSL 内部数据模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping, Optional, Sequence


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _deep_freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_deep_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_deep_freeze(item) for item in value)
    if isinstance(value, set):
        return frozenset(_deep_freeze(item) for item in value)
    return value


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
    sql_params: Mapping[str, Any] = field(default_factory=dict)
    outputs: Sequence[str] = field(default_factory=list)
    description: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "sql_params", _deep_freeze(self.sql_params))
        object.__setattr__(self, "outputs", tuple(self.outputs))


@dataclass(frozen=True)
class ContextNode(SqlNode):
    """执行上下文节点。"""


@dataclass(frozen=True)
class VariableCondition:
    """变量条件分支。"""

    condition: str
    value: Any

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _deep_freeze(self.value))


@dataclass(frozen=True)
class VariableDefinition:
    """变量定义。"""

    when: Sequence[VariableCondition] = field(default_factory=list)
    default: Any = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "when", tuple(self.when))
        object.__setattr__(self, "default", _deep_freeze(self.default))


@dataclass(frozen=True)
class PrecheckNode(SqlNode):
    """前置检查节点。"""

    name: str = ""
    on_fail: Optional[FailPolicy] = None


@dataclass(frozen=True)
class StepNode(SqlNode):
    """主执行步骤节点。"""

    name: str = ""
    consumes: Sequence[ConsumeSpec] = field(default_factory=list)

    def __post_init__(self) -> None:
        super().__post_init__()
        object.__setattr__(self, "consumes", tuple(self.consumes))


@dataclass(frozen=True)
class DslDocument:
    """完整 DSL 文档。"""

    context: Optional[ContextNode]
    steps: Sequence[StepNode]
    on_fail: FailPolicy
    raw: Mapping[str, Any]
    variables: Mapping[str, VariableDefinition] = field(default_factory=dict)
    prechecks: Sequence[PrecheckNode] = field(default_factory=list)

    def __post_init__(self) -> None:
        object.__setattr__(self, "steps", tuple(self.steps))
        object.__setattr__(self, "raw", _deep_freeze(self.raw))
        object.__setattr__(self, "variables", MappingProxyType(dict(self.variables)))
        object.__setattr__(self, "prechecks", tuple(self.prechecks))
