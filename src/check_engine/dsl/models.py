"""ExecDSL 内部数据模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Optional, Sequence, Union

NODE_TYPE_SQL = "sql"
NODE_TYPE_VARIABLE = "variable"
RESULT_MODE_RECORD = "record"
RESULT_MODE_RECORDS = "records"
FAIL_MODE_SUB_REPEAT = "sub_repeat"
FAIL_MODE_FULL_REPEAT = "full_repeat"
FAIL_MODE_SINGLE = "single"
EXISTS_DECISION = "exists"

NodeType = str
ResultMode = Literal["record", "records"]
FailMode = Literal["sub_repeat", "full_repeat", "single"]


@dataclass(frozen=True)
class ConsumeSpec:
    """步骤间结果消费定义。"""

    from_path: str
    alias: str


@dataclass(frozen=True)
class FailPolicy:
    """失败判定与消息渲染配置。"""

    decision: str
    mode: FailMode
    message_cn: str
    message_en: str
    divider: Optional[str] = None
    divider_cn: Optional[str] = None
    divider_en: Optional[str] = None


@dataclass(frozen=True)
class PassPolicy:
    """成功短路判定配置。"""

    decision: str


@dataclass(frozen=True)
class SqlNode:
    """SQL 类型节点的通用字段。"""

    type: NodeType
    datasource: str
    result_mode: ResultMode
    sql_template: str
    sql_params: Mapping[str, Any] = field(default_factory=dict)
    outputs: Sequence[str] = field(default_factory=list)
    description: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "sql_params", dict(self.sql_params))
        object.__setattr__(self, "outputs", tuple(self.outputs))


@dataclass(frozen=True)
class VariableCondition:
    """变量条件分支。"""

    condition: str
    value: Any
    value_specified: bool = True


@dataclass(frozen=True)
class VariableDefinition:
    """变量定义。"""

    when: Sequence[VariableCondition] = field(default_factory=list)
    default: Any = None
    default_specified: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "when", tuple(self.when))


@dataclass(frozen=True)
class SqlStepNode(SqlNode):
    """主执行步骤节点。"""

    name: str = ""
    consumes: Sequence[ConsumeSpec] = field(default_factory=list)
    on_fail: Optional[FailPolicy] = None
    on_pass: Optional[PassPolicy] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        object.__setattr__(self, "consumes", tuple(self.consumes))


@dataclass(frozen=True)
class VariableNode:
    """variable 类型节点。"""

    type: NodeType
    when: Sequence[VariableCondition] = field(default_factory=list)
    default: Any = None
    default_specified: bool = True
    description: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "when", tuple(self.when))


@dataclass(frozen=True)
class VariableStepNode(VariableNode):
    """variable 类型步骤节点。"""

    name: str = ""
    consumes: Sequence[ConsumeSpec] = field(default_factory=list)
    on_fail: Optional[FailPolicy] = None
    on_pass: Optional[PassPolicy] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        object.__setattr__(self, "consumes", tuple(self.consumes))


StepNode = Union[SqlStepNode, VariableStepNode]


@dataclass(frozen=True)
class DslDocument:
    """完整 DSL 文档。"""

    steps: Sequence[StepNode]
    on_fail: FailPolicy
    raw: Mapping[str, Any]
    variables: Mapping[str, VariableDefinition] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "steps", tuple(self.steps))
        object.__setattr__(self, "raw", dict(self.raw))
        object.__setattr__(self, "variables", dict(self.variables))
