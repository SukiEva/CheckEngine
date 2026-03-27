"""ExecDSL 字段枚举定义。"""

from __future__ import annotations

from enum import Enum


class TopLevelField(str, Enum):
    """DSL 顶层块字段。"""

    CONTEXT = "context"
    VARIABLES = "variables"
    PRECHECKS = "prechecks"
    STEPS = "steps"
    ON_FAIL = "on_fail"


class SqlNodeField(str, Enum):
    """SQL 节点通用字段。"""

    TYPE = "type"
    DATASOURCE = "datasource"
    RESULT_MODE = "result_mode"
    SQL_TEMPLATE = "sql_template"
    SQL_PARAMS = "sql_params"
    OUTPUTS = "outputs"
    DESCRIPTION = "description"


class VariableField(str, Enum):
    """变量定义字段。"""

    WHEN = "when"
    CONDITION = "condition"
    VALUE = "value"
    DEFAULT = "default"


class NamedNodeField(str, Enum):
    """具名节点通用字段。"""

    NAME = "name"


class StepField(str, Enum):
    """步骤字段。"""

    CONSUMES = "consumes"


class ConsumeField(str, Enum):
    """consumes 子项字段。"""

    FROM = "from"
    ALIAS = "alias"


class FailPolicyField(str, Enum):
    """失败策略字段。"""

    ON_FAIL = "on_fail"
    DECISION = "decision"
    MODE = "mode"
    MESSAGE_CN = "message_cn"
    MESSAGE_EN = "message_en"
    DIVIDER = "divider"
    DIVIDER_CN = "divider_cn"
    DIVIDER_EN = "divider_en"


class RuntimeScope(str, Enum):
    """运行时作用域名称。"""

    INPUT = "input"
    CONTEXT = "context"
    VARIABLES = "variables"
    STEPS = "steps"


class ReservedNodeName(str, Enum):
    """保留节点名。"""

    INPUT = "input"
    CONTEXT = "context"
    VARIABLES = "variables"
    STEPS = "steps"
    ON_FAIL = "on_fail"
