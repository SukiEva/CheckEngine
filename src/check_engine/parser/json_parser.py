"""ExecDSL JSON 解析器。"""

from __future__ import annotations

import json
from enum import Enum
from typing import Any, Mapping, Optional, Sequence, cast

from ..dsl import (
    ConsumeSpec,
    ContextNode,
    DslDocument,
    FailMode,
    FailPolicy,
    NodeType,
    PrecheckNode,
    ResultMode,
    StepNode,
    VariableCondition,
    VariableDefinition,
)
from ..exceptions import DSLParseError


class DslField(str, Enum):
    """DSL 字段常量。"""

    CONTEXT = "context"
    VARIABLES = "variables"
    PRECHECKS = "prechecks"
    STEPS = "steps"
    ON_FAIL = "on_fail"

    TYPE = "type"
    DATASOURCE = "datasource"
    RESULT_MODE = "result_mode"
    SQL_TEMPLATE = "sql_template"
    SQL_PARAMS = "sql_params"
    OUTPUTS = "outputs"
    DESCRIPTION = "description"

    WHEN = "when"
    CONDITION = "condition"
    VALUE = "value"
    DEFAULT = "default"
    NAME = "name"

    CONSUMES = "consumes"
    FROM = "from"
    ALIAS = "alias"

    DECISION = "decision"
    MODE = "mode"
    MESSAGE_CN = "message_cn"
    MESSAGE_EN = "message_en"
    DIVIDER = "divider"
    DIVIDER_CN = "divider_cn"
    DIVIDER_EN = "divider_en"


class JsonDslParser:
    """将 JSON DSL 文本解析为内部数据模型。"""

    REQUIRED_TOP_LEVEL_KEYS = (
        DslField.STEPS.value,
        DslField.ON_FAIL.value,
    )

    def parse(self, dsl_text: str) -> DslDocument:
        if not isinstance(dsl_text, str):
            raise DSLParseError("DSL text must be a string.")

        try:
            data = json.loads(dsl_text)
        except json.JSONDecodeError as exc:
            raise DSLParseError(f"Failed to parse DSL JSON: {exc.msg}") from exc

        if not isinstance(data, dict):
            raise DSLParseError("Top-level DSL must be a JSON object.")

        missing = [key for key in self.REQUIRED_TOP_LEVEL_KEYS if key not in data]
        if missing:
            raise DSLParseError(f"DSL is missing top-level blocks: {', '.join(missing)}")

        return DslDocument(
            context=self._parse_context(data[DslField.CONTEXT.value]) if DslField.CONTEXT.value in data else None,
            variables=self._parse_variables(data.get(DslField.VARIABLES.value, {})),
            prechecks=self._parse_prechecks(data.get(DslField.PRECHECKS.value, [])),
            steps=self._parse_steps(data[DslField.STEPS.value]),
            on_fail=self._parse_fail_policy(data[DslField.ON_FAIL.value], DslField.ON_FAIL.value),
            raw=data,
        )

    def _parse_context(self, value: Any) -> ContextNode:
        path = DslField.CONTEXT.value
        mapping = self._expect_dict(value, path)
        return ContextNode(**self._parse_sql_node_fields(mapping, path))

    def _parse_variables(self, value: Any) -> Mapping[str, VariableDefinition]:
        path = DslField.VARIABLES.value
        mapping = self._expect_dict(value, path)
        variables: dict[str, VariableDefinition] = {}

        for name, raw_definition in mapping.items():
            var_path = f"{path}.{name}"
            definition = self._expect_dict(raw_definition, var_path)
            when_path = f"{var_path}.{DslField.WHEN.value}"
            when_items = self._expect_list(definition.get(DslField.WHEN.value, []), when_path)
            variables[name] = VariableDefinition(
                when=[
                    VariableCondition(
                        condition=self._expect_string(
                            self._expect_dict(item, f"{when_path}[{index}]").get(DslField.CONDITION.value),
                            f"{when_path}[{index}].{DslField.CONDITION.value}",
                        ),
                        value=self._expect_dict(item, f"{when_path}[{index}]").get(DslField.VALUE.value),
                    )
                    for index, item in enumerate(when_items)
                ],
                default=definition.get(DslField.DEFAULT.value),
            )

        return variables

    def _parse_prechecks(self, value: Any) -> Sequence[PrecheckNode]:
        path = DslField.PRECHECKS.value
        items = self._expect_list(value, path)
        nodes: list[PrecheckNode] = []
        for index, item in enumerate(items):
            node_path = f"{path}[{index}]"
            mapping = self._expect_dict(item, node_path)
            sql_node_fields = self._parse_sql_node_fields(mapping, node_path)
            nodes.append(
                PrecheckNode(
                    name=self._expect_string(mapping.get(DslField.NAME.value), f"{node_path}.{DslField.NAME.value}"),
                    on_fail=self._parse_fail_policy(mapping.get(DslField.ON_FAIL.value), f"{node_path}.{DslField.ON_FAIL.value}"),
                    **sql_node_fields,
                )
            )
        return nodes

    def _parse_steps(self, value: Any) -> Sequence[StepNode]:
        path = DslField.STEPS.value
        items = self._expect_list(value, path)
        nodes: list[StepNode] = []
        for index, item in enumerate(items):
            node_path = f"{path}[{index}]"
            mapping = self._expect_dict(item, node_path)
            consumes_path = f"{node_path}.{DslField.CONSUMES.value}"
            raw_consumes = self._expect_list(mapping.get(DslField.CONSUMES.value, []), consumes_path)
            sql_node_fields = self._parse_sql_node_fields(mapping, node_path)
            consumes = [
                ConsumeSpec(
                    from_path=self._expect_string(
                        self._expect_dict(raw_consume, f"{consumes_path}[{consume_index}]").get(DslField.FROM.value),
                        f"{consumes_path}[{consume_index}].{DslField.FROM.value}",
                    ),
                    alias=self._expect_string(
                        self._expect_dict(raw_consume, f"{consumes_path}[{consume_index}]").get(DslField.ALIAS.value),
                        f"{consumes_path}[{consume_index}].{DslField.ALIAS.value}",
                    ),
                )
                for consume_index, raw_consume in enumerate(raw_consumes)
            ]
            nodes.append(
                StepNode(
                    name=self._expect_string(mapping.get(DslField.NAME.value), f"{node_path}.{DslField.NAME.value}"),
                    consumes=consumes,
                    **sql_node_fields,
                )
            )
        return nodes

    def _parse_fail_policy(self, value: Any, path: str) -> FailPolicy:
        mapping = self._expect_dict(value, path)
        return FailPolicy(
            decision=self._expect_string(mapping.get(DslField.DECISION.value), f"{path}.{DslField.DECISION.value}"),
            mode=self._expect_fail_mode(mapping.get(DslField.MODE.value), f"{path}.{DslField.MODE.value}"),
            message_cn=self._expect_string(mapping.get(DslField.MESSAGE_CN.value), f"{path}.{DslField.MESSAGE_CN.value}"),
            message_en=self._expect_string(mapping.get(DslField.MESSAGE_EN.value), f"{path}.{DslField.MESSAGE_EN.value}"),
            divider=self._optional_string(mapping.get(DslField.DIVIDER.value), f"{path}.{DslField.DIVIDER.value}"),
            divider_cn=self._optional_string(mapping.get(DslField.DIVIDER_CN.value), f"{path}.{DslField.DIVIDER_CN.value}"),
            divider_en=self._optional_string(mapping.get(DslField.DIVIDER_EN.value), f"{path}.{DslField.DIVIDER_EN.value}"),
        )

    def _parse_string_list(self, value: Any, path: str) -> Sequence[str]:
        items = self._expect_list(value, path)
        return [self._expect_string(item, f"{path}[{index}]") for index, item in enumerate(items)]

    def _parse_sql_node_fields(self, mapping: Mapping[str, Any], path: str) -> dict[str, Any]:
        return {
            "type": self._expect_node_type(mapping.get(DslField.TYPE.value), f"{path}.{DslField.TYPE.value}"),
            "datasource": self._expect_string(mapping.get(DslField.DATASOURCE.value), f"{path}.{DslField.DATASOURCE.value}"),
            "result_mode": self._expect_result_mode(mapping.get(DslField.RESULT_MODE.value), f"{path}.{DslField.RESULT_MODE.value}"),
            "sql_template": self._expect_string(mapping.get(DslField.SQL_TEMPLATE.value), f"{path}.{DslField.SQL_TEMPLATE.value}"),
            "sql_params": self._expect_dict(mapping.get(DslField.SQL_PARAMS.value, {}), f"{path}.{DslField.SQL_PARAMS.value}"),
            "outputs": self._parse_string_list(mapping.get(DslField.OUTPUTS.value, []), f"{path}.{DslField.OUTPUTS.value}"),
            "description": self._optional_string(mapping.get(DslField.DESCRIPTION.value), f"{path}.{DslField.DESCRIPTION.value}"),
        }

    @staticmethod
    def _expect_node_type(value: Any, path: str) -> NodeType:
        return cast(NodeType, JsonDslParser._expect_string(value, path))

    @staticmethod
    def _expect_result_mode(value: Any, path: str) -> ResultMode:
        return cast(ResultMode, JsonDslParser._expect_string(value, path))

    @staticmethod
    def _expect_fail_mode(value: Any, path: str) -> FailMode:
        return cast(FailMode, JsonDslParser._expect_string(value, path))

    @staticmethod
    def _expect_dict(value: Any, path: str) -> Mapping[str, Any]:
        if not isinstance(value, dict):
            raise DSLParseError(f"{path} must be an object.")
        return value

    @staticmethod
    def _expect_list(value: Any, path: str) -> Sequence[Any]:
        if not isinstance(value, list):
            raise DSLParseError(f"{path} must be a list.")
        return value

    @staticmethod
    def _expect_string(value: Any, path: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise DSLParseError(f"{path} must be a non-empty string.")
        return value

    def _optional_string(self, value: Any, path: str) -> Optional[str]:
        if value is None:
            return None
        return self._expect_string(value, path)
