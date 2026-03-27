"""JSON DSL 节点级解析组件。"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Optional, cast

from ..dsl import (
    ConsumeSpec,
    FailPolicy,
    FailMode,
    NodeType,
    PrecheckNode,
    ResultMode,
    StepNode,
    VariableCondition,
    VariableDefinition,
)


@dataclass(frozen=True)
class ParserHelpers:
    """解析基础能力集合。"""

    expect_dict: Callable[[Any, str], Mapping[str, Any]]
    expect_list: Callable[[Any, str], Sequence[Any]]
    expect_string: Callable[[Any, str], str]
    optional_string: Callable[[Any, str], Optional[str]]
    parse_string_list: Callable[[Any, str], Sequence[str]]


@dataclass
class JsonNodeParser:
    """节点级解析器，负责变量/步骤/失败策略的转换。"""

    helpers: ParserHelpers

    def parse_variables(self, value: Any, path: str, field: Any) -> Mapping[str, VariableDefinition]:
        mapping = self.helpers.expect_dict(value, path)
        variables: dict[str, VariableDefinition] = {}

        for name, raw_definition in mapping.items():
            var_path = f"{path}.{name}"
            definition = self.helpers.expect_dict(raw_definition, var_path)
            when_path = f"{var_path}.{field.WHEN.value}"
            when_items = self.helpers.expect_list(definition.get(field.WHEN.value, []), when_path)
            variables[name] = VariableDefinition(
                when=[
                    self._parse_variable_condition(item, index, when_path, field)
                    for index, item in enumerate(when_items)
                ],
                default=definition.get(field.DEFAULT.value),
            )

        return variables

    def parse_prechecks(self, value: Any, path: str, field: Any) -> Sequence[PrecheckNode]:
        items = self.helpers.expect_list(value, path)
        nodes: list[PrecheckNode] = []
        for index, item in enumerate(items):
            node_path = f"{path}[{index}]"
            mapping = self.helpers.expect_dict(item, node_path)
            sql_node_fields = self.parse_sql_node_fields(mapping, node_path, field)
            nodes.append(
                PrecheckNode(
                    name=self.helpers.expect_string(mapping.get(field.NAME.value), f"{node_path}.{field.NAME.value}"),
                    on_fail=self.parse_fail_policy(mapping.get(field.ON_FAIL.value), f"{node_path}.{field.ON_FAIL.value}", field),
                    **sql_node_fields,
                )
            )
        return nodes

    def parse_steps(self, value: Any, path: str, field: Any) -> Sequence[StepNode]:
        items = self.helpers.expect_list(value, path)
        nodes: list[StepNode] = []
        for index, item in enumerate(items):
            node_path = f"{path}[{index}]"
            mapping = self.helpers.expect_dict(item, node_path)
            consumes = self._parse_consumes(mapping, node_path, field)
            sql_node_fields = self.parse_sql_node_fields(mapping, node_path, field)
            nodes.append(
                StepNode(
                    name=self.helpers.expect_string(mapping.get(field.NAME.value), f"{node_path}.{field.NAME.value}"),
                    consumes=consumes,
                    **sql_node_fields,
                )
            )
        return nodes

    def parse_fail_policy(self, value: Any, path: str, field: Any) -> FailPolicy:
        mapping = self.helpers.expect_dict(value, path)
        return FailPolicy(
            decision=self.helpers.expect_string(mapping.get(field.DECISION.value), f"{path}.{field.DECISION.value}"),
            mode=cast(FailMode, self.helpers.expect_string(mapping.get(field.MODE.value), f"{path}.{field.MODE.value}")),
            message_cn=self.helpers.expect_string(mapping.get(field.MESSAGE_CN.value), f"{path}.{field.MESSAGE_CN.value}"),
            message_en=self.helpers.expect_string(mapping.get(field.MESSAGE_EN.value), f"{path}.{field.MESSAGE_EN.value}"),
            divider=self.helpers.optional_string(mapping.get(field.DIVIDER.value), f"{path}.{field.DIVIDER.value}"),
            divider_cn=self.helpers.optional_string(mapping.get(field.DIVIDER_CN.value), f"{path}.{field.DIVIDER_CN.value}"),
            divider_en=self.helpers.optional_string(mapping.get(field.DIVIDER_EN.value), f"{path}.{field.DIVIDER_EN.value}"),
        )

    def parse_sql_node_fields(self, mapping: Mapping[str, Any], path: str, field: Any) -> dict[str, Any]:
        return {
            "type": cast(NodeType, self.helpers.expect_string(mapping.get(field.TYPE.value), f"{path}.{field.TYPE.value}")),
            "datasource": self.helpers.expect_string(mapping.get(field.DATASOURCE.value), f"{path}.{field.DATASOURCE.value}"),
            "result_mode": cast(
                ResultMode,
                self.helpers.expect_string(mapping.get(field.RESULT_MODE.value), f"{path}.{field.RESULT_MODE.value}"),
            ),
            "sql_template": self.helpers.expect_string(mapping.get(field.SQL_TEMPLATE.value), f"{path}.{field.SQL_TEMPLATE.value}"),
            "sql_params": self.helpers.expect_dict(mapping.get(field.SQL_PARAMS.value, {}), f"{path}.{field.SQL_PARAMS.value}"),
            "outputs": self.helpers.parse_string_list(mapping.get(field.OUTPUTS.value, []), f"{path}.{field.OUTPUTS.value}"),
            "description": self.helpers.optional_string(mapping.get(field.DESCRIPTION.value), f"{path}.{field.DESCRIPTION.value}"),
        }

    def _parse_variable_condition(self, item: Any, index: int, when_path: str, field: Any) -> VariableCondition:
        condition_path = f"{when_path}[{index}]"
        condition_mapping = self.helpers.expect_dict(item, condition_path)
        return VariableCondition(
            condition=self.helpers.expect_string(
                condition_mapping.get(field.CONDITION.value),
                f"{condition_path}.{field.CONDITION.value}",
            ),
            value=condition_mapping.get(field.VALUE.value),
        )

    def _parse_consumes(self, mapping: Mapping[str, Any], node_path: str, field: Any) -> Sequence[ConsumeSpec]:
        consumes_path = f"{node_path}.{field.CONSUMES.value}"
        raw_consumes = self.helpers.expect_list(mapping.get(field.CONSUMES.value, []), consumes_path)
        return [
            self._parse_consume_spec(raw_consume, consume_index, consumes_path, field)
            for consume_index, raw_consume in enumerate(raw_consumes)
        ]

    def _parse_consume_spec(self, raw_consume: Any, index: int, consumes_path: str, field: Any) -> ConsumeSpec:
        consume_path = f"{consumes_path}[{index}]"
        consume_mapping = self.helpers.expect_dict(raw_consume, consume_path)
        return ConsumeSpec(
            from_path=self.helpers.expect_string(
                consume_mapping.get(field.FROM.value),
                f"{consume_path}.{field.FROM.value}",
            ),
            alias=self.helpers.expect_string(
                consume_mapping.get(field.ALIAS.value),
                f"{consume_path}.{field.ALIAS.value}",
            ),
        )
