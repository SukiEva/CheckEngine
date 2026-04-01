"""JSON DSL 节点级解析组件。"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Optional, cast

from ..dsl import (
    ConsumeField,
    ConsumeSpec,
    FailPolicy,
    FailPolicyField,
    FailMode,
    NamedNodeField,
    NodeType,
    NODE_TYPE_SQL,
    NODE_TYPE_VARIABLE,
    PassPolicy,
    ResultMode,
    SqlNodeField,
    StepField,
    StepNode,
    SqlStepNode,
    VariableStepNode,
    VariableCondition,
    VariableDefinition,
    VariableField,
)
from ..exceptions import DSLParseError
from ..step_registry import StepTypeRegistry, build_default_step_registry


@dataclass(frozen=True)
class ParserHelpers:
    """解析基础能力集合。"""

    expect_dict: Callable[[Any, str], Mapping[str, Any]]
    expect_list: Callable[[Any, str], Sequence[Any]]
    expect_string: Callable[[Any, str], str]
    optional_string: Callable[[Any, str], Optional[str]]
    optional_string_allow_empty: Callable[[Any, str], Optional[str]]
    parse_string_list: Callable[[Any, str], Sequence[str]]


@dataclass
class JsonNodeParser:
    """节点级解析器，负责变量/步骤/失败策略的转换。"""

    helpers: ParserHelpers
    step_registry: StepTypeRegistry = field(default_factory=build_default_step_registry)

    def parse_variables(self, value: Any, path: str) -> Mapping[str, VariableDefinition]:
        mapping = self.helpers.expect_dict(value, path)
        variables: dict[str, VariableDefinition] = {}

        for name, raw_definition in mapping.items():
            var_path = f"{path}.{name}"
            definition = self.helpers.expect_dict(raw_definition, var_path)
            when_path = f"{var_path}.{VariableField.WHEN}"
            when_items = self.helpers.expect_list(definition.get(VariableField.WHEN, []), when_path)
            variables[name] = VariableDefinition(
                when=[
                    self._parse_variable_condition(item, index, when_path)
                    for index, item in enumerate(when_items)
                ],
                default=definition.get(VariableField.DEFAULT),
                default_specified=VariableField.DEFAULT in definition,
            )

        return variables

    def parse_steps(self, value: Any, path: str) -> Sequence[StepNode]:
        items = self.helpers.expect_list(value, path)
        nodes: list[StepNode] = []
        for index, item in enumerate(items):
            node_path = f"{path}[{index}]"
            mapping = self.helpers.expect_dict(item, node_path)
            node_type = cast(NodeType, self.helpers.expect_string(mapping.get(SqlNodeField.TYPE), f"{node_path}.{SqlNodeField.TYPE}"))
            consumes = self._parse_consumes(mapping, node_path)
            definition = self.step_registry.require(node_type, f"{node_path}.{SqlNodeField.TYPE}")
            nodes.append(definition.parse(self, mapping, node_path, consumes))
        return nodes

    def parse_fail_policy(self, value: Any, path: str, required: bool = True) -> Optional[FailPolicy]:
        if value is None:
            if required:
                self.helpers.expect_dict(value, path)
            return None
        mapping = self.helpers.expect_dict(value, path)
        return FailPolicy(
            decision=self.helpers.expect_string(mapping.get(FailPolicyField.DECISION), f"{path}.{FailPolicyField.DECISION}"),
            mode=cast(FailMode, self.helpers.expect_string(mapping.get(FailPolicyField.MODE), f"{path}.{FailPolicyField.MODE}")),
            message_cn=self.helpers.expect_string(mapping.get(FailPolicyField.MESSAGE_CN), f"{path}.{FailPolicyField.MESSAGE_CN}"),
            message_en=self.helpers.expect_string(mapping.get(FailPolicyField.MESSAGE_EN), f"{path}.{FailPolicyField.MESSAGE_EN}"),
            divider=self.helpers.optional_string_allow_empty(mapping.get(FailPolicyField.DIVIDER), f"{path}.{FailPolicyField.DIVIDER}"),
            divider_cn=self.helpers.optional_string_allow_empty(mapping.get(FailPolicyField.DIVIDER_CN), f"{path}.{FailPolicyField.DIVIDER_CN}"),
            divider_en=self.helpers.optional_string_allow_empty(mapping.get(FailPolicyField.DIVIDER_EN), f"{path}.{FailPolicyField.DIVIDER_EN}"),
        )

    def parse_pass_policy(self, value: Any, path: str) -> Optional[PassPolicy]:
        if value is None:
            return None
        mapping = self.helpers.expect_dict(value, path)
        return PassPolicy(
            decision=self.helpers.expect_string(mapping.get(FailPolicyField.DECISION), f"{path}.{FailPolicyField.DECISION}"),
        )

    def parse_sql_node_fields(self, mapping: Mapping[str, Any], path: str, node_type: NodeType) -> dict[str, Any]:
        return {
            "type": node_type,
            "datasource": self.helpers.expect_string(mapping.get(SqlNodeField.DATASOURCE), f"{path}.{SqlNodeField.DATASOURCE}"),
            "result_mode": cast(
                ResultMode,
                self.helpers.expect_string(mapping.get(SqlNodeField.RESULT_MODE), f"{path}.{SqlNodeField.RESULT_MODE}"),
            ),
            "sql_template": self.helpers.expect_string(mapping.get(SqlNodeField.SQL_TEMPLATE), f"{path}.{SqlNodeField.SQL_TEMPLATE}"),
            "sql_params": self.helpers.expect_dict(mapping.get(SqlNodeField.SQL_PARAMS, {}), f"{path}.{SqlNodeField.SQL_PARAMS}"),
            "outputs": self.helpers.parse_string_list(mapping.get(SqlNodeField.OUTPUTS, []), f"{path}.{SqlNodeField.OUTPUTS}"),
            "description": self.helpers.optional_string(mapping.get(SqlNodeField.DESCRIPTION), f"{path}.{SqlNodeField.DESCRIPTION}"),
        }

    def parse_variable_node_fields(self, mapping: Mapping[str, Any], path: str, node_type: NodeType) -> dict[str, Any]:
        when_path = f"{path}.{VariableField.WHEN}"
        when_items = self.helpers.expect_list(mapping.get(VariableField.WHEN, []), when_path)
        return {
            "type": node_type,
            "when": [self._parse_variable_condition(item, index, when_path) for index, item in enumerate(when_items)],
            "default": mapping.get(VariableField.DEFAULT),
            "default_specified": VariableField.DEFAULT in mapping,
            "description": self.helpers.optional_string(mapping.get(SqlNodeField.DESCRIPTION), f"{path}.{SqlNodeField.DESCRIPTION}"),
        }

    def _parse_variable_condition(self, item: Any, index: int, when_path: str) -> VariableCondition:
        condition_path = f"{when_path}[{index}]"
        condition_mapping = self.helpers.expect_dict(item, condition_path)
        return VariableCondition(
            condition=self.helpers.expect_string(
                condition_mapping.get(VariableField.CONDITION),
                f"{condition_path}.{VariableField.CONDITION}",
            ),
            value=condition_mapping.get(VariableField.VALUE),
            value_specified=VariableField.VALUE in condition_mapping,
        )

    def _parse_consumes(self, mapping: Mapping[str, Any], node_path: str) -> Sequence[ConsumeSpec]:
        consumes_path = f"{node_path}.{StepField.CONSUMES}"
        raw_consumes = self.helpers.expect_list(mapping.get(StepField.CONSUMES, []), consumes_path)
        return [
            self._parse_consume_spec(raw_consume, consume_index, consumes_path)
            for consume_index, raw_consume in enumerate(raw_consumes)
        ]

    def _parse_consume_spec(self, raw_consume: Any, index: int, consumes_path: str) -> ConsumeSpec:
        consume_path = f"{consumes_path}[{index}]"
        consume_mapping = self.helpers.expect_dict(raw_consume, consume_path)
        return ConsumeSpec(
            from_path=self.helpers.expect_string(
                consume_mapping.get(ConsumeField.FROM),
                f"{consume_path}.{ConsumeField.FROM}",
            ),
            alias=self.helpers.expect_string(
                consume_mapping.get(ConsumeField.ALIAS),
                f"{consume_path}.{ConsumeField.ALIAS}",
            ),
        )
