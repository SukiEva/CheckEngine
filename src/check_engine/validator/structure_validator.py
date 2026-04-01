"""ExecDSL 结构校验器。"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any, NoReturn, Optional

from ..dsl import (
    DslDocument,
    ReservedNodeName,
    TopLevelField,
    VariableField,
    EXISTS_DECISION,
    FAIL_MODE_FULL_REPEAT,
    FAIL_MODE_SINGLE,
    FAIL_MODE_SUB_REPEAT,
    NODE_TYPE_SQL,
    NODE_TYPE_VARIABLE,
    RESULT_MODE_RECORD,
    RESULT_MODE_RECORDS,
    FailPolicy,
    PassPolicy,
    StepNode,
    SqlNode,
    SqlStepNode,
    VariableStepNode,
    VariableDefinition,
)
from ..exceptions import DSLValidationError
from ..reference_parser import ReferenceParser
from ..step_registry import StepTypeRegistry, build_default_step_registry


class StructureValidator:
    """校验 DSL 的结构与基础约束。"""

    VALID_TOP_LEVEL_FIELDS = {field for field in TopLevelField}
    VALID_RESULT_MODES = {RESULT_MODE_RECORD, RESULT_MODE_RECORDS}
    VALID_FAIL_MODES = {FAIL_MODE_SUB_REPEAT, FAIL_MODE_FULL_REPEAT, FAIL_MODE_SINGLE}
    RESERVED_NODE_NAMES = {field for field in ReservedNodeName}
    ALIAS_PATTERN = re.compile(r"^[A-Za-z_]\w*$")
    def __init__(
        self,
        reference_parser: Optional[ReferenceParser] = None,
        step_registry: Optional[StepTypeRegistry] = None,
    ) -> None:
        self.reference_parser = reference_parser or ReferenceParser()
        self.step_registry = step_registry or build_default_step_registry()

    def validate(self, document: DslDocument) -> None:
        self._validate_top_level_fields(document.raw)
        self._validate_variables(document.variables)
        self._validate_steps(document.steps, document.raw.get(TopLevelField.STEPS, []))
        self._validate_fail_policy(document.on_fail, TopLevelField.ON_FAIL)

    def _validate_top_level_fields(self, raw: Mapping[str, object]) -> None:
        unknown_fields = sorted(set(raw.keys()) - self.VALID_TOP_LEVEL_FIELDS)
        if unknown_fields:
            self._raise(f"Unknown top-level fields are not allowed: {', '.join(unknown_fields)}")

    def _validate_variables(self, variables: Mapping[str, VariableDefinition]) -> None:
        for name, definition in variables.items():
            if not self.ALIAS_PATTERN.fullmatch(name):
                self._raise(f"variables.{name} must use a valid identifier name.")
            if not definition.default_specified:
                self._raise(f"variables.{name}.default is required.")
            for index, item in enumerate(definition.when):
                if not item.condition.strip():
                    self._raise(f"variables.{name}.when[{index}].condition must not be empty.")
                if not item.value_specified:
                    self._raise(f"variables.{name}.when[{index}].value is required.")

    def _validate_steps(self, steps: Sequence[StepNode], raw_steps: Any) -> None:
        if not isinstance(raw_steps, Sequence):
            self._raise("steps must be a list.")
        names = set()
        for index, node in enumerate(steps):
            if node.name in names:
                self._raise(f"steps[{index}].name is duplicated: {node.name}")
            names.add(node.name)
            self._validate_node_name(node.name, f"steps[{index}].name")
            raw_step = raw_steps[index] if index < len(raw_steps) else {}
            definition = self.step_registry.get(node.type)
            if definition is None:
                self._raise(f"steps[{index}].type is not supported: {node.type}")
            definition.validate_structure(self, node, raw_step, f"steps[{index}]")
            if node.on_fail is not None:
                self._validate_fail_policy(node.on_fail, f"steps[{index}].on_fail")
            if node.on_pass is not None:
                self._validate_pass_policy(node.on_pass, f"steps[{index}].on_pass")
            aliases = set()
            for consume_index, consume in enumerate(node.consumes):
                if not consume.from_path.strip():
                    self._raise(f"steps[{index}].consumes[{consume_index}].from must not be empty.")
                if not consume.alias.strip():
                    self._raise(f"steps[{index}].consumes[{consume_index}].alias must not be empty.")
                if not self.ALIAS_PATTERN.fullmatch(consume.alias):
                    self._raise(f"steps[{index}].consumes[{consume_index}].alias must be a valid SQL identifier.")
                if consume.alias in aliases:
                    self._raise(f"steps[{index}].consumes[{consume_index}].alias is duplicated: {consume.alias}")
                aliases.add(consume.alias)
            if isinstance(node, VariableStepNode) and node.consumes:
                self._raise(f"steps[{index}].consumes is not supported when type is variable.")

    def _validate_sql_node(self, node: SqlNode, path: str) -> None:
        if node.result_mode not in self.VALID_RESULT_MODES:
            self._raise(f"{path}.result_mode is not supported: {node.result_mode}")
        if not node.datasource.strip():
            self._raise(f"{path}.datasource must not be empty.")
        if not node.sql_template.strip():
            self._raise(f"{path}.sql_template must not be empty.")
        self._validate_outputs(node.outputs, f"{path}.outputs")

    def validate_sql_step_structure(self, node: StepNode, path: str) -> None:
        if not isinstance(node, SqlStepNode):
            self._raise(f"{path} node class does not match sql type.")
        self._validate_sql_node(node, path)
        self._validate_outputs(node.outputs, f"{path}.outputs")

    def validate_variable_step_structure(self, node: StepNode, path: str) -> None:
        if not isinstance(node, VariableStepNode):
            self._raise(f"{path} node class does not match variable type.")
        if not node.default_specified:
            self._raise(f"{path}.default is required.")
        for index, condition in enumerate(node.when):
            if not condition.condition.strip():
                self._raise(f"{path}.when[{index}].condition must not be empty.")
            if not condition.value_specified:
                self._raise(f"{path}.when[{index}].value is required.")

    def _validate_outputs(self, outputs: Sequence[str], path: str) -> None:
        seen = set()
        for index, output in enumerate(outputs):
            if output in seen:
                self._raise(f"{path}[{index}] is duplicated: {output}")
            if not self.ALIAS_PATTERN.fullmatch(output):
                self._raise(f"{path}[{index}] must be a valid SQL identifier.")
            seen.add(output)

    def _validate_fail_policy(self, policy: FailPolicy, path: str) -> None:
        if policy.mode not in self.VALID_FAIL_MODES:
            self._raise(f"{path}.mode is not supported: {policy.mode}")
        decision = policy.decision.strip()
        if not decision:
            self._raise(f"{path}.decision must not be empty.")
        if decision == EXISTS_DECISION:
            self._raise(f"{path}.decision does not support bare 'exists'; use exists($path) instead.")
        if decision == "not exists":
            self._raise(f"{path}.decision does not support bare 'not exists'; use not exists($path) instead.")
        if decision != EXISTS_DECISION and decision.startswith(EXISTS_DECISION) and not self.reference_parser.is_exists_call(decision):
            self._raise(f"{path}.decision exists syntax is invalid: {policy.decision}")
        if decision.startswith("not exists") and not self.reference_parser.is_not_exists_call(decision):
            self._raise(f"{path}.decision not exists syntax is invalid: {policy.decision}")
        if not policy.message_cn.strip():
            self._raise(f"{path}.message_cn must not be empty.")
        if not policy.message_en.strip():
            self._raise(f"{path}.message_en must not be empty.")
        self._validate_optional_non_empty_string(policy.divider, f"{path}.divider")
        self._validate_optional_non_empty_string(policy.divider_cn, f"{path}.divider_cn")
        self._validate_optional_non_empty_string(policy.divider_en, f"{path}.divider_en")

        if policy.mode == FAIL_MODE_SUB_REPEAT:
            if policy.divider is None and (policy.divider_cn is None or policy.divider_en is None):
                self._raise(f"{path} must provide divider, or provide both divider_cn and divider_en when mode is sub_repeat.")
            self._validate_repeat_segment(policy.message_cn, f"{path}.message_cn")
            self._validate_repeat_segment(policy.message_en, f"{path}.message_en")

    def _validate_optional_non_empty_string(self, value: Any, path: str) -> None:
        if value is None:
            return
        if value == "":
            self._raise(f"{path} must not be an empty string.")

    def _validate_repeat_segment(self, template: str, path: str) -> None:
        left_count = template.count("[")
        right_count = template.count("]")
        if left_count != 1 or right_count != 1:
            self._raise(f"{path} must contain exactly one [] segment.")
        if template.index("[") > template.index("]"):
            self._raise(f"{path} has invalid [] ordering.")

    def _validate_pass_policy(self, policy: PassPolicy, path: str) -> None:
        decision = policy.decision.strip()
        if not decision:
            self._raise(f"{path}.decision must not be empty.")
        if decision == EXISTS_DECISION:
            self._raise(f"{path}.decision does not support bare 'exists'; use exists($path) instead.")
        if decision == "not exists":
            self._raise(f"{path}.decision does not support bare 'not exists'; use not exists($path) instead.")
        if decision.startswith(EXISTS_DECISION) and not self.reference_parser.is_exists_call(decision):
            self._raise(f"{path}.decision exists syntax is invalid: {policy.decision}")
        if decision.startswith("not exists") and not self.reference_parser.is_not_exists_call(decision):
            self._raise(f"{path}.decision not exists syntax is invalid: {policy.decision}")

    def _validate_node_name(self, name: str, path: str) -> None:
        if name in self.RESERVED_NODE_NAMES:
            self._raise(f"{path} uses reserved node name: {name}")
        if not self.ALIAS_PATTERN.fullmatch(name):
            self._raise(f"{path} must be a valid identifier.")

    @staticmethod
    def _raise(message: str) -> NoReturn:
        raise DSLValidationError(message)
