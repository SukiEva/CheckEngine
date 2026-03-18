"""ExecDSL 结构校验器。"""

from __future__ import annotations

import re
from typing import Mapping, NoReturn, Sequence

from ..dsl import ContextNode, DslDocument, FailPolicy, PrecheckNode, SqlNode, StepNode, VariableDefinition
from ..exceptions import DSLValidationError, ValidationErrorCode


class StructureValidator:
    """校验 DSL 的结构与基础约束。"""

    VALID_TOP_LEVEL_FIELDS = {"context", "variables", "prechecks", "steps", "on_fail"}
    VALID_SQL_NODE_TYPES = {"sql"}
    VALID_RESULT_MODES = {"record", "records"}
    VALID_FAIL_MODES = {"sub_repeat", "full_repeat", "single"}
    RESERVED_NODE_NAMES = {"input", "context", "variables", "steps", "on_fail"}
    ALIAS_PATTERN = re.compile(r"^[A-Za-z_]\w*$")
    EXISTS_CALL_PATTERN = re.compile(r"^exists\(\s*\$[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*\s*\)$")

    def validate(self, document: DslDocument) -> None:
        self._validate_top_level_fields(document.raw)
        if document.context is not None:
            self._validate_context(document.context)
        self._validate_variables(document.variables, document.raw.get("variables", {}))
        self._validate_prechecks(document.prechecks)
        self._validate_steps(document.steps)
        self._validate_global_node_names(document.prechecks, document.steps)
        self._validate_fail_policy(document.on_fail, "on_fail")

    def _validate_top_level_fields(self, raw: Mapping[str, object]) -> None:
        unknown_fields = sorted(set(raw.keys()) - self.VALID_TOP_LEVEL_FIELDS)
        if unknown_fields:
            self._raise(
                ValidationErrorCode.UNKNOWN_TOP_LEVEL_FIELD,
                f"Unknown top-level fields are not allowed: {', '.join(unknown_fields)}",
            )

    def _validate_context(self, context: ContextNode) -> None:
        self._validate_sql_node(context, "context")
        if not context.outputs:
            self._raise(ValidationErrorCode.MISSING_OUTPUTS, "context.outputs must not be empty.")

    def _validate_variables(self, variables: Mapping[str, VariableDefinition], raw_variables: Mapping[str, object]) -> None:
        for name, definition in variables.items():
            raw_definition = raw_variables.get(name)
            if not isinstance(raw_definition, Mapping) or "default" not in raw_definition:
                self._raise(ValidationErrorCode.MISSING_REQUIRED_FIELD, f"variables.{name}.default is required.")
            for index, item in enumerate(definition.when):
                if not item.condition.strip():
                    self._raise(
                        ValidationErrorCode.INVALID_EXPRESSION,
                        f"variables.{name}.when[{index}].condition must not be empty.",
                    )

    def _validate_prechecks(self, prechecks: Sequence[PrecheckNode]) -> None:
        names = set()
        for index, node in enumerate(prechecks):
            if node.name in names:
                self._raise(ValidationErrorCode.DUPLICATE_NODE_NAME, f"prechecks[{index}].name is duplicated: {node.name}")
            names.add(node.name)
            self._validate_node_name(node.name, f"prechecks[{index}].name")
            self._validate_sql_node(node, f"prechecks[{index}]")
            if node.on_fail is None:
                self._raise(ValidationErrorCode.MISSING_REQUIRED_FIELD, f"prechecks[{index}].on_fail must not be empty.")
            self._validate_fail_policy(node.on_fail, f"prechecks[{index}].on_fail")
            if node.on_fail.decision not in {"exists"} and not self.EXISTS_CALL_PATTERN.fullmatch(node.on_fail.decision.strip()):
                self._raise(
                    ValidationErrorCode.INVALID_EXPRESSION,
                    f"prechecks[{index}].on_fail.decision only supports 'exists' or 'exists($path)'."
                )

    def _validate_steps(self, steps: Sequence[StepNode]) -> None:
        names = set()
        for index, node in enumerate(steps):
            if node.name in names:
                self._raise(ValidationErrorCode.DUPLICATE_NODE_NAME, f"steps[{index}].name is duplicated: {node.name}")
            names.add(node.name)
            self._validate_node_name(node.name, f"steps[{index}].name")
            self._validate_sql_node(node, f"steps[{index}]")
            self._validate_outputs(node.outputs, f"steps[{index}].outputs")
            aliases = set()
            for consume_index, consume in enumerate(node.consumes):
                if not consume.from_path.strip():
                    self._raise(
                        ValidationErrorCode.MISSING_REQUIRED_FIELD,
                        f"steps[{index}].consumes[{consume_index}].from must not be empty.",
                    )
                if not consume.alias.strip():
                    self._raise(
                        ValidationErrorCode.MISSING_REQUIRED_FIELD,
                        f"steps[{index}].consumes[{consume_index}].alias must not be empty.",
                    )
                if not self.ALIAS_PATTERN.fullmatch(consume.alias):
                    self._raise(
                        ValidationErrorCode.INVALID_CONSUMES_ALIAS,
                        f"steps[{index}].consumes[{consume_index}].alias must be a valid SQL identifier."
                    )
                if consume.alias in aliases:
                    self._raise(
                        ValidationErrorCode.INVALID_CONSUMES_ALIAS,
                        f"steps[{index}].consumes[{consume_index}].alias is duplicated: {consume.alias}",
                    )
                aliases.add(consume.alias)

    def _validate_sql_node(self, node: SqlNode, path: str) -> None:
        if node.type not in self.VALID_SQL_NODE_TYPES:
            self._raise(ValidationErrorCode.INVALID_FIELD_TYPE, f"{path}.type is not supported: {node.type}")
        if node.result_mode not in self.VALID_RESULT_MODES:
            self._raise(ValidationErrorCode.INVALID_RESULT_MODE, f"{path}.result_mode is not supported: {node.result_mode}")
        if not node.datasource.strip():
            self._raise(ValidationErrorCode.MISSING_REQUIRED_FIELD, f"{path}.datasource must not be empty.")
        if not node.sql_template.strip():
            self._raise(ValidationErrorCode.MISSING_REQUIRED_FIELD, f"{path}.sql_template must not be empty.")
        self._validate_outputs(node.outputs, f"{path}.outputs")

    def _validate_outputs(self, outputs: Sequence[str], path: str) -> None:
        seen = set()
        for index, output in enumerate(outputs):
            if output in seen:
                self._raise(ValidationErrorCode.INVALID_FIELD_TYPE, f"{path}[{index}] is duplicated: {output}")
            seen.add(output)

    def _validate_fail_policy(self, policy: FailPolicy, path: str) -> None:
        if policy.mode not in self.VALID_FAIL_MODES:
            self._raise(ValidationErrorCode.INVALID_FIELD_TYPE, f"{path}.mode is not supported: {policy.mode}")
        decision = policy.decision.strip()
        if not decision:
            self._raise(ValidationErrorCode.MISSING_REQUIRED_FIELD, f"{path}.decision must not be empty.")
        if decision == "exists" and path == "on_fail":
            self._raise(
                ValidationErrorCode.INVALID_EXPRESSION,
                "on_fail.decision does not support bare 'exists'; use exists($path) instead.",
            )
        if decision != "exists" and decision.startswith("exists") and not self.EXISTS_CALL_PATTERN.fullmatch(decision):
            self._raise(ValidationErrorCode.INVALID_EXPRESSION, f"{path}.decision exists syntax is invalid: {policy.decision}")
        if not policy.message_cn.strip():
            self._raise(ValidationErrorCode.MISSING_REQUIRED_FIELD, f"{path}.message_cn must not be empty.")
        if not policy.message_en.strip():
            self._raise(ValidationErrorCode.MISSING_REQUIRED_FIELD, f"{path}.message_en must not be empty.")

        if policy.mode == "sub_repeat":
            if policy.divider is None and (policy.divider_cn is None or policy.divider_en is None):
                self._raise(
                    ValidationErrorCode.INVALID_MESSAGE_TEMPLATE,
                    f"{path} must provide divider, or provide both divider_cn and divider_en when mode is sub_repeat."
                )
            self._validate_repeat_segment(policy.message_cn, f"{path}.message_cn")
            self._validate_repeat_segment(policy.message_en, f"{path}.message_en")

    def _validate_repeat_segment(self, template: str, path: str) -> None:
        left_count = template.count("[")
        right_count = template.count("]")
        if left_count != 1 or right_count != 1:
            self._raise(ValidationErrorCode.INVALID_MESSAGE_TEMPLATE, f"{path} must contain exactly one [] segment.")
        if template.index("[") > template.index("]"):
            self._raise(ValidationErrorCode.INVALID_MESSAGE_TEMPLATE, f"{path} has invalid [] ordering.")

    def _validate_node_name(self, name: str, path: str) -> None:
        if name in self.RESERVED_NODE_NAMES:
            self._raise(ValidationErrorCode.RESERVED_NODE_NAME, f"{path} uses reserved node name: {name}")

    def _validate_global_node_names(self, prechecks: Sequence[PrecheckNode], steps: Sequence[StepNode]) -> None:
        precheck_names = {node.name for node in prechecks}
        for index, node in enumerate(steps):
            if node.name in precheck_names:
                self._raise(ValidationErrorCode.DUPLICATE_NODE_NAME, f"steps[{index}].name conflicts with precheck name: {node.name}")

    @staticmethod
    def _raise(code: ValidationErrorCode, message: str) -> NoReturn:
        raise DSLValidationError(message, code=code)
