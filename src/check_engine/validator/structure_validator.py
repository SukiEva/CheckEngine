"""ExecDSL 结构校验器。"""

from __future__ import annotations

import re

from ..dsl.models import ContextNode, DslDocument, FailPolicy, PrecheckNode, SqlNode, StepNode, VariableDefinition
from ..exceptions import DSLValidationError


class StructureValidator:
    """校验 DSL 的结构与基础约束。"""

    VALID_SQL_NODE_TYPES = {"sql"}
    VALID_RESULT_MODES = {"record", "records"}
    VALID_FAIL_MODES = {"sub_repeat", "full_repeat", "single"}
    VALID_VARIABLE_TYPES = {"assign_by_condition"}
    EXISTS_CALL_PATTERN = re.compile(r"^exists\(\s*\$[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*\s*\)$")

    def validate(self, document: DslDocument) -> None:
        if document.context is not None:
            self._validate_context(document.context)
        self._validate_variables(document.variables)
        self._validate_prechecks(document.prechecks)
        self._validate_steps(document.steps)
        self._validate_fail_policy(document.on_fail, "on_fail")

    def _validate_context(self, context: ContextNode) -> None:
        self._validate_sql_node(context, "context")
        if not context.outputs:
            raise DSLValidationError("context.outputs must not be empty.")

    def _validate_variables(self, variables: dict[str, VariableDefinition]) -> None:
        for name, definition in variables.items():
            if definition.type not in self.VALID_VARIABLE_TYPES:
                raise DSLValidationError(f"variables.{name}.type is not supported: {definition.type}")
            if not definition.when:
                raise DSLValidationError(f"variables.{name}.when must not be empty.")
            for index, item in enumerate(definition.when):
                if not item.condition.strip():
                    raise DSLValidationError(f"variables.{name}.when[{index}].condition must not be empty.")

    def _validate_prechecks(self, prechecks: list[PrecheckNode]) -> None:
        names = set()
        for index, node in enumerate(prechecks):
            if node.name in names:
                raise DSLValidationError(f"prechecks[{index}].name is duplicated: {node.name}")
            names.add(node.name)
            self._validate_sql_node(node, f"prechecks[{index}]")
            if node.on_fail is None:
                raise DSLValidationError(f"prechecks[{index}].on_fail must not be empty.")
            self._validate_fail_policy(node.on_fail, f"prechecks[{index}].on_fail")

    def _validate_steps(self, steps: list[StepNode]) -> None:
        names = set()
        for index, node in enumerate(steps):
            if node.name in names:
                raise DSLValidationError(f"steps[{index}].name is duplicated: {node.name}")
            names.add(node.name)
            self._validate_sql_node(node, f"steps[{index}]")
            self._validate_outputs(node.outputs, f"steps[{index}].outputs")
            for consume_index, consume in enumerate(node.consumes):
                if not consume.from_path.strip():
                    raise DSLValidationError(f"steps[{index}].consumes[{consume_index}].from must not be empty.")
                if not consume.alias.strip():
                    raise DSLValidationError(f"steps[{index}].consumes[{consume_index}].alias must not be empty.")

    def _validate_sql_node(self, node: SqlNode, path: str) -> None:
        if node.type not in self.VALID_SQL_NODE_TYPES:
            raise DSLValidationError(f"{path}.type is not supported: {node.type}")
        if node.result_mode not in self.VALID_RESULT_MODES:
            raise DSLValidationError(f"{path}.result_mode is not supported: {node.result_mode}")
        if not node.datasource.strip():
            raise DSLValidationError(f"{path}.datasource must not be empty.")
        if not node.sql_template.strip():
            raise DSLValidationError(f"{path}.sql_template must not be empty.")
        self._validate_outputs(node.outputs, f"{path}.outputs")

    def _validate_outputs(self, outputs: list[str], path: str) -> None:
        seen = set()
        for index, output in enumerate(outputs):
            if output in seen:
                raise DSLValidationError(f"{path}[{index}] is duplicated: {output}")
            seen.add(output)

    def _validate_fail_policy(self, policy: FailPolicy, path: str) -> None:
        if policy.mode not in self.VALID_FAIL_MODES:
            raise DSLValidationError(f"{path}.mode is not supported: {policy.mode}")
        decision = policy.decision.strip()
        if not decision:
            raise DSLValidationError(f"{path}.decision must not be empty.")
        if decision == "exists" and path == "on_fail":
            raise DSLValidationError("on_fail.decision does not support bare 'exists'; use exists($path) instead.")
        if decision != "exists" and decision.startswith("exists") and not self.EXISTS_CALL_PATTERN.fullmatch(decision):
            raise DSLValidationError(f"{path}.decision exists syntax is invalid: {policy.decision}")
        if not policy.message_cn.strip():
            raise DSLValidationError(f"{path}.message_cn must not be empty.")
        if not policy.message_en.strip():
            raise DSLValidationError(f"{path}.message_en must not be empty.")

        if policy.mode == "sub_repeat":
            if policy.divider is None:
                raise DSLValidationError(f"{path}.divider must not be empty.")
            self._validate_repeat_segment(policy.message_cn, f"{path}.message_cn")
            self._validate_repeat_segment(policy.message_en, f"{path}.message_en")

    def _validate_repeat_segment(self, template: str, path: str) -> None:
        left_count = template.count("[")
        right_count = template.count("]")
        if left_count != 1 or right_count != 1:
            raise DSLValidationError(f"{path} must contain exactly one [] segment.")
        if template.index("[") > template.index("]"):
            raise DSLValidationError(f"{path} has invalid [] ordering.")
