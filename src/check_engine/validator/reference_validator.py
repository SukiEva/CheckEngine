"""ExecDSL 引用校验器。"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Callable, NoReturn, Optional

from ..dsl import (
    DslDocument,
    FAIL_MODE_SINGLE,
    NODE_TYPE_SQL,
    NODE_TYPE_VARIABLE,
    PassPolicy,
    RESULT_MODE_RECORDS,
    FailPolicy,
    SqlStepNode,
    StepNode,
    VariableDefinition,
    VariableStepNode,
)
from ..exceptions import DSLValidationError


class ReferenceValidator:
    """校验 DSL 中的作用域引用是否合法。"""

    PATH_PATTERN = re.compile(r"\$(?:\.(?:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)?|[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)")
    EXISTS_CALL_PATTERN = re.compile(
        r"(?:not\s+)?exists\(\s*(\$(?:\.(?:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)?|[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*))\s*\)"
    )

    def __init__(self) -> None:
        self.step_reference_validators: dict[str, Callable[[StepNode, DslDocument, set[str], set[str], dict[str, StepNode], str], None]] = {
            NODE_TYPE_SQL: self._validate_sql_step_references,
            NODE_TYPE_VARIABLE: self._validate_variable_step_references,
        }

    def validate(self, document: DslDocument) -> None:
        step_names = tuple(step.name for step in document.steps)
        step_map = {step.name: step for step in document.steps}
        available_variables: set[str] = set()
        all_variables = set(document.variables.keys())

        for variable_name, definition in document.variables.items():
            self._validate_variable_definition(variable_name, definition, document, available_variables, step_map)
            available_variables.add(variable_name)

        available_steps: set[str] = set()
        for index, step in enumerate(document.steps):
            step_reference_validator = self.step_reference_validators.get(step.type)
            if step_reference_validator is not None:
                step_reference_validator(
                    step,
                    document,
                    available_steps,
                    all_variables,
                    step_map,
                    f"steps[{index}]",
                )
            if step.on_fail is not None:
                local_outputs = set(step.outputs) if isinstance(step, SqlStepNode) else None
                self._validate_fail_policy(
                    step.on_fail,
                    document,
                    available_steps=available_steps,
                    available_variables=all_variables,
                    path=f"steps[{index}].on_fail",
                    step_map=step_map,
                    local_outputs=local_outputs,
                )
            if step.on_pass is not None:
                local_outputs = set(step.outputs) if isinstance(step, SqlStepNode) else None
                self._validate_pass_policy(
                    step.on_pass,
                    document,
                    available_steps=available_steps,
                    available_variables=all_variables,
                    path=f"steps[{index}].on_pass",
                    step_map=step_map,
                    local_outputs=local_outputs,
                )
            available_steps.add(step.name)

        self._validate_fail_policy(
            document.on_fail,
            document,
            available_steps=set(step_names),
            available_variables=all_variables,
            path="on_fail",
            step_map=step_map,
            local_outputs=None,
        )

    def _validate_variable_definition(
        self,
        variable_name: str,
        definition: VariableDefinition,
        document: DslDocument,
        available_variables: set[str],
        step_map: dict[str, StepNode],
        available_steps: Optional[set[str]] = None,
        path_prefix: Optional[str] = None,
    ) -> None:
        variable_path_prefix = path_prefix or f"variables.{variable_name}"
        for index, condition in enumerate(definition.when):
            for reference in self._extract_references(condition.condition):
                self._validate_reference(
                    reference,
                    document,
                    available_steps=available_steps or set(),
                    available_variables=available_variables,
                    path=f"{variable_path_prefix}.when[{index}].condition",
                    step_map=step_map,
                    local_outputs=None,
                )

    def _validate_sql_params(
        self,
        sql_params: Mapping[str, object],
        document: DslDocument,
        available_steps: set[str],
        available_variables: set[str],
        step_map: dict[str, StepNode],
        path_prefix: str = "sql_params",
    ) -> None:
        for key, value in sql_params.items():
            if isinstance(value, str) and value.startswith("$"):
                self._validate_reference(
                    value,
                    document,
                    available_steps=available_steps,
                    available_variables=available_variables,
                    path=f"{path_prefix}.{key}",
                    step_map=step_map,
                    local_outputs=None,
                )

    def _validate_consumes(
        self,
        step: StepNode,
        available_steps: set[str],
        _document: DslDocument,
        step_map: dict[str, StepNode],
    ) -> None:
        for consume in step.consumes:
            parts = self._split_reference(consume.from_path)
            if len(parts) != 2 or parts[0] != "steps":
                self._raise(f"Invalid consumes.from reference: {consume.from_path}")
            if parts[1] not in available_steps:
                self._raise(f"consumes.from references a step that has not executed yet: {consume.from_path}")
            source_step = self._find_step(step_map, parts[1])
            if not isinstance(source_step, SqlStepNode) or not source_step.outputs:
                self._raise(f"consumes.from references step outputs that are not declared: {consume.from_path}")

    def _validate_sql_step_references(
        self,
        step: StepNode,
        document: DslDocument,
        available_steps: set[str],
        available_variables: set[str],
        step_map: dict[str, StepNode],
        path_prefix: str,
    ) -> None:
        if not isinstance(step, SqlStepNode):
            return
        self._validate_sql_params(
            step.sql_params,
            document,
            available_steps=available_steps,
            available_variables=available_variables,
            step_map=step_map,
            path_prefix=f"{path_prefix}.sql_params",
        )
        self._validate_consumes(step, available_steps, document, step_map)

    def _validate_variable_step_references(
        self,
        step: StepNode,
        document: DslDocument,
        available_steps: set[str],
        available_variables: set[str],
        step_map: dict[str, StepNode],
        path_prefix: str,
    ) -> None:
        if not isinstance(step, VariableStepNode):
            return
        self._validate_variable_definition(
            step.name,
            VariableDefinition(when=step.when, default=step.default),
            document,
            available_variables=available_variables,
            available_steps=available_steps,
            step_map=step_map,
            path_prefix=path_prefix,
        )

    def _validate_fail_policy(
        self,
        policy: FailPolicy,
        document: DslDocument,
        available_steps: set[str],
        available_variables: set[str],
        path: str,
        step_map: dict[str, StepNode],
        local_outputs: Optional[set[str]],
    ) -> None:
        exists_argument_references = self._extract_exists_argument_references(policy.decision)
        for reference in self._extract_references(policy.decision):
            self._validate_reference(
                reference,
                document,
                available_steps,
                available_variables=available_variables,
                path=f"{path}.decision",
                step_map=step_map,
                local_outputs=local_outputs,
                allow_sql_step_root=reference in exists_argument_references,
            )

        for field_name, template in (("message_cn", policy.message_cn), ("message_en", policy.message_en)):
            for reference in self._extract_references(template):
                self._validate_reference(
                    reference,
                    document,
                    available_steps,
                    available_variables=available_variables,
                    path=f"{path}.{field_name}",
                    step_map=step_map,
                    local_outputs=local_outputs,
                )
                if policy.mode == FAIL_MODE_SINGLE:
                    self._validate_single_mode_message_reference(reference, step_map, f"{path}.{field_name}")

    def _validate_pass_policy(
        self,
        policy: PassPolicy,
        document: DslDocument,
        available_steps: set[str],
        available_variables: set[str],
        path: str,
        step_map: dict[str, StepNode],
        local_outputs: Optional[set[str]],
    ) -> None:
        exists_argument_references = self._extract_exists_argument_references(policy.decision)
        for reference in self._extract_references(policy.decision):
            self._validate_reference(
                reference,
                document,
                available_steps=available_steps,
                available_variables=available_variables,
                path=f"{path}.decision",
                step_map=step_map,
                local_outputs=local_outputs,
                allow_sql_step_root=reference in exists_argument_references,
            )

    def _validate_reference(
        self,
        reference: str,
        document: DslDocument,
        available_steps: set[str],
        available_variables: set[str],
        path: str,
        step_map: dict[str, StepNode],
        local_outputs: Optional[set[str]],
        allow_sql_step_root: bool = False,
    ) -> None:
        parts = self._split_reference(reference)
        if not parts:
            self._raise(f"{path} contains invalid reference: {reference}")

        if parts[0] == "":
            self._validate_local_reference(
                reference,
                parts,
                path,
                local_outputs,
                allow_local_root=allow_sql_step_root,
            )
            return

        root = parts[0]
        if root == "input":
            if len(parts) < 2:
                self._raise(f"{path} input reference must include a field: {reference}")
            return

        if root == "variables":
            if len(parts) != 2:
                self._raise(f"{path} variables reference has invalid depth: {reference}")
            if parts[1] not in available_variables:
                self._raise(f"{path} references a variable not available at this point: {reference}")
            return

        if root == "steps":
            if len(parts) not in (2, 3):
                self._raise(f"{path} steps reference has invalid depth: {reference}")
            step_name = parts[1]
            if step_name not in available_steps:
                self._raise(f"{path} references a step not available at this point: {reference}")
            step = self._find_step(step_map, step_name)
            if len(parts) == 2:
                if step.type == NODE_TYPE_VARIABLE:
                    return
                if allow_sql_step_root and step.type == NODE_TYPE_SQL:
                    if not isinstance(step, SqlStepNode):
                        self._raise(f"{path} references an unsupported step type: {reference}")
                    if not step.outputs:
                        self._raise(f"{path} references step outputs that are not declared: {reference}")
                    return
                if step.type != NODE_TYPE_VARIABLE:
                    self._raise(f"{path} sql step reference must include exported field: {reference}")
            if step.type == NODE_TYPE_VARIABLE:
                self._raise(f"{path} variable step reference only supports $steps.<name>: {reference}")
            if not isinstance(step, SqlStepNode):
                self._raise(f"{path} references an unsupported step type: {reference}")
            field_name = parts[2]
            if not step.outputs:
                self._raise(f"{path} references step outputs that are not declared: {reference}")
            if field_name not in step.outputs:
                self._raise(f"{path} references a non-exported step field: {reference}")
            return

        self._raise(f"{path} contains unknown scope: {reference}")

    def _validate_local_reference(
        self,
        reference: str,
        parts: list[str],
        path: str,
        local_outputs: Optional[set[str]],
        allow_local_root: bool = False,
    ) -> None:
        if local_outputs is None:
            self._raise(f"{path} cannot use local scope reference: {reference}")
        if len(parts) == 2 and parts[1] == "":
            if allow_local_root:
                return
            self._raise(f"{path} local reference must include a field: {reference}")
        if len(parts) < 2:
            self._raise(f"{path} local reference must include a field: {reference}")
        field_name = parts[1]
        if field_name not in local_outputs:
            self._raise(f"{path} references a non-exported local field: {reference}")

    def _validate_single_mode_message_reference(self, reference: str, step_map: dict[str, StepNode], path: str) -> None:
        parts = self._split_reference(reference)
        if len(parts) == 3 and parts[0] == "steps":
            step = self._find_step(step_map, parts[1])
            if isinstance(step, SqlStepNode) and step.result_mode == RESULT_MODE_RECORDS:
                self._raise(f"{path} cannot reference array outputs in single mode: {reference}")

    def _extract_references(self, text: str) -> list[str]:
        return self.PATH_PATTERN.findall(text)

    def _extract_exists_argument_references(self, decision: str) -> set[str]:
        return {match.group(1) for match in self.EXISTS_CALL_PATTERN.finditer(decision)}

    @staticmethod
    def _split_reference(reference: str) -> list[str]:
        return reference[1:].split(".") if reference.startswith("$") else []

    def _find_step(self, step_map: dict[str, StepNode], step_name: str) -> StepNode:
        if step_name in step_map:
            return step_map[step_name]
        self._raise(f"Step not found: {step_name}")
        raise AssertionError("unreachable")

    @staticmethod
    def _raise(message: str) -> NoReturn:
        raise DSLValidationError(message)
