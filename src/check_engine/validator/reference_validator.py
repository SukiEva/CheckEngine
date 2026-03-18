"""ExecDSL 引用校验器。"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import NoReturn

from ..dsl.models import ContextNode, DslDocument, FailPolicy, StepNode, VariableDefinition
from ..exceptions import DSLValidationError, ValidationErrorCode


class ReferenceValidator:
    """校验 DSL 中的作用域引用是否合法。"""

    PATH_PATTERN = re.compile(r"\$[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*")

    def validate(self, document: DslDocument) -> None:
        step_names = tuple(step.name for step in document.steps)
        step_map = {step.name: step for step in document.steps}
        available_variables: set[str] = set()
        all_variables = set(document.variables.keys())

        if document.context is not None:
            self._validate_context(document.context, document, step_map)

        for variable_name, definition in document.variables.items():
            self._validate_variable_definition(variable_name, definition, document, available_variables, step_map)
            available_variables.add(variable_name)

        for index, precheck in enumerate(document.prechecks):
            self._validate_sql_params(precheck.sql_params, document, available_steps=set(), available_variables=all_variables, step_map=step_map)
            self._validate_fail_policy(precheck.on_fail, document, available_steps=set(), available_variables=all_variables, path=f"prechecks[{index}].on_fail", step_map=step_map)

        available_steps: set[str] = set()
        for index, step in enumerate(document.steps):
            self._validate_sql_params(
                step.sql_params,
                document,
                available_steps=available_steps,
                available_variables=all_variables,
                step_map=step_map,
            )
            self._validate_consumes(step, available_steps, document, step_map)
            available_steps.add(step.name)

        self._validate_fail_policy(
            document.on_fail,
            document,
            available_steps=set(step_names),
            available_variables=all_variables,
            path="on_fail",
            step_map=step_map,
        )

    def _validate_context(self, context: ContextNode, document: DslDocument, step_map: dict[str, StepNode]) -> None:
        self._validate_sql_params(
            context.sql_params,
            document,
            available_steps=set(),
            available_variables=set(),
            step_map=step_map,
            path_prefix="context.sql_params",
        )

    def _validate_variable_definition(
        self,
        variable_name: str,
        definition: VariableDefinition,
        document: DslDocument,
        available_variables: set[str],
        step_map: dict[str, StepNode],
    ) -> None:
        for index, condition in enumerate(definition.when):
            for reference in self._extract_references(condition.condition):
                self._validate_reference(
                    reference,
                    document,
                    available_steps=set(),
                    available_variables=available_variables,
                    path=f"variables.{variable_name}.when[{index}].condition",
                    step_map=step_map,
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
                )

    def _validate_consumes(self, step: StepNode, available_steps: set[str], document: DslDocument, step_map: dict[str, StepNode]) -> None:
        for consume in step.consumes:
            if consume.from_path == "$context":
                if document.context is None:
                    self._raise(
                        ValidationErrorCode.INVALID_CONSUMES_REF,
                        "consumes.from references $context but context block is not defined.",
                    )
                if not document.context.outputs:
                    self._raise(
                        ValidationErrorCode.MISSING_OUTPUTS,
                        "consumes.from references $context but context.outputs are not declared.",
                    )
                continue

            parts = self._split_reference(consume.from_path)
            if len(parts) != 2 or parts[0] != "steps":
                self._raise(ValidationErrorCode.INVALID_CONSUMES_REF, f"Invalid consumes.from reference: {consume.from_path}")
            if parts[1] not in available_steps:
                self._raise(
                    ValidationErrorCode.INVALID_CONSUMES_REF,
                    f"consumes.from references a step that has not executed yet: {consume.from_path}",
                )
            source_step = self._find_step(step_map, parts[1])
            if not source_step.outputs:
                self._raise(
                    ValidationErrorCode.MISSING_OUTPUTS,
                    f"consumes.from references step outputs that are not declared: {consume.from_path}",
                )

    def _validate_fail_policy(
        self,
        policy: FailPolicy,
        document: DslDocument,
        available_steps: set[str],
        available_variables: set[str],
        path: str,
        step_map: dict[str, StepNode],
    ) -> None:
        if policy.decision != "exists":
            for reference in self._extract_references(policy.decision):
                self._validate_reference(
                    reference,
                    document,
                    available_steps,
                    available_variables=available_variables,
                    path=f"{path}.decision",
                    step_map=step_map,
                )

        for field_name, template in (
            ("message_cn", policy.message_cn),
            ("message_en", policy.message_en),
        ):
            for reference in self._extract_references(template):
                self._validate_reference(
                    reference,
                    document,
                    available_steps,
                    available_variables=available_variables,
                    path=f"{path}.{field_name}",
                    step_map=step_map,
                )
                if path == "on_fail" and policy.mode == "single":
                    self._validate_single_mode_message_reference(reference, step_map, f"{path}.{field_name}")

    def _validate_reference(
        self,
        reference: str,
        document: DslDocument,
        available_steps: set[str],
        available_variables: set[str],
        path: str,
        step_map: dict[str, StepNode],
    ) -> None:
        parts = self._split_reference(reference)
        if not parts:
            self._raise(ValidationErrorCode.UNRESOLVED_PATH, f"{path} contains invalid reference: {reference}")

        root = parts[0]
        if root == "input":
            if len(parts) < 2:
                self._raise(ValidationErrorCode.UNRESOLVED_PATH, f"{path} input reference must include a field: {reference}")
            return

        if root == "context":
            if document.context is None:
                self._raise(ValidationErrorCode.UNRESOLVED_PATH, f"{path} references $context but context block is not defined: {reference}")
            if len(parts) != 2:
                self._raise(ValidationErrorCode.UNRESOLVED_PATH, f"{path} context reference has invalid depth: {reference}")
            if parts[1] not in document.context.outputs:
                self._raise(ValidationErrorCode.UNRESOLVED_PATH, f"{path} references a non-exported context field: {reference}")
            return

        if root == "variables":
            if len(parts) != 2:
                self._raise(ValidationErrorCode.UNRESOLVED_PATH, f"{path} variables reference has invalid depth: {reference}")
            if parts[1] not in available_variables:
                self._raise(ValidationErrorCode.UNRESOLVED_PATH, f"{path} references a variable not available at this point: {reference}")
            return

        if root == "steps":
            if len(parts) != 3:
                self._raise(ValidationErrorCode.UNRESOLVED_PATH, f"{path} steps reference has invalid depth: {reference}")
            step_name = parts[1]
            field_name = parts[2]
            if step_name not in available_steps:
                self._raise(ValidationErrorCode.UNRESOLVED_PATH, f"{path} references a step not available at this point: {reference}")
            step = self._find_step(step_map, step_name)
            if not step.outputs:
                self._raise(ValidationErrorCode.MISSING_OUTPUTS, f"{path} references step outputs that are not declared: {reference}")
            if field_name not in step.outputs:
                self._raise(ValidationErrorCode.UNRESOLVED_PATH, f"{path} references a non-exported step field: {reference}")
            return

        self._raise(ValidationErrorCode.UNRESOLVED_PATH, f"{path} contains unknown scope: {reference}")

    def _validate_single_mode_message_reference(self, reference: str, step_map: dict[str, StepNode], path: str) -> None:
        parts = self._split_reference(reference)
        if len(parts) == 3 and parts[0] == "steps":
            step = self._find_step(step_map, parts[1])
            if step.result_mode == "records":
                self._raise(
                    ValidationErrorCode.INVALID_MESSAGE_TEMPLATE,
                    f"{path} cannot reference array outputs in single mode: {reference}",
                )

    def _extract_references(self, text: str) -> list[str]:
        return self.PATH_PATTERN.findall(text)

    @staticmethod
    def _split_reference(reference: str) -> list[str]:
        return reference[1:].split(".") if reference.startswith("$") else []

    def _find_step(self, step_map: dict[str, StepNode], step_name: str) -> StepNode:
        if step_name in step_map:
            return step_map[step_name]
        self._raise(ValidationErrorCode.UNRESOLVED_PATH, f"Step not found: {step_name}")
        raise AssertionError("unreachable")

    @staticmethod
    def _raise(code: ValidationErrorCode, message: str) -> NoReturn:
        raise DSLValidationError(message, code=code)
