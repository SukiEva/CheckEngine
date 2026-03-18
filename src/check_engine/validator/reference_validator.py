"""ExecDSL 引用校验器。"""

from __future__ import annotations

import re
from typing import NoReturn

from ..dsl.models import DslDocument, FailPolicy, PrecheckNode, StepNode, VariableDefinition
from ..exceptions import DSLValidationError, ValidationErrorCode


class ReferenceValidator:
    """校验 DSL 中的作用域引用是否合法。"""

    PATH_PATTERN = re.compile(r"\$[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*")

    def validate(self, document: DslDocument) -> None:
        step_names = [step.name for step in document.steps]
        available_variables: list[str] = []

        for variable_name, definition in document.variables.items():
            self._validate_variable_definition(variable_name, definition, document, available_variables)
            available_variables.append(variable_name)

        for index, precheck in enumerate(document.prechecks):
            self._validate_sql_params(precheck.sql_params, document, available_steps=[], available_variables=available_variables)
            self._validate_fail_policy(precheck.on_fail, document, available_steps=[], path=f"prechecks[{index}].on_fail")

        for index, step in enumerate(document.steps):
            available_steps = step_names[:index]
            self._validate_sql_params(
                step.sql_params,
                document,
                available_steps=available_steps,
                available_variables=available_variables,
            )
            self._validate_consumes(step, available_steps, document)

        self._validate_fail_policy(
            document.on_fail,
            document,
            available_steps=step_names,
            path="on_fail",
        )

    def _validate_variable_definition(
        self,
        variable_name: str,
        definition: VariableDefinition,
        document: DslDocument,
        available_variables: list[str],
    ) -> None:
        for index, condition in enumerate(definition.when):
            for reference in self._extract_references(condition.condition):
                self._validate_reference(
                    reference,
                    document,
                    available_steps=[],
                    available_variables=available_variables,
                    path=f"variables.{variable_name}.when[{index}].condition",
                )

    def _validate_sql_params(
        self,
        sql_params: dict[str, object],
        document: DslDocument,
        available_steps: list[str],
        available_variables: list[str],
    ) -> None:
        for key, value in sql_params.items():
            if isinstance(value, str) and value.startswith("$"):
                self._validate_reference(
                    value,
                    document,
                    available_steps=available_steps,
                    available_variables=available_variables,
                    path=f"sql_params.{key}",
                )

    def _validate_consumes(self, step: StepNode, available_steps: list[str], document: DslDocument) -> None:
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
            source_step = self._find_step(document.steps, parts[1])
            if not source_step.outputs:
                self._raise(
                    ValidationErrorCode.MISSING_OUTPUTS,
                    f"consumes.from references step outputs that are not declared: {consume.from_path}",
                )

    def _validate_fail_policy(
        self,
        policy: FailPolicy,
        document: DslDocument,
        available_steps: list[str],
        path: str,
    ) -> None:
        if policy.decision != "exists":
            for reference in self._extract_references(policy.decision):
                self._validate_reference(
                    reference,
                    document,
                    available_steps,
                    available_variables=list(document.variables.keys()),
                    path=f"{path}.decision",
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
                    available_variables=list(document.variables.keys()),
                    path=f"{path}.{field_name}",
                )
                if path == "on_fail":
                    self._validate_single_mode_message_reference(reference, document, f"{path}.{field_name}")

    def _validate_reference(
        self,
        reference: str,
        document: DslDocument,
        available_steps: list[str],
        available_variables: list[str],
        path: str,
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
            step = self._find_step(document.steps, step_name)
            if not step.outputs:
                self._raise(ValidationErrorCode.MISSING_OUTPUTS, f"{path} references step outputs that are not declared: {reference}")
            if field_name not in step.outputs:
                self._raise(ValidationErrorCode.UNRESOLVED_PATH, f"{path} references a non-exported step field: {reference}")
            return

        self._raise(ValidationErrorCode.UNRESOLVED_PATH, f"{path} contains unknown scope: {reference}")

    def _validate_single_mode_message_reference(self, reference: str, document: DslDocument, path: str) -> None:
        parts = self._split_reference(reference)
        if len(parts) == 3 and parts[0] == "steps":
            step = self._find_step(document.steps, parts[1])
            if step.result_mode == "records":
                self._raise(
                    ValidationErrorCode.INVALID_MESSAGE_TEMPLATE,
                    f"{path} cannot reference array outputs in single mode: {reference}",
                )

    def _extract_references(self, text: str) -> list[str]:
        return self.PATH_PATTERN.findall(text)

    def _split_reference(self, reference: str) -> list[str]:
        return reference[1:].split(".") if reference.startswith("$") else []

    def _find_step(self, steps: list[StepNode], step_name: str) -> StepNode:
        for step in steps:
            if step.name == step_name:
                return step
        self._raise(ValidationErrorCode.UNRESOLVED_PATH, f"Step not found: {step_name}")
        raise AssertionError("unreachable")

    def _raise(self, code: ValidationErrorCode, message: str) -> NoReturn:
        raise DSLValidationError(message, code=code)
