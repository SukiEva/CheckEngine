"""ExecDSL 引用校验器。"""

from __future__ import annotations

import re

from check_engine.dsl.models import DslDocument, FailPolicy, PrecheckNode, StepNode, VariableDefinition
from check_engine.exceptions import DSLValidationError


class ReferenceValidator:
    """校验 DSL 中的作用域引用是否合法。"""

    PATH_PATTERN = re.compile(r"\$[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*")

    def validate(self, document: DslDocument) -> None:
        step_names = [step.name for step in document.steps]

        for variable_name, definition in document.variables.items():
            self._validate_variable_definition(variable_name, definition, document)

        for index, precheck in enumerate(document.prechecks):
            self._validate_sql_params(precheck.sql_params, document, available_steps=[])
            self._validate_fail_policy(precheck.on_fail, document, available_steps=[], path=f"prechecks[{index}].on_fail")

        for index, step in enumerate(document.steps):
            available_steps = step_names[:index]
            self._validate_sql_params(step.sql_params, document, available_steps=available_steps)
            self._validate_consumes(step, available_steps)

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
    ) -> None:
        for index, condition in enumerate(definition.when):
            for reference in self._extract_references(condition.condition):
                self._validate_reference(
                    reference,
                    document,
                    available_steps=[],
                    path=f"variables.{variable_name}.when[{index}].condition",
                )

    def _validate_sql_params(
        self,
        sql_params: dict[str, object],
        document: DslDocument,
        available_steps: list[str],
    ) -> None:
        for key, value in sql_params.items():
            if isinstance(value, str) and value.startswith("$"):
                self._validate_reference(
                    value,
                    document,
                    available_steps=available_steps,
                    path=f"sql_params.{key}",
                )

    def _validate_consumes(self, step: StepNode, available_steps: list[str]) -> None:
        for consume in step.consumes:
            if consume.from_path == "$context":
                continue

            parts = self._split_reference(consume.from_path)
            if len(parts) != 2 or parts[0] != "steps":
                raise DSLValidationError(f"consumes.from 引用非法: {consume.from_path}")
            if parts[1] not in available_steps:
                raise DSLValidationError(f"consumes.from 引用了未执行的步骤: {consume.from_path}")

    def _validate_fail_policy(
        self,
        policy: FailPolicy,
        document: DslDocument,
        available_steps: list[str],
        path: str,
    ) -> None:
        if policy.decision != "exists":
            for reference in self._extract_references(policy.decision):
                self._validate_reference(reference, document, available_steps, f"{path}.decision")

        for field_name, template in (
            ("message_cn", policy.message_cn),
            ("message_en", policy.message_en),
        ):
            for reference in self._extract_references(template):
                self._validate_reference(reference, document, available_steps, f"{path}.{field_name}")

    def _validate_reference(
        self,
        reference: str,
        document: DslDocument,
        available_steps: list[str],
        path: str,
    ) -> None:
        parts = self._split_reference(reference)
        if not parts:
            raise DSLValidationError(f"{path} 引用非法: {reference}")

        root = parts[0]
        if root == "input":
            if len(parts) < 2:
                raise DSLValidationError(f"{path} 的 input 引用必须包含字段: {reference}")
            return

        if root == "context":
            if len(parts) != 2:
                raise DSLValidationError(f"{path} 的 context 引用层级非法: {reference}")
            if parts[1] not in document.context.outputs:
                raise DSLValidationError(f"{path} 引用了未导出的 context 字段: {reference}")
            return

        if root == "variables":
            if len(parts) != 2:
                raise DSLValidationError(f"{path} 的 variables 引用层级非法: {reference}")
            if parts[1] not in document.variables:
                raise DSLValidationError(f"{path} 引用了未声明的变量: {reference}")
            return

        if root == "steps":
            if len(parts) != 3:
                raise DSLValidationError(f"{path} 的 steps 引用层级非法: {reference}")
            step_name = parts[1]
            field_name = parts[2]
            if step_name not in available_steps:
                raise DSLValidationError(f"{path} 引用了当前不可用的步骤: {reference}")
            step = self._find_step(document.steps, step_name)
            if step.outputs and field_name not in step.outputs:
                raise DSLValidationError(f"{path} 引用了未导出的步骤字段: {reference}")
            return

        raise DSLValidationError(f"{path} 存在未知作用域: {reference}")

    def _extract_references(self, text: str) -> list[str]:
        return self.PATH_PATTERN.findall(text)

    def _split_reference(self, reference: str) -> list[str]:
        return reference[1:].split(".") if reference.startswith("$") else []

    def _find_step(self, steps: list[StepNode], step_name: str) -> StepNode:
        for step in steps:
            if step.name == step_name:
                return step
        raise DSLValidationError(f"未找到步骤: {step_name}")
