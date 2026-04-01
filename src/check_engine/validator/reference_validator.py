"""ExecDSL 引用校验器。"""

from __future__ import annotations

from collections.abc import Mapping
from typing import NoReturn, Optional

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
from ..exceptions import DSLExecutionError, DSLValidationError
from ..reference_parser import ReferenceParser, ReferenceSpec
from ..renderer.template_parser import TemplateParser
from ..step_registry import StepTypeRegistry, build_default_step_registry


class ReferenceValidator:
    """校验 DSL 中的作用域引用是否合法。"""

    def __init__(
        self,
        reference_parser: Optional[ReferenceParser] = None,
        step_registry: Optional[StepTypeRegistry] = None,
    ) -> None:
        self.reference_parser = reference_parser or ReferenceParser()
        self.template_parser = TemplateParser(self.reference_parser)
        self.step_registry = step_registry or build_default_step_registry()

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
            step_definition = self.step_registry.get(step.type)
            if step_definition is not None:
                step_definition.validate_references(
                    self,
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
            for reference in self.reference_parser.extract_references(condition.condition):
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
                reference_spec = self._parse_reference_for_validation(value, f"{path_prefix}.{key}")
                self._validate_reference(
                    reference_spec,
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
            reference_spec = self._parse_reference_for_validation(
                consume.from_path,
                "consumes.from",
            )
            if reference_spec.scope != "steps" or len(reference_spec.path) != 1:
                self._raise(f"Invalid consumes.from reference: {consume.from_path}")
            step_name = reference_spec.path[0]
            if step_name not in available_steps:
                self._raise(f"consumes.from references a step that has not executed yet: {consume.from_path}")
            source_step = self._find_step(step_map, step_name)
            if not isinstance(source_step, SqlStepNode) or not source_step.outputs:
                self._raise(f"consumes.from references step outputs that are not declared: {consume.from_path}")

    def validate_sql_step_references(
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

    def validate_variable_step_references(
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
        exists_argument_references = {
            reference.explicit
            for reference in self.reference_parser.extract_exists_argument_references(policy.decision)
        }
        for reference in self.reference_parser.extract_references(policy.decision):
            self._validate_reference(
                reference,
                document,
                available_steps,
                available_variables=available_variables,
                path=f"{path}.decision",
                step_map=step_map,
                local_outputs=local_outputs,
                allow_sql_step_root=reference.explicit in exists_argument_references,
            )

        for field_name, template in (("message_cn", policy.message_cn), ("message_en", policy.message_en)):
            for reference in self._extract_template_references(template):
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
        exists_argument_references = {
            reference.explicit
            for reference in self.reference_parser.extract_exists_argument_references(policy.decision)
        }
        for reference in self.reference_parser.extract_references(policy.decision):
            self._validate_reference(
                reference,
                document,
                available_steps=available_steps,
                available_variables=available_variables,
                path=f"{path}.decision",
                step_map=step_map,
                local_outputs=local_outputs,
                allow_sql_step_root=reference.explicit in exists_argument_references,
            )

    def _validate_reference(
        self,
        reference_spec: ReferenceSpec,
        document: DslDocument,
        available_steps: set[str],
        available_variables: set[str],
        path: str,
        step_map: dict[str, StepNode],
        local_outputs: Optional[set[str]],
        allow_sql_step_root: bool = False,
    ) -> None:
        del document
        if reference_spec.is_local:
            self._validate_local_reference(
                reference_spec,
                path,
                local_outputs,
                allow_local_root=allow_sql_step_root,
            )
            return

        root = reference_spec.scope
        if root == "input":
            if not reference_spec.path:
                self._raise(f"{path} input reference must include a field: {reference_spec.explicit}")
            return

        if root == "variables":
            if len(reference_spec.path) != 1:
                self._raise(f"{path} variables reference has invalid depth: {reference_spec.explicit}")
            if reference_spec.path[0] not in available_variables:
                self._raise(f"{path} references a variable not available at this point: {reference_spec.explicit}")
            return

        if root == "steps":
            step_path = reference_spec.path
            if len(step_path) not in (1, 2):
                self._raise(f"{path} steps reference has invalid depth: {reference_spec.explicit}")
            step_name = step_path[0]
            if step_name not in available_steps:
                self._raise(f"{path} references a step not available at this point: {reference_spec.explicit}")
            step = self._find_step(step_map, step_name)
            if len(step_path) == 1:
                if step.type == NODE_TYPE_VARIABLE:
                    return
                if allow_sql_step_root and step.type == NODE_TYPE_SQL:
                    if not isinstance(step, SqlStepNode):
                        self._raise(f"{path} references an unsupported step type: {reference_spec.explicit}")
                    if not step.outputs:
                        self._raise(f"{path} references step outputs that are not declared: {reference_spec.explicit}")
                    return
                if step.type != NODE_TYPE_VARIABLE:
                    self._raise(f"{path} sql step reference must include exported field: {reference_spec.explicit}")
            if len(step_path) != 2:
                self._raise(f"{path} steps reference has invalid depth: {reference_spec.explicit}")
            if step.type == NODE_TYPE_VARIABLE:
                self._raise(f"{path} variable step reference only supports $steps.<name>: {reference_spec.explicit}")
            if not isinstance(step, SqlStepNode):
                self._raise(f"{path} references an unsupported step type: {reference_spec.explicit}")
            field_name = step_path[1]
            if not step.outputs:
                self._raise(f"{path} references step outputs that are not declared: {reference_spec.explicit}")
            if field_name not in step.outputs:
                self._raise(f"{path} references a non-exported step field: {reference_spec.explicit}")
            return

        self._raise(f"{path} contains unknown scope: {reference_spec.explicit}")

    def _validate_local_reference(
        self,
        reference_spec: ReferenceSpec,
        path: str,
        local_outputs: Optional[set[str]],
        allow_local_root: bool = False,
    ) -> None:
        if local_outputs is None:
            self._raise(f"{path} cannot use local scope reference: {reference_spec.explicit}")
        if not reference_spec.path:
            if allow_local_root:
                return
            self._raise(f"{path} local reference must include a field: {reference_spec.explicit}")
        field_name = reference_spec.path[0]
        if field_name not in local_outputs:
            self._raise(f"{path} references a non-exported local field: {reference_spec.explicit}")

    def _validate_single_mode_message_reference(
        self,
        reference_spec: ReferenceSpec,
        step_map: dict[str, StepNode],
        path: str,
    ) -> None:
        if reference_spec.scope == "steps" and len(reference_spec.path) == 2:
            step = self._find_step(step_map, reference_spec.path[0])
            if isinstance(step, SqlStepNode) and step.result_mode == RESULT_MODE_RECORDS:
                self._raise(f"{path} cannot reference array outputs in single mode: {reference_spec.explicit}")

    def _extract_template_references(self, template: str) -> tuple[ReferenceSpec, ...]:
        references: list[ReferenceSpec] = []
        try:
            for template_token in self.template_parser.extract_tokens(template):
                if template_token.reference is not None:
                    references.append(template_token.reference)
        except DSLExecutionError as exc:
            self._raise(str(exc))
        return tuple(references)

    def _parse_reference_for_validation(self, reference: str, path: str) -> ReferenceSpec:
        try:
            return self.reference_parser.parse(reference)
        except DSLExecutionError as exc:
            self._raise(f"{path} contains invalid reference: {reference}")
            raise AssertionError("unreachable") from exc

    def _find_step(self, step_map: dict[str, StepNode], step_name: str) -> StepNode:
        if step_name in step_map:
            return step_map[step_name]
        self._raise(f"Step not found: {step_name}")
        raise AssertionError("unreachable")

    @staticmethod
    def _raise(message: str) -> NoReturn:
        raise DSLValidationError(message)
