"""步骤类型注册表。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable, Optional

from .dsl import (
    ConsumeSpec,
    FailPolicyField,
    NamedNodeField,
    NODE_TYPE_SQL,
    NODE_TYPE_VARIABLE,
    NodeType,
    SqlNodeField,
    SqlStepNode,
    StepNode,
    VariableStepNode,
)
from .exceptions import DSLParseError
from .runtime import ExecutionState, NodeExecutionResult
from .sql import DatasourceRegistry

ParseStepFn = Callable[[Any, Mapping[str, Any], str, Sequence[ConsumeSpec]], StepNode]
ValidateStructureFn = Callable[[Any, StepNode, Any, str], None]
ValidateReferenceFn = Callable[[Any, StepNode, Any, set[str], set[str], dict[str, StepNode], str], None]
CompileStepFn = Callable[[Any, StepNode], Any]
ExecuteStepFn = Callable[[Any, StepNode, Any, ExecutionState, DatasourceRegistry], NodeExecutionResult]


@dataclass(frozen=True)
class StepTypeDefinition:
    """单个步骤类型的完整能力定义。"""

    type_name: str
    parse: ParseStepFn
    validate_structure: ValidateStructureFn
    validate_references: ValidateReferenceFn
    compile: CompileStepFn
    execute: ExecuteStepFn


class StepTypeRegistry:
    """统一管理步骤类型分发。"""

    def __init__(self, definitions: Sequence[StepTypeDefinition]) -> None:
        self._definitions = {definition.type_name: definition for definition in definitions}

    def get(self, type_name: str) -> Optional[StepTypeDefinition]:
        return self._definitions.get(type_name)

    def definitions(self) -> tuple[StepTypeDefinition, ...]:
        return tuple(self._definitions.values())

    def require(self, type_name: str, path: str) -> StepTypeDefinition:
        definition = self.get(type_name)
        if definition is None:
            raise DSLParseError(f"{path} is not supported: {type_name}")
        return definition


def build_default_step_registry() -> StepTypeRegistry:
    return StepTypeRegistry(
        definitions=(
            StepTypeDefinition(
                type_name=NODE_TYPE_SQL,
                parse=_parse_sql_step,
                validate_structure=_validate_sql_step_structure,
                validate_references=_validate_sql_step_references,
                compile=_compile_sql_step,
                execute=_execute_sql_step,
            ),
            StepTypeDefinition(
                type_name=NODE_TYPE_VARIABLE,
                parse=_parse_variable_step,
                validate_structure=_validate_variable_step_structure,
                validate_references=_validate_variable_step_references,
                compile=_compile_variable_step,
                execute=_execute_variable_step,
            ),
        )
    )


def _parse_sql_step(node_parser: Any, mapping: Mapping[str, Any], node_path: str, consumes: Sequence[ConsumeSpec]) -> StepNode:
    sql_node_fields = node_parser.parse_sql_node_fields(mapping, node_path, NODE_TYPE_SQL)
    return SqlStepNode(
        name=node_parser.helpers.expect_string(
            mapping.get(NamedNodeField.NAME),
            f"{node_path}.{NamedNodeField.NAME}",
        ),
        consumes=consumes,
        on_fail=node_parser.parse_fail_policy(
            mapping.get(FailPolicyField.ON_FAIL),
            f"{node_path}.{FailPolicyField.ON_FAIL}",
            required=False,
        ),
        on_pass=node_parser.parse_pass_policy(
            mapping.get(FailPolicyField.ON_PASS),
            f"{node_path}.{FailPolicyField.ON_PASS}",
        ),
        **sql_node_fields,
    )


def _parse_variable_step(
    node_parser: Any,
    mapping: Mapping[str, Any],
    node_path: str,
    consumes: Sequence[ConsumeSpec],
) -> StepNode:
    node_type = node_parser.helpers.expect_string(
        mapping.get(SqlNodeField.TYPE),
        f"{node_path}.{SqlNodeField.TYPE}",
    )
    variable_node_fields = node_parser.parse_variable_node_fields(mapping, node_path, node_type)
    return VariableStepNode(
        name=node_parser.helpers.expect_string(
            mapping.get(NamedNodeField.NAME),
            f"{node_path}.{NamedNodeField.NAME}",
        ),
        consumes=consumes,
        on_fail=node_parser.parse_fail_policy(
            mapping.get(FailPolicyField.ON_FAIL),
            f"{node_path}.{FailPolicyField.ON_FAIL}",
            required=False,
        ),
        on_pass=node_parser.parse_pass_policy(
            mapping.get(FailPolicyField.ON_PASS),
            f"{node_path}.{FailPolicyField.ON_PASS}",
        ),
        **variable_node_fields,
    )


def _validate_sql_step_structure(validator: Any, node: StepNode, raw_step: Any, path: str) -> None:
    del raw_step
    validator.validate_sql_step_structure(node, path)


def _validate_variable_step_structure(validator: Any, node: StepNode, raw_step: Any, path: str) -> None:
    del raw_step
    validator.validate_variable_step_structure(node, path)


def _validate_sql_step_references(
    validator: Any,
    step: StepNode,
    document: Any,
    available_steps: set[str],
    available_variables: set[str],
    step_map: dict[str, StepNode],
    path_prefix: str,
) -> None:
    validator.validate_sql_step_references(
        step,
        document,
        available_steps,
        available_variables,
        step_map,
        path_prefix,
    )


def _validate_variable_step_references(
    validator: Any,
    step: StepNode,
    document: Any,
    available_steps: set[str],
    available_variables: set[str],
    step_map: dict[str, StepNode],
    path_prefix: str,
) -> None:
    validator.validate_variable_step_references(
        step,
        document,
        available_steps,
        available_variables,
        step_map,
        path_prefix,
    )


def _compile_sql_step(_compiler: Any, _step: StepNode) -> Any:
    return None


def _compile_variable_step(compiler: Any, step: StepNode) -> Any:
    return compiler.compile_variable_step(step)


def _execute_sql_step(
    pipeline: Any,
    step: StepNode,
    _compiled_step_data: Any,
    state: ExecutionState,
    datasource_registry: DatasourceRegistry,
) -> NodeExecutionResult:
    return pipeline.execute_sql_step(step, state, datasource_registry)


def _execute_variable_step(
    pipeline: Any,
    step: StepNode,
    compiled_step_data: Any,
    state: ExecutionState,
    datasource_registry: DatasourceRegistry,
) -> NodeExecutionResult:
    del datasource_registry
    return pipeline.execute_variable_step(step, compiled_step_data, state)
