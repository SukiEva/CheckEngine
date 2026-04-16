"""DSL 运行执行流水线。"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import partial
from typing import Any, Optional, TypeVar, cast

from check_engine.compiler import CompiledDsl
from check_engine.dsl import SqlNode, StepNode, VariableDefinition, VariableStepNode
from check_engine.exceptions import DSLExecutionError, log_dsl_error
from check_engine.expression import CompiledExpression, ExpressionEvaluator
from check_engine.renderer import MessageRenderer
from check_engine.runtime import ExecutionResult, ExecutionState, NodeExecutionResult
from check_engine.sql import DatasourceRegistry, SqlExecutor
from check_engine.step_registry import StepTypeRegistry

RuntimeValueT = TypeVar("RuntimeValueT")


@dataclass
class ExecutionPipeline:
    """负责运行阶段调度。"""

    expression_evaluator: ExpressionEvaluator
    sql_executor: SqlExecutor
    message_renderer: MessageRenderer
    step_registry: StepTypeRegistry
    logger: logging.Logger

    def execute(
        self,
        compiled_dsl: CompiledDsl,
        input_data: Mapping[str, Any],
        datasource_registry: DatasourceRegistry,
    ) -> ExecutionResult:
        state = ExecutionState.new(input_data=input_data)

        runtime_failure = self._run_variables(compiled_dsl, state)
        if runtime_failure is not None:
            return runtime_failure

        runtime_failure = self._run_steps(compiled_dsl, state, datasource_registry)
        if runtime_failure is not None:
            return runtime_failure

        final_failure = self._run_final_decision(compiled_dsl, state)
        if final_failure is not None:
            return final_failure

        return ExecutionResult.build_pass(state)

    def execute_sql_step(
        self,
        step: StepNode,
        state: ExecutionState,
        datasource_registry: DatasourceRegistry,
    ) -> NodeExecutionResult:
        if not isinstance(step, SqlNode):
            raise DSLExecutionError(f"Unsupported sql step node class: {type(step).__name__}")
        return self._execute_sql_node(
            phase="step",
            node=cast(SqlNode, step),
            state=state,
            datasource_registry=datasource_registry,
            node_name=step.name,
        )

    def execute_variable_step(
        self,
        step: StepNode,
        compiled_step_data: Any,
        state: ExecutionState,
    ) -> NodeExecutionResult:
        if not isinstance(step, VariableStepNode):
            raise DSLExecutionError(f"Unsupported variable step node class: {type(step).__name__}")
        compiled_conditions = cast(tuple[CompiledExpression, ...], compiled_step_data or ())
        value = self._evaluate_variable(
            VariableDefinition(when=step.when, default=step.default),
            compiled_conditions,
            state,
        )
        state.record_node_execution(
            phase="step",
            node_name=step.name,
            datasource=None,
            result_mode=None,
            row_count=None,
            executed_sql=None,
        )
        return NodeExecutionResult(
            raw_rows=(),
            exported_data=value,
            exported_fields=(),
        )

    def _run_variables(
        self,
        compiled_dsl: CompiledDsl,
        state: ExecutionState,
    ) -> Optional[ExecutionResult]:
        for variable_name, definition in compiled_dsl.document.variables.items():
            compiled_conditions = compiled_dsl.variable_conditions.get(variable_name, ())
            value, runtime_failure = self._run_runtime_action(
                state=state,
                failed_node=f"variables.{variable_name}",
                action=partial(self._evaluate_variable, definition, compiled_conditions, state),
            )
            if runtime_failure is not None:
                return runtime_failure
            state.variables_data[variable_name] = value
        return None

    def _run_steps(
        self,
        compiled_dsl: CompiledDsl,
        state: ExecutionState,
        datasource_registry: DatasourceRegistry,
    ) -> Optional[ExecutionResult]:
        for step in compiled_dsl.document.steps:
            step_definition = self.step_registry.get(step.type)
            if step_definition is None:
                raise DSLExecutionError(f"Unsupported step type: {step.type}")
            compiled_step_data = compiled_dsl.compiled_steps.get(step.name)
            result, runtime_failure = self._run_runtime_action(
                state=state,
                failed_node=step.name,
                action=partial(
                    step_definition.execute,
                    self,
                    step,
                    compiled_step_data,
                    state,
                    datasource_registry,
                ),
            )
            if runtime_failure is not None:
                return runtime_failure
            if result is None:
                raise RuntimeError("step execution result is unexpectedly None.")
            state.set_step_result(step.name, result)
            step_fail_decision = compiled_dsl.step_fail_decisions.get(step.name)
            if step_fail_decision is not None and self._evaluate_step_decision(state, step_fail_decision, step.name):
                if step.on_fail is None:
                    raise RuntimeError("step.on_fail is unexpectedly None.")
                message_cn, message_en = self.message_renderer.render(
                    step.on_fail,
                    state,
                    result.raw_rows,
                    local_data=result.exported_data,
                )
                return ExecutionResult.build_failure(
                    phase="step",
                    failed_node=step.name,
                    message_cn=message_cn,
                    message_en=message_en,
                    state=state,
                )
            step_pass_decision = compiled_dsl.step_pass_decisions.get(step.name)
            if step_pass_decision is not None and self._evaluate_step_decision(state, step_pass_decision, step.name):
                return ExecutionResult.build_pass(state)
        return None

    def _run_final_decision(self, compiled_dsl: CompiledDsl, state: ExecutionState) -> Optional[ExecutionResult]:
        should_fail, runtime_failure = self._run_runtime_action(
            state=state,
            failed_node="on_fail",
            action=lambda: self._should_fail_by_expression(compiled_dsl.on_fail_decision, state),
        )
        if runtime_failure is not None:
            return runtime_failure
        if should_fail is None:
            raise RuntimeError("final decision result is unexpectedly None.")
        if should_fail:
            rendered_message, runtime_failure = self._run_runtime_action(
                state=state,
                failed_node="on_fail",
                action=lambda: self.message_renderer.render(compiled_dsl.document.on_fail, state),
            )
            if runtime_failure is not None:
                return runtime_failure
            if rendered_message is None:
                raise RuntimeError("rendered failure message is unexpectedly None.")
            message_cn, message_en = rendered_message
            return ExecutionResult.build_failure(
                phase="final",
                failed_node="on_fail",
                message_cn=message_cn,
                message_en=message_en,
                state=state,
            )
        return None

    def _execute_sql_node(
        self,
        *,
        phase: str,
        node: SqlNode,
        state: ExecutionState,
        datasource_registry: DatasourceRegistry,
        node_name: str,
    ) -> NodeExecutionResult:
        result = self.sql_executor.execute_node(
            node,
            state=state,
            datasource_registry=datasource_registry,
            node_name=node_name,
        )
        state.record_node_execution(
            phase=phase,
            node_name=node_name,
            datasource=node.datasource,
            result_mode=node.result_mode,
            row_count=len(result.raw_rows),
            executed_sql=result.executed_sql,
        )
        return result

    def _evaluate_variable(
        self,
        definition: VariableDefinition,
        compiled_conditions: tuple[CompiledExpression, ...],
        state: ExecutionState,
    ) -> Any:
        for item, compiled_condition in zip(definition.when, compiled_conditions):
            if self._should_fail_by_expression(compiled_condition, state):
                return item.value
        return definition.default

    def _evaluate_step_decision(
        self,
        state: ExecutionState,
        compiled_expression: CompiledExpression,
        step_name: str,
    ) -> bool:
        step_data = state.step_data.get(step_name)
        return self._should_fail_by_expression(compiled_expression, state, local_data=step_data)

    def _should_fail_by_expression(
        self,
        expression: CompiledExpression,
        state: ExecutionState,
        local_data: Optional[Any] = None,
    ) -> bool:
        return bool(self.expression_evaluator.evaluate_compiled(expression, state, local_data=local_data))

    def _run_runtime_action(
        self,
        *,
        state: ExecutionState,
        failed_node: str,
        action: Callable[[], RuntimeValueT],
    ) -> tuple[Optional[RuntimeValueT], Optional[ExecutionResult]]:
        try:
            return action(), None
        except DSLExecutionError as exc:
            log_dsl_error(self.logger, f"runtime:{failed_node}", exc)
            return None, ExecutionResult.build_runtime_failure(exc, state, failed_node=failed_node)
