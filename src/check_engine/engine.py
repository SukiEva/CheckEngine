"""ExecDSL 主执行引擎。"""

from __future__ import annotations

import logging
import traceback
from collections.abc import Callable, Mapping
from functools import partial
from typing import Any, Optional, TypeVar, cast

from .compiler import CompiledDsl, CompileCacheLike, DslCompiler, HashedLruCompileCache, NoopCompileCache
from .dsl import DslDocument, NODE_TYPE_SQL, NODE_TYPE_VARIABLE, SqlNode, StepNode, VariableDefinition, VariableStepNode
from .expression import CompiledExpression, ExpressionEvaluator
from .exceptions import DSLExecutionError
from .parser import JsonDslParser
from .renderer import MessageRenderer
from .runtime import ExecutionResult, ExecutionState, NodeExecutionResult
from .sql import DatasourceRegistry, SqlExecutor
from .validator import DslValidator

RuntimeValueT = TypeVar("RuntimeValueT")


class DslEngine:
    """统一入口：编译、校验并执行 DSL。"""

    def __init__(
        self,
        compile_cache_size: int = 128,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        if compile_cache_size < 0:
            raise ValueError("compile_cache_size must be greater than or equal to 0.")

        self.logger = logger or logging.getLogger(__name__)
        self.parser: JsonDslParser = JsonDslParser()
        self.validator: DslValidator = DslValidator()
        self.expression_evaluator: ExpressionEvaluator = ExpressionEvaluator()
        self.compiler: DslCompiler = DslCompiler(
            expression_evaluator=self.expression_evaluator,
        )
        self.sql_executor: SqlExecutor = SqlExecutor()
        self.message_renderer: MessageRenderer = MessageRenderer()
        self._step_executors: dict[str, Callable[[StepNode, CompiledDsl, ExecutionState, DatasourceRegistry], NodeExecutionResult]] = {
            NODE_TYPE_SQL: self._execute_sql_step,
            NODE_TYPE_VARIABLE: self._execute_variable_step,
        }
        self.compile_cache_size = compile_cache_size
        self._compile_cache_backend: CompileCacheLike[CompiledDsl] = (
            HashedLruCompileCache(compile_cache_size) if compile_cache_size > 0 else NoopCompileCache()
        )

    def validate(self, dsl_text: str) -> None:
        """显式校验 DSL（适合保存/更新规则时调用）。"""
        if not isinstance(dsl_text, str):
            raise TypeError("dsl_text must be a string.")
        try:
            document = self.parser.parse(dsl_text)
            self.validator.validate(document)
        except Exception as exc:  # noqa: BLE001
            handled_error = self._ensure_dsl_error(exc)
            self._log_dsl_error("validate", handled_error)
            raise handled_error from exc

    def execute(
        self,
        dsl_text: str,
        input_data: Mapping[str, Any],
        datasource_registry: DatasourceRegistry,
    ) -> ExecutionResult:
        if not isinstance(dsl_text, str):
            raise TypeError("dsl_text must be a string.")
        try:
            compiled_dsl = self._compile(dsl_text)
        except Exception as exc:  # noqa: BLE001
            handled_error = self._ensure_dsl_error(exc)
            self._log_dsl_error("compile", handled_error)
            raise handled_error from exc
        document = compiled_dsl.document
        state = ExecutionState.new(input_data=input_data)

        runtime_failure = self._run_variables(document, compiled_dsl, state)
        if runtime_failure is not None:
            return runtime_failure

        runtime_failure = self._run_steps(document, compiled_dsl, state, datasource_registry)
        if runtime_failure is not None:
            return runtime_failure

        final_failure = self._run_final_decision(compiled_dsl, state)
        if final_failure is not None:
            return final_failure

        return ExecutionResult.build_pass(state)

    def _compile(self, dsl_text: str) -> CompiledDsl:
        """编译 DSL 并返回可由缓存共享复用的只读结果。"""
        cached = self._compile_cache_backend.get(dsl_text)
        if cached is not None:
            return cached

        document = self.parser.parse(dsl_text)
        compiled = self.compiler.compile(document=document)
        self._compile_cache_backend.put(dsl_text, compiled)
        return compiled

    def _run_variables(
        self,
        document: DslDocument,
        compiled_dsl: CompiledDsl,
        state: ExecutionState,
    ) -> Optional[ExecutionResult]:
        for variable_name, definition in document.variables.items():
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
        document: DslDocument,
        compiled_dsl: CompiledDsl,
        state: ExecutionState,
        datasource_registry: DatasourceRegistry,
    ) -> Optional[ExecutionResult]:
        for step in document.steps:
            result, runtime_failure = self._run_runtime_action(
                state=state,
                failed_node=step.name,
                action=partial(
                    self._execute_step,
                    step=step,
                    compiled_dsl=compiled_dsl,
                    state=state,
                    datasource_registry=datasource_registry,
                ),
            )
            if runtime_failure is not None:
                return runtime_failure
            if result is None:
                raise RuntimeError("step execution result is unexpectedly None.")
            state.set_step_result(step.name, result)
            step_fail_decision = compiled_dsl.step_fail_decisions.get(step.name)
            if step_fail_decision is not None and self._should_fail_step(state, step_fail_decision, step.name):
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
            if step_pass_decision is not None and self._should_pass_step(state, step_pass_decision, step.name):
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

    def _execute_variable_step(
        self,
        node: StepNode,
        compiled_dsl: CompiledDsl,
        state: ExecutionState,
        _datasource_registry: DatasourceRegistry,
    ) -> NodeExecutionResult:
        if not isinstance(node, VariableStepNode):
            raise DSLExecutionError(f"Unsupported variable step node class: {type(node).__name__}")
        compiled_conditions = compiled_dsl.step_variable_conditions.get(node.name, ())
        value = self._evaluate_variable(
            VariableDefinition(when=node.when, default=node.default),
            compiled_conditions,
            state,
        )
        state.record_node_execution(
            phase="step",
            node_name=node.name,
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

    def _execute_sql_step(
        self,
        step: StepNode,
        _compiled_dsl: CompiledDsl,
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

    def _execute_step(
        self,
        *,
        step: StepNode,
        compiled_dsl: CompiledDsl,
        state: ExecutionState,
        datasource_registry: DatasourceRegistry,
    ) -> NodeExecutionResult:
        step_executor = self._step_executors.get(step.type)
        if step_executor is None:
            raise DSLExecutionError(f"Unsupported step type: {step.type}")
        return step_executor(step, compiled_dsl, state, datasource_registry)

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

    def _should_fail_step(
        self,
        state: ExecutionState,
        compiled_expression: CompiledExpression,
        step_name: str,
    ) -> bool:
        step_data = state.step_data.get(step_name)
        return self._should_fail_by_expression(compiled_expression, state, local_data=step_data)

    def _should_pass_step(
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
            self._log_dsl_error(f"runtime:{failed_node}", exc)
            return None, ExecutionResult.build_runtime_failure(exc, state, failed_node=failed_node)

    @staticmethod
    def _ensure_dsl_error(exc: Exception) -> Exception:
        from .exceptions import DSLParseError, DSLValidationError

        if isinstance(exc, (DSLParseError, DSLValidationError, DSLExecutionError)):
            return exc
        return DSLExecutionError("Unexpected runtime error in DSL engine.", original_exception=exc)

    def _log_dsl_error(self, phase: str, exc: Exception) -> None:
        error_traceback = getattr(exc, "original_traceback", None)
        if not isinstance(error_traceback, str) or not error_traceback:
            error_traceback = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        self.logger.error("DslEngine %s failed: %s\n%s", phase, exc, error_traceback)
