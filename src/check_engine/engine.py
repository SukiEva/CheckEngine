"""ExecDSL 主执行引擎。"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from functools import partial
from typing import Any, Optional, TypeVar

from .compiler import CompiledDsl, CompileCacheLike, DslCompiler, HashedLruCompileCache, NoopCompileCache
from .dsl import DslDocument, EXISTS_DECISION, PrecheckNode, SqlNode, VariableDefinition
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

        self.parser: JsonDslParser = JsonDslParser()
        self.validator: DslValidator = DslValidator()
        self.logger = logger or logging.getLogger(__name__)
        self.expression_evaluator: ExpressionEvaluator = ExpressionEvaluator()
        self.compiler: DslCompiler = DslCompiler(
            expression_evaluator=self.expression_evaluator,
            logger=self.logger,
        )
        self.sql_executor: SqlExecutor = SqlExecutor()
        self.message_renderer: MessageRenderer = MessageRenderer()
        self.compile_cache_size = compile_cache_size
        self._compile_cache_backend: CompileCacheLike[CompiledDsl] = (
            HashedLruCompileCache(compile_cache_size) if compile_cache_size > 0 else NoopCompileCache()
        )

    def validate(self, dsl_text: str) -> None:
        """显式校验 DSL（适合保存/更新规则时调用）。"""
        if not isinstance(dsl_text, str):
            raise TypeError("dsl_text must be a string.")
        document = self.parser.parse(dsl_text)
        self.validator.validate(document)

    def execute(
        self,
        dsl_text: str,
        input_data: Mapping[str, Any],
        datasource_registry: DatasourceRegistry,
    ) -> ExecutionResult:
        if not isinstance(dsl_text, str):
            raise TypeError("dsl_text must be a string.")
        compiled_dsl = self._compile(dsl_text)
        return self._execute_compiled(compiled_dsl, input_data, datasource_registry)

    def _compile(self, dsl_text: str) -> CompiledDsl:
        """编译 DSL 并返回可由缓存共享复用的只读结果。"""
        cached = self._compile_cache_backend.get(dsl_text)
        if cached is not None:
            return cached

        document = self.parser.parse(dsl_text)
        compiled = self.compiler.compile(document=document)
        self._compile_cache_backend.put(dsl_text, compiled)
        return compiled

    def _execute_compiled(
        self,
        compiled_dsl: CompiledDsl,
        input_data: Mapping[str, Any],
        datasource_registry: DatasourceRegistry,
    ) -> ExecutionResult:
        document = compiled_dsl.document
        state = ExecutionState.new(input_data=input_data)

        runtime_failure = self._run_context(document, state, datasource_registry)
        if runtime_failure is not None:
            return runtime_failure

        runtime_failure = self._run_variables(document, compiled_dsl, state)
        if runtime_failure is not None:
            return runtime_failure

        precheck_failure = self._run_prechecks(document, compiled_dsl, state, datasource_registry)
        if precheck_failure is not None:
            return precheck_failure

        runtime_failure = self._run_steps(document, state, datasource_registry)
        if runtime_failure is not None:
            return runtime_failure

        final_failure = self._run_final_decision(compiled_dsl, state)
        if final_failure is not None:
            return final_failure

        return ExecutionResult.build_pass(state)

    def _run_context(
        self,
        document: DslDocument,
        state: ExecutionState,
        datasource_registry: DatasourceRegistry,
    ) -> Optional[ExecutionResult]:
        if document.context is None:
            return None
        context_node = document.context
        result, runtime_failure = self._run_runtime_action(
            state=state,
            failed_node="context",
            action=lambda: self._execute_sql_node(
                phase="context",
                node=context_node,
                state=state,
                datasource_registry=datasource_registry,
                node_name="context",
            ),
        )
        if runtime_failure is not None:
            return runtime_failure
        if result is None:
            raise RuntimeError("context execution result is unexpectedly None.")
        state.set_context_result(result)
        return None

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

    def _run_prechecks(
        self,
        document: DslDocument,
        compiled_dsl: CompiledDsl,
        state: ExecutionState,
        datasource_registry: DatasourceRegistry,
    ) -> Optional[ExecutionResult]:
        for precheck in document.prechecks:
            result, runtime_failure = self._run_runtime_action(
                state=state,
                failed_node=precheck.name,
                action=partial(
                    self._execute_sql_node,
                    phase="precheck",
                    node=precheck,
                    state=state,
                    datasource_registry=datasource_registry,
                    node_name=precheck.name,
                ),
            )
            if runtime_failure is not None:
                return runtime_failure
            if result is None:
                raise RuntimeError("precheck execution result is unexpectedly None.")
            if self._should_fail_precheck(
                precheck,
                result.raw_rows,
                state,
                compiled_dsl.precheck_decisions.get(precheck.name),
            ):
                if precheck.on_fail is None:
                    raise RuntimeError("precheck.on_fail is unexpectedly None.")
                message_cn, message_en = self.message_renderer.render(precheck.on_fail, state, result.raw_rows)
                return ExecutionResult.build_failure(
                    phase="precheck",
                    failed_node=precheck.name,
                    message_cn=message_cn,
                    message_en=message_en,
                    state=state,
                )
        return None

    def _run_steps(
        self,
        document: DslDocument,
        state: ExecutionState,
        datasource_registry: DatasourceRegistry,
    ) -> Optional[ExecutionResult]:
        for step in document.steps:
            result, runtime_failure = self._run_runtime_action(
                state=state,
                failed_node=step.name,
                action=partial(
                    self._execute_sql_node,
                    phase="step",
                    node=step,
                    state=state,
                    datasource_registry=datasource_registry,
                    node_name=step.name,
                ),
            )
            if runtime_failure is not None:
                return runtime_failure
            if result is None:
                raise RuntimeError("step execution result is unexpectedly None.")
            state.set_step_result(step.name, result)
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

    def _should_fail_precheck(
        self,
        precheck: PrecheckNode,
        rows: Sequence[Mapping[str, Any]],
        state: ExecutionState,
        compiled_expression: Optional[CompiledExpression],
    ) -> bool:
        if precheck.on_fail is None:
            raise ValueError("precheck.on_fail must not be None.")
        if precheck.on_fail.decision == EXISTS_DECISION:
            return len(rows) > 0
        if compiled_expression is None:
            raise ValueError("compiled_expression must not be None when precheck decision is not bare exists.")
        return self._should_fail_by_expression(compiled_expression, state)

    def _should_fail_by_expression(self, expression: CompiledExpression, state: ExecutionState) -> bool:
        return bool(self.expression_evaluator.evaluate_compiled(expression, state))

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
            self.logger.exception("Runtime execution failed at node %s", failed_node)
            return None, ExecutionResult.build_runtime_failure(exc, state, failed_node=failed_node)
