"""ExecDSL 主执行引擎。"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Callable, Optional

from .dsl.models import DslDocument, PrecheckNode, VariableDefinition
from .expression import CompiledExpression, ExpressionEvaluator
from .exceptions import DSLExecutionError
from .parser import JsonDslParser
from .renderer import MessageRenderer
from .result import ResultBuilder
from .runtime.state import ExecutionResult, ExecutionState
from .sql import SqlExecutor
from .validator import DslValidator


@dataclass(frozen=True)
class CompiledDsl:
    """已完成解析、校验与表达式预编译的 DSL。"""

    document: DslDocument
    variable_conditions: dict[str, tuple[CompiledExpression, ...]]
    precheck_decisions: dict[str, Optional[CompiledExpression]]
    on_fail_decision: CompiledExpression

    def clone(self) -> "CompiledDsl":
        return CompiledDsl(
            document=deepcopy(self.document),
            variable_conditions=dict(self.variable_conditions),
            precheck_decisions=dict(self.precheck_decisions),
            on_fail_decision=self.on_fail_decision,
        )


class DslEngine:
    """统一入口：编译、校验并执行 DSL。"""

    def __init__(
        self,
        parser: Optional[JsonDslParser] = None,
        validator: Optional[DslValidator] = None,
        expression_evaluator: Optional[ExpressionEvaluator] = None,
        sql_executor: Optional[SqlExecutor] = None,
        message_renderer: Optional[MessageRenderer] = None,
        result_builder: Optional[ResultBuilder] = None,
        compile_cache_size: int = 128,
    ) -> None:
        if compile_cache_size < 0:
            raise ValueError("compile_cache_size must be greater than or equal to 0.")

        self.parser = parser or JsonDslParser()
        self.validator = validator or DslValidator()
        self.expression_evaluator = expression_evaluator or ExpressionEvaluator()
        self.sql_executor = sql_executor or SqlExecutor()
        self.message_renderer = message_renderer or MessageRenderer()
        self.result_builder = result_builder or ResultBuilder()
        self.compile_cache_size = compile_cache_size
        self._cache_compiled_results = compile_cache_size > 0
        self._compile_callable = self._build_compile_callable(compile_cache_size)

    def compile(self, dsl_text: str) -> CompiledDsl:
        if not isinstance(dsl_text, str):
            raise TypeError("dsl_text must be a string.")
        compiled_dsl = self._compile_callable(dsl_text)
        if not self._cache_compiled_results:
            return compiled_dsl
        return compiled_dsl.clone()

    def clear_compile_cache(self) -> None:
        cache_clear = getattr(self._compile_callable, "cache_clear", None)
        if cache_clear is not None:
            cache_clear()

    def compile_cache_info(self) -> Optional[Any]:
        cache_info = getattr(self._compile_callable, "cache_info", None)
        if cache_info is None:
            return None
        return cache_info()

    def execute(
        self,
        dsl_text: str,
        input_data: dict[str, Any],
        datasource_registry: Any,
    ) -> ExecutionResult:
        return self.execute_compiled(self.compile(dsl_text), input_data, datasource_registry)

    def execute_document(
        self,
        document: DslDocument,
        input_data: dict[str, Any],
        datasource_registry: Any,
    ) -> ExecutionResult:
        return self.execute_compiled(self._compile_document(document), input_data, datasource_registry)

    def execute_compiled(
        self,
        compiled_dsl: CompiledDsl,
        input_data: dict[str, Any],
        datasource_registry: Any,
    ) -> ExecutionResult:
        document = compiled_dsl.document
        state = ExecutionState.new(input_data=input_data)
        if document.context is not None:
            try:
                context_result = self.sql_executor.execute_node(
                    document.context,
                    state=state,
                    datasource_registry=datasource_registry,
                    node_name="context",
                )
                state.set_context_result(context_result)
            except DSLExecutionError as exc:
                return self.result_builder.build_runtime_failure(exc, state, failed_node="context")

        for variable_name, definition in document.variables.items():
            try:
                state.variables_data[variable_name] = self._evaluate_variable(
                    definition,
                    compiled_dsl.variable_conditions.get(variable_name, ()),
                    state,
                )
            except DSLExecutionError as exc:
                return self.result_builder.build_runtime_failure(exc, state, failed_node=f"variables.{variable_name}")

        for precheck in document.prechecks:
            try:
                result = self.sql_executor.execute_node(
                    precheck,
                    state=state,
                    datasource_registry=datasource_registry,
                    node_name=precheck.name,
                )
                if self._should_fail_precheck(
                    precheck,
                    result.raw_rows,
                    state,
                    compiled_dsl.precheck_decisions.get(precheck.name),
                ):
                    message_cn, message_en = self.message_renderer.render(precheck.on_fail, state, result.raw_rows)
                    return self.result_builder.build_failure(
                        phase="precheck",
                        failed_node=precheck.name,
                        message_cn=message_cn,
                        message_en=message_en,
                        state=state,
                    )
            except DSLExecutionError as exc:
                return self.result_builder.build_runtime_failure(exc, state, failed_node=precheck.name)

        for step in document.steps:
            try:
                result = self.sql_executor.execute_node(
                    step,
                    state=state,
                    datasource_registry=datasource_registry,
                    node_name=step.name,
                )
                state.set_step_result(step.name, result)
            except DSLExecutionError as exc:
                return self.result_builder.build_runtime_failure(exc, state, failed_node=step.name)

        try:
            if self._should_fail_by_expression(compiled_dsl.on_fail_decision, state):
                message_cn, message_en = self.message_renderer.render(document.on_fail, state)
                return self.result_builder.build_failure(
                    phase="final",
                    failed_node="on_fail",
                    message_cn=message_cn,
                    message_en=message_en,
                    state=state,
                )
        except DSLExecutionError as exc:
            return self.result_builder.build_runtime_failure(exc, state, failed_node="on_fail")

        return self.result_builder.build_pass(state)

    def _build_compile_callable(self, compile_cache_size: int) -> Callable[[str], CompiledDsl]:
        if compile_cache_size == 0:
            return self._compile_uncached
        return lru_cache(maxsize=compile_cache_size)(self._compile_uncached)

    def _compile_uncached(self, dsl_text: str) -> CompiledDsl:
        document = self.parser.parse(dsl_text)
        return self._compile_document(document)

    def _compile_document(self, document: DslDocument) -> CompiledDsl:
        self.validator.validate(document)
        return CompiledDsl(
            document=deepcopy(document),
            variable_conditions={
                variable_name: tuple(self.expression_evaluator.compile(item.condition) for item in definition.when)
                for variable_name, definition in document.variables.items()
            },
            precheck_decisions={
                precheck.name: (None if precheck.on_fail.decision == "exists" else self.expression_evaluator.compile(precheck.on_fail.decision))
                for precheck in document.prechecks
            },
            on_fail_decision=self.expression_evaluator.compile(document.on_fail.decision),
        )

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
        rows: list[dict[str, Any]],
        state: ExecutionState,
        compiled_expression: Optional[CompiledExpression],
    ) -> bool:
        if precheck.on_fail.decision == "exists":
            return len(rows) > 0
        if compiled_expression is None:
            raise ValueError("compiled_expression must not be None when precheck decision is not bare exists.")
        return self._should_fail_by_expression(compiled_expression, state)

    def _should_fail_by_expression(self, expression: CompiledExpression, state: ExecutionState) -> bool:
        return bool(self.expression_evaluator.evaluate_compiled(expression, state))
