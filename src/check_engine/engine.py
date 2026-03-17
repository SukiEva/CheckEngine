"""ExecDSL 主执行引擎。"""

from __future__ import annotations

from typing import Any, Optional

from .dsl.models import DslDocument, FailPolicy, VariableDefinition
from .expression import ExpressionEvaluator
from .parser import JsonDslParser
from .renderer import MessageRenderer
from .result import ResultBuilder
from .runtime.state import ExecutionResult, ExecutionState
from .sql import SqlExecutor
from .validator import DslValidator


class DslEngine:
    """统一入口：解析、校验并执行 DSL。"""

    def __init__(
        self,
        parser: Optional[JsonDslParser] = None,
        validator: Optional[DslValidator] = None,
        expression_evaluator: Optional[ExpressionEvaluator] = None,
        sql_executor: Optional[SqlExecutor] = None,
        message_renderer: Optional[MessageRenderer] = None,
        result_builder: Optional[ResultBuilder] = None,
    ) -> None:
        self.parser = parser or JsonDslParser()
        self.validator = validator or DslValidator()
        self.expression_evaluator = expression_evaluator or ExpressionEvaluator()
        self.sql_executor = sql_executor or SqlExecutor()
        self.message_renderer = message_renderer or MessageRenderer()
        self.result_builder = result_builder or ResultBuilder()

    def execute(
        self,
        dsl_text: str,
        input_data: dict[str, Any],
        datasource_registry: Any,
    ) -> ExecutionResult:
        document = self.parser.parse(dsl_text)
        self.validator.validate(document)
        return self.execute_document(document, input_data, datasource_registry)

    def execute_document(
        self,
        document: DslDocument,
        input_data: dict[str, Any],
        datasource_registry: Any,
    ) -> ExecutionResult:
        state = ExecutionState.new(input_data=input_data)

        if document.context is not None:
            context_result, context_trace = self.sql_executor.execute_node(
                document.context,
                phase="context",
                state=state,
                datasource_registry=datasource_registry,
                node_name="context",
            )
            state.add_trace(context_trace)
            state.set_context_result(context_result)

        for variable_name, definition in document.variables.items():
            state.variables_data[variable_name] = self._evaluate_variable(definition, state)

        for precheck in document.prechecks:
            result, trace = self.sql_executor.execute_node(
                precheck,
                phase="precheck",
                state=state,
                datasource_registry=datasource_registry,
                node_name=precheck.name,
            )
            state.add_trace(trace)
            if self._should_fail_precheck(precheck.on_fail, result.raw_rows, state):
                message_cn, message_en = self.message_renderer.render(precheck.on_fail, state, result.raw_rows)
                return self.result_builder.build_failure(
                    phase="precheck",
                    failed_node=precheck.name,
                    message_cn=message_cn,
                    message_en=message_en,
                    state=state,
                )

        for step in document.steps:
            result, trace = self.sql_executor.execute_node(
                step,
                phase="step",
                state=state,
                datasource_registry=datasource_registry,
                node_name=step.name,
            )
            state.add_trace(trace)
            state.set_step_result(step.name, result)

        if self._should_fail_by_policy(document.on_fail, state):
            message_cn, message_en = self.message_renderer.render(document.on_fail, state)
            return self.result_builder.build_failure(
                phase="final",
                failed_node="on_fail",
                message_cn=message_cn,
                message_en=message_en,
                state=state,
            )

        return self.result_builder.build_pass(state)

    def _evaluate_variable(self, definition: VariableDefinition, state: ExecutionState) -> Any:
        for item in definition.when:
            if bool(self.expression_evaluator.evaluate(item.condition, state)):
                return item.value
        return definition.default

    def _should_fail_precheck(
        self,
        policy: FailPolicy,
        rows: list[dict[str, Any]],
        state: ExecutionState,
    ) -> bool:
        if policy.decision == "exists":
            return len(rows) > 0
        return self._should_fail_by_policy(policy, state)

    def _should_fail_by_policy(self, policy: FailPolicy, state: ExecutionState) -> bool:
        return bool(self.expression_evaluator.evaluate(policy.decision, state))
