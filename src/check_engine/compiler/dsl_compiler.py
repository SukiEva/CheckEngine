"""DSL 编译：校验与表达式预编译。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from ..dsl import DslDocument, NODE_TYPE_VARIABLE, StepNode, VariableStepNode
from ..exceptions import DSLExecutionError, DSLValidationError
from ..expression import CompiledExpression, ExpressionEvaluator


@dataclass(frozen=True)
class CompiledDsl:
    """已完成解析与表达式预编译的 DSL。"""

    document: DslDocument
    variable_conditions: dict[str, tuple[CompiledExpression, ...]]
    step_variable_conditions: dict[str, tuple[CompiledExpression, ...]]
    step_fail_decisions: dict[str, CompiledExpression]
    step_pass_decisions: dict[str, CompiledExpression]
    on_fail_decision: CompiledExpression


class DslCompiler:
    """负责 DSL 的表达式预编译。"""

    def __init__(
        self,
        expression_evaluator: ExpressionEvaluator,
    ) -> None:
        self.expression_evaluator = expression_evaluator
        self.step_compilers: dict[str, Callable[[StepNode], tuple[CompiledExpression, ...]]] = {
            NODE_TYPE_VARIABLE: self._compile_variable_step_conditions,
        }

    def compile(self, document: DslDocument) -> CompiledDsl:
        variable_conditions = {
            variable_name: tuple(
                self._compile_expression(item.condition, f"variables.{variable_name}.when[{index}].condition")
                for index, item in enumerate(definition.when)
            )
            for variable_name, definition in document.variables.items()
        }
        step_fail_decisions: dict[str, CompiledExpression] = {}
        step_pass_decisions: dict[str, CompiledExpression] = {}
        step_variable_conditions: dict[str, tuple[CompiledExpression, ...]] = {}
        for step in document.steps:
            step_compiler = self.step_compilers.get(step.type)
            if step_compiler is not None:
                step_variable_conditions[step.name] = step_compiler(step)
            if step.on_fail is not None:
                step_fail_decisions[step.name] = self._compile_expression(
                    step.on_fail.decision,
                    f"steps.{step.name}.on_fail.decision",
                )
            if step.on_pass is not None:
                step_pass_decisions[step.name] = self._compile_expression(
                    step.on_pass.decision,
                    f"steps.{step.name}.on_pass.decision",
                )
        on_fail_decision = self._compile_expression(document.on_fail.decision, "on_fail.decision")
        return CompiledDsl(
            document=document,
            variable_conditions=variable_conditions,
            step_variable_conditions=step_variable_conditions,
            step_fail_decisions=step_fail_decisions,
            step_pass_decisions=step_pass_decisions,
            on_fail_decision=on_fail_decision,
        )

    def _compile_expression(self, expression: str, path: str) -> CompiledExpression:
        try:
            return self.expression_evaluator.compile(expression)
        except DSLExecutionError as exc:
            raise DSLValidationError(
                f"{path} is invalid: {exc}",
                original_exception=exc,
            ) from exc

    def _compile_variable_step_conditions(self, step: StepNode) -> tuple[CompiledExpression, ...]:
        if not isinstance(step, VariableStepNode):
            return ()
        return tuple(
            self._compile_expression(item.condition, f"steps.{step.name}.when[{index}].condition")
            for index, item in enumerate(step.when)
        )
