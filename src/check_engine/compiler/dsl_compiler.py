"""DSL 编译：校验与表达式预编译。"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from ..dsl import DslDocument
from ..exceptions import DSLExecutionError, DSLValidationError
from ..expression import CompiledExpression, ExpressionEvaluator


@dataclass(frozen=True)
class CompiledDsl:
    """已完成解析与表达式预编译的 DSL。"""

    document: DslDocument
    variable_conditions: dict[str, tuple[CompiledExpression, ...]]
    precheck_decisions: dict[str, CompiledExpression]
    on_fail_decision: CompiledExpression


class DslCompiler:
    """负责 DSL 的表达式预编译。"""

    def __init__(
        self,
        expression_evaluator: ExpressionEvaluator,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.expression_evaluator = expression_evaluator
        self.logger = logger or logging.getLogger(__name__)

    def compile(self, document: DslDocument) -> CompiledDsl:
        variable_conditions = {
            variable_name: tuple(
                self._compile_expression(item.condition, f"variables.{variable_name}.when[{index}].condition")
                for index, item in enumerate(definition.when)
            )
            for variable_name, definition in document.variables.items()
        }
        precheck_decisions: dict[str, CompiledExpression] = {}
        for precheck in document.prechecks:
            if precheck.on_fail is None:
                raise DSLValidationError(
                    f"prechecks.{precheck.name}.on_fail must be provided.",
                )
            precheck_decisions[precheck.name] = self._compile_expression(
                precheck.on_fail.decision,
                f"prechecks.{precheck.name}.on_fail.decision",
            )
        on_fail_decision = self._compile_expression(document.on_fail.decision, "on_fail.decision")
        return CompiledDsl(
            document=document,
            variable_conditions=variable_conditions,
            precheck_decisions=precheck_decisions,
            on_fail_decision=on_fail_decision,
        )

    def _compile_expression(self, expression: str, path: str) -> CompiledExpression:
        try:
            return self.expression_evaluator.compile(expression)
        except DSLExecutionError as exc:
            self.logger.exception("Failed to compile expression at %s: %s", path, expression)
            raise DSLValidationError(
                f"{path} is invalid: {exc}",
            ) from exc
