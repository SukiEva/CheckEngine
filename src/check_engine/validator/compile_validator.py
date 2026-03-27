"""DSL 编译前校验与表达式预编译。"""

from __future__ import annotations

import logging
from typing import Optional

from ..dsl import DslDocument, EXISTS_DECISION
from ..expression import CompiledExpression, ExpressionEvaluator
from ..exceptions import DSLExecutionError, DSLValidationError
from .document_validator import DslValidator


class DslCompileValidator:
    """负责 DSL 编译阶段的校验与表达式预编译。"""

    def __init__(
        self,
        dsl_validator: Optional[DslValidator] = None,
        expression_evaluator: Optional[ExpressionEvaluator] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.dsl_validator = dsl_validator or DslValidator()
        self.expression_evaluator = expression_evaluator or ExpressionEvaluator()
        self.logger = logger or logging.getLogger(__name__)

    def validate_and_compile(
        self,
        document: DslDocument,
        run_validation: bool,
    ) -> tuple[dict[str, tuple[CompiledExpression, ...]], dict[str, Optional[CompiledExpression]], CompiledExpression]:
        if run_validation:
            self.dsl_validator.validate(document)

        variable_conditions = {
            variable_name: tuple(
                self._compile_expression(item.condition, f"variables.{variable_name}.when[{index}].condition")
                for index, item in enumerate(definition.when)
            )
            for variable_name, definition in document.variables.items()
        }
        precheck_decisions: dict[str, Optional[CompiledExpression]] = {}
        for precheck in document.prechecks:
            if precheck.on_fail is None:
                raise DSLValidationError(
                    f"prechecks.{precheck.name}.on_fail must be provided.",
                )
            precheck_decisions[precheck.name] = (
                None
                if precheck.on_fail.decision == EXISTS_DECISION
                else self._compile_expression(precheck.on_fail.decision, f"prechecks.{precheck.name}.on_fail.decision")
            )
        on_fail_decision = self._compile_expression(document.on_fail.decision, "on_fail.decision")
        return variable_conditions, precheck_decisions, on_fail_decision

    def _compile_expression(self, expression: str, path: str) -> CompiledExpression:
        try:
            return self.expression_evaluator.compile(expression)
        except DSLExecutionError as exc:
            self.logger.exception("Failed to compile expression at %s: %s", path, expression)
            raise DSLValidationError(
                f"{path} is invalid: {exc}",
            ) from exc
