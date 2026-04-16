"""ExecDSL 表达式求值器。"""

from __future__ import annotations

import ast
import logging
import re
from collections.abc import Collection
from dataclasses import dataclass
from types import CodeType
from typing import Any, Optional

from check_engine.exceptions import DSLExecutionError
from check_engine.reference_parser import ReferenceParser, ReferenceSpec
from check_engine.runtime import ExecutionState


@dataclass(frozen=True)
class CompiledExpression:
    """预编译后的 DSL 表达式。"""

    source: str
    python_expression: str
    references: tuple[str, ...]
    code: CodeType


class _SafeExpressionValidator(ast.NodeVisitor):
    """限制可执行的 AST 节点。"""

    ALLOWED_NODES = (
        ast.Expression,
        ast.BoolOp,
        ast.UnaryOp,
        ast.Compare,
        ast.Name,
        ast.Load,
        ast.Constant,
        ast.Tuple,
        ast.List,
        ast.And,
        ast.Or,
        ast.Not,
        ast.Eq,
        ast.NotEq,
        ast.Gt,
        ast.GtE,
        ast.Lt,
        ast.LtE,
        ast.In,
        ast.NotIn,
        ast.Call,
    )

    def visit_Call(self, node: ast.Call) -> None:
        if not isinstance(node.func, ast.Name) or node.func.id != "exists":
            raise DSLExecutionError("Only exists(...) function is supported in expression.")
        if len(node.args) != 1 or node.keywords:
            raise DSLExecutionError("exists(...) must have exactly one positional argument.")
        self.visit(node.args[0])

    def generic_visit(self, node: ast.AST) -> None:
        if not isinstance(node, self.ALLOWED_NODES):
            raise DSLExecutionError(f"Expression contains unsupported syntax node: {node.__class__.__name__}")
        super().generic_visit(node)


class ExpressionEvaluator:
    """求值 DSL 布尔表达式。"""

    NULL_PATTERN = re.compile(r"\bnull\b")

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        reference_parser: Optional[ReferenceParser] = None,
    ) -> None:
        self.logger = logger or logging.getLogger(__name__)
        self.reference_parser = reference_parser or ReferenceParser()

    def compile(self, expression: str) -> CompiledExpression:
        if expression == "exists":
            raise DSLExecutionError("Keyword 'exists' must be used as exists($path) and cannot be evaluated directly.")

        references: list[str] = []
        ref_names: dict[str, str] = {}

        def replace_reference(reference_spec: ReferenceSpec) -> str:
            reference = reference_spec.explicit
            if reference in ref_names:
                return ref_names[reference]
            ref_name = "__ref_{0}".format(len(references))
            references.append(reference)
            ref_names[reference] = ref_name
            return ref_name

        python_expr = self.reference_parser.replace_references(expression, replace_reference)
        python_expr = self.NULL_PATTERN.sub("None", python_expr)

        try:
            tree = ast.parse(python_expr, mode="eval")
        except SyntaxError as exc:
            raise DSLExecutionError(
                f"Expression syntax error: {expression}",
                original_exception=exc,
            ) from exc

        _SafeExpressionValidator().visit(tree)
        code = compile(tree, "<dsl-expression>", "eval")
        return CompiledExpression(
            source=expression,
            python_expression=python_expr,
            references=tuple(references),
            code=code,
        )

    def evaluate(self, expression: str, state: ExecutionState, local_data: Optional[Any] = None) -> Any:
        return self.evaluate_compiled(self.compile(expression), state, local_data=local_data)

    def evaluate_compiled(
        self,
        expression: CompiledExpression,
        state: ExecutionState,
        local_data: Optional[Any] = None,
    ) -> Any:
        ref_env = {
            "__ref_{0}".format(index): state.resolve_reference(reference, local_data=local_data)
            for index, reference in enumerate(expression.references)
        }

        try:
            return eval(expression.code, {"__builtins__": {}}, {**ref_env, "exists": self._exists})
        except Exception as exc:  # noqa: BLE001
            raise DSLExecutionError(
                f"Expression evaluation failed: {expression.source}",
                original_exception=exc,
            ) from exc

    @staticmethod
    def _exists(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, Collection):
            return len(value) > 0
        return True
