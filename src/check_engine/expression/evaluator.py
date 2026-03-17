"""ExecDSL 表达式求值器。"""

from __future__ import annotations

import ast
import re
from typing import Any

from ..exceptions import DSLExecutionError
from ..runtime.state import ExecutionState


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

    REF_PATTERN = re.compile(r"\$[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*")
    NULL_PATTERN = re.compile(r"\bnull\b")

    def evaluate(self, expression: str, state: ExecutionState) -> Any:
        if expression == "exists":
            raise DSLExecutionError("Keyword 'exists' is only valid for precheck failure decision and must not be evaluated directly.")

        ref_env = {}

        def exists(value: Any) -> bool:
            if value is None:
                return False
            if isinstance(value, (str, bytes, list, tuple, dict, set)):
                return len(value) > 0
            return True

        def replace_reference(match: re.Match[str]) -> str:
            ref_name = "__ref_{0}".format(len(ref_env))
            ref_env[ref_name] = state.resolve_reference(match.group(0))
            return ref_name

        python_expr = self.REF_PATTERN.sub(replace_reference, expression)
        python_expr = self.NULL_PATTERN.sub("None", python_expr)

        try:
            tree = ast.parse(python_expr, mode="eval")
        except SyntaxError as exc:
            raise DSLExecutionError(f"Expression syntax error: {expression}") from exc

        _SafeExpressionValidator().visit(tree)

        try:
            return eval(compile(tree, "<dsl-expression>", "eval"), {"__builtins__": {}}, {**ref_env, "exists": exists})
        except Exception as exc:  # noqa: BLE001
            raise DSLExecutionError(f"Expression evaluation failed: {expression}") from exc
