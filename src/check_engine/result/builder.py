"""执行结果封装器。"""

from __future__ import annotations

from ..exceptions import DSLExecutionError
from ..runtime.state import ExecutionResult, ExecutionState


class ResultBuilder:
    """统一构建最终返回结构。"""

    def build_pass(self, state: ExecutionState) -> ExecutionResult:
        return ExecutionResult(
            passed=True,
            phase="pass",
            failed_node=None,
            error_code=None,
            error_detail=None,
            message_cn=None,
            message_en=None,
            context=state.context_data,
            variables=state.variables_data,
            steps=state.step_data,
            executed_nodes=tuple(state.executed_nodes),
        )

    def build_failure(
        self,
        phase: str,
        failed_node: str,
        message_cn: str,
        message_en: str,
        state: ExecutionState,
        *,
        error_code: str | None = None,
        error_detail: str | None = None,
    ) -> ExecutionResult:
        return ExecutionResult(
            passed=False,
            phase=phase,
            failed_node=failed_node,
            error_code=error_code,
            error_detail=error_detail,
            message_cn=message_cn,
            message_en=message_en,
            context=state.context_data,
            variables=state.variables_data,
            steps=state.step_data,
            executed_nodes=tuple(state.executed_nodes),
        )

    def build_runtime_failure(
        self,
        error: DSLExecutionError,
        state: ExecutionState,
        *,
        failed_node: str | None = None,
    ) -> ExecutionResult:
        return ExecutionResult(
            passed=False,
            phase="runtime",
            failed_node=failed_node,
            error_code=error.code.value,
            error_detail=str(error),
            message_cn=str(error),
            message_en=str(error),
            context=state.context_data,
            variables=state.variables_data,
            steps=state.step_data,
            executed_nodes=tuple(state.executed_nodes),
        )
