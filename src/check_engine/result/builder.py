"""执行结果封装器。"""

from __future__ import annotations

from typing import Optional

from ..exceptions import DSLExecutionError
from ..runtime import ExecutionResult, ExecutionState


class ResultBuilder:
    """统一构建最终返回结构。"""

    @staticmethod
    def build_pass(state: ExecutionState) -> ExecutionResult:
        return ResultBuilder._build_result(
            passed=True,
            phase="pass",
            failed_node=None,
            error_code=None,
            error_detail=None,
            message_cn=None,
            message_en=None,
            state=state,
        )

    @staticmethod
    def build_failure(
        phase: str,
        failed_node: str,
        message_cn: str,
        message_en: str,
        state: ExecutionState,
        *,
        error_code: Optional[str] = None,
        error_detail: Optional[str] = None,
    ) -> ExecutionResult:
        return ResultBuilder._build_result(
            passed=False,
            phase=phase,
            failed_node=failed_node,
            error_code=error_code,
            error_detail=error_detail,
            message_cn=message_cn,
            message_en=message_en,
            state=state,
        )

    @staticmethod
    def build_runtime_failure(
        error: DSLExecutionError,
        state: ExecutionState,
        *,
        failed_node: Optional[str] = None,
    ) -> ExecutionResult:
        return ResultBuilder._build_result(
            passed=False,
            phase="runtime",
            failed_node=failed_node,
            error_code=error.code.value,
            error_detail=str(error),
            message_cn=str(error),
            message_en=str(error),
            state=state,
        )

    @staticmethod
    def _build_result(
        *,
        passed: bool,
        phase: str,
        failed_node: Optional[str],
        error_code: Optional[str],
        error_detail: Optional[str],
        message_cn: Optional[str],
        message_en: Optional[str],
        state: ExecutionState,
    ) -> ExecutionResult:
        return ExecutionResult(
            passed=passed,
            phase=phase,
            failed_node=failed_node,
            error_code=error_code,
            error_detail=error_detail,
            message_cn=message_cn,
            message_en=message_en,
            **ResultBuilder._state_payload(state),
        )

    @staticmethod
    def _state_payload(state: ExecutionState) -> dict[str, object]:
        return {
            "context": state.context_data,
            "variables": state.variables_data,
            "steps": state.step_data,
            "executed_nodes": tuple(state.executed_nodes),
        }
