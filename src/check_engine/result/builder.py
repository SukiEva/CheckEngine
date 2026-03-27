"""执行结果封装器。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Optional, TypedDict

from ..exceptions import DSLExecutionError
from ..runtime import ExecutedNodeTrace, ExecutionResult, ExecutionState


class _StatePayload(TypedDict):
    context: Mapping[str, Any]
    variables: Mapping[str, Any]
    steps: Mapping[str, Any]
    executed_nodes: Sequence[ExecutedNodeTrace]


class ResultBuilder:
    """统一构建最终返回结构。"""

    @staticmethod
    def build_pass(state: ExecutionState) -> ExecutionResult:
        return ResultBuilder._build_result(
            passed=True,
            phase="pass",
            failed_node=None,
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
    ) -> ExecutionResult:
        return ResultBuilder._build_result(
            passed=False,
            phase=phase,
            failed_node=failed_node,
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
        message_cn: Optional[str],
        message_en: Optional[str],
        state: ExecutionState,
    ) -> ExecutionResult:
        return ExecutionResult(
            passed=passed,
            phase=phase,
            failed_node=failed_node,
            message_cn=message_cn,
            message_en=message_en,
            **ResultBuilder._state_payload(state),
        )

    @staticmethod
    def _state_payload(state: ExecutionState) -> _StatePayload:
        return {
            "context": state.context_data,
            "variables": state.variables_data,
            "steps": state.step_data,
            "executed_nodes": tuple(state.executed_nodes),
        }
