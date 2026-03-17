"""执行结果封装器。"""

from __future__ import annotations

from ..runtime.state import ExecutionResult, ExecutionState


class ResultBuilder:
    """统一构建最终返回结构。"""

    def build_pass(self, state: ExecutionState) -> ExecutionResult:
        return ExecutionResult(
            passed=True,
            phase="pass",
            failed_node=None,
            message_cn=None,
            message_en=None,
            context=state.context_data,
            variables=state.variables_data,
            steps=state.step_data,
        )

    def build_failure(
        self,
        phase: str,
        failed_node: str,
        message_cn: str,
        message_en: str,
        state: ExecutionState,
    ) -> ExecutionResult:
        return ExecutionResult(
            passed=False,
            phase=phase,
            failed_node=failed_node,
            message_cn=message_cn,
            message_en=message_en,
            context=state.context_data,
            variables=state.variables_data,
            steps=state.step_data,
        )
