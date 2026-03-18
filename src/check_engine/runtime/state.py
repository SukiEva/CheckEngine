"""ExecDSL 运行时状态对象。"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Optional

from ..exceptions import DSLExecutionError, ExecutionErrorCode


@dataclass(frozen=True)
class NodeExecutionResult:
    """节点执行结果。"""

    raw_rows: list[dict[str, Any]]
    exported_data: Any
    exported_fields: list[str]

    def as_rows(self) -> list[dict[str, Any]]:
        return list(self.raw_rows)


@dataclass(frozen=True)
class ExecutionResult:
    """最终执行结果。"""

    passed: bool
    phase: str
    failed_node: Optional[str]
    error_code: Optional[str]
    error_detail: Optional[str]
    message_cn: Optional[str]
    message_en: Optional[str]
    context: dict[str, Any]
    variables: dict[str, Any]
    steps: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ExecutionState:
    """执行过程中的可变状态。"""

    input_data: dict[str, Any]
    context_result: Optional[NodeExecutionResult] = None
    context_data: dict[str, Any] = field(default_factory=dict)
    variables_data: dict[str, Any] = field(default_factory=dict)
    step_results: dict[str, NodeExecutionResult] = field(default_factory=dict)
    step_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def new(cls, input_data: dict[str, Any]) -> "ExecutionState":
        return cls(input_data=input_data)

    def set_context_result(self, result: NodeExecutionResult) -> None:
        self.context_result = result
        self.context_data = result.exported_data if isinstance(result.exported_data, dict) else {}

    def set_step_result(self, step_name: str, result: NodeExecutionResult) -> None:
        self.step_results[step_name] = result
        self.step_data[step_name] = result.exported_data

    def resolve_reference(self, reference: str) -> Any:
        if not reference.startswith("$"):
            raise DSLExecutionError(f"Invalid reference path: {reference}", code=ExecutionErrorCode.EXECUTION_ERROR)

        parts = reference[1:].split(".")
        root = parts[0]
        if root == "input":
            return self._resolve_from_mapping(self.input_data, parts[1:], reference)
        if root == "context":
            return self._resolve_from_mapping(self.context_data, parts[1:], reference)
        if root == "variables":
            return self._resolve_from_mapping(self.variables_data, parts[1:], reference)
        if root == "steps":
            if len(parts) < 2:
                raise DSLExecutionError(
                    f"Steps reference must include step name: {reference}",
                    code=ExecutionErrorCode.EXECUTION_ERROR,
                )
            step_name = parts[1]
            if step_name not in self.step_data:
                raise DSLExecutionError(f"Step execution result not found: {reference}", code=ExecutionErrorCode.EXECUTION_ERROR)
            return self._resolve_from_mapping_or_object(self.step_data[step_name], parts[2:], reference)
        raise DSLExecutionError(f"Unknown scope: {reference}", code=ExecutionErrorCode.EXECUTION_ERROR)

    def resolve_path(self, path: str) -> Any:
        return self.resolve_reference(path if path.startswith("$") else "$" + path)

    def get_consumable_rows(self, from_path: str) -> tuple[list[dict[str, Any]], list[str]]:
        if from_path == "$context":
            if self.context_result is None:
                raise DSLExecutionError(
                    "Context result is missing; cannot build consumes.",
                    code=ExecutionErrorCode.EXECUTION_ERROR,
                )
            return self._rows_and_fields(self.context_result)

        if not from_path.startswith("$steps."):
            raise DSLExecutionError(
                f"Unsupported consumes.from reference: {from_path}",
                code=ExecutionErrorCode.EXECUTION_ERROR,
            )

        parts = from_path[1:].split(".")
        if len(parts) != 2:
            raise DSLExecutionError(
                f"consumes.from only supports referencing whole step outputs: {from_path}",
                code=ExecutionErrorCode.EXECUTION_ERROR,
            )

        step_name = parts[1]
        if step_name not in self.step_results:
            raise DSLExecutionError(
                f"consumes.from references a non-existent step output: {from_path}",
                code=ExecutionErrorCode.EXECUTION_ERROR,
            )

        return self._rows_and_fields(self.step_results[step_name])

    def _rows_and_fields(self, result: NodeExecutionResult) -> tuple[list[dict[str, Any]], list[str]]:
        rows = result.as_rows()
        fields = list(result.exported_fields)
        if not fields and rows:
            fields = list(rows[0].keys())
        return rows, fields

    def _resolve_from_mapping(self, mapping: dict[str, Any], parts: list[str], reference: str) -> Any:
        current: Any = mapping
        for part in parts:
            if not isinstance(current, dict):
                raise DSLExecutionError(
                    f"Cannot resolve reference path further: {reference}",
                    code=ExecutionErrorCode.EXECUTION_ERROR,
                )
            if part not in current:
                raise DSLExecutionError(f"Referenced field does not exist: {reference}", code=ExecutionErrorCode.EXECUTION_ERROR)
            current = current[part]
        return current

    def _resolve_from_mapping_or_object(self, value: Any, parts: list[str], reference: str) -> Any:
        current = value
        if not parts:
            return current
        for part in parts:
            if isinstance(current, dict):
                if part not in current:
                    raise DSLExecutionError(
                        f"Referenced field does not exist: {reference}",
                        code=ExecutionErrorCode.EXECUTION_ERROR,
                    )
                current = current[part]
                continue

            if isinstance(current, list):
                projected = []
                for item in current:
                    if not isinstance(item, dict):
                        raise DSLExecutionError(
                            f"Cannot resolve reference path further: {reference}",
                            code=ExecutionErrorCode.EXECUTION_ERROR,
                        )
                    if part not in item:
                        raise DSLExecutionError(
                            f"Referenced field does not exist: {reference}",
                            code=ExecutionErrorCode.EXECUTION_ERROR,
                        )
                    projected.append(item[part])
                current = projected
                continue

            raise DSLExecutionError(
                f"Cannot resolve reference path further: {reference}",
                code=ExecutionErrorCode.EXECUTION_ERROR,
            )
        return current
