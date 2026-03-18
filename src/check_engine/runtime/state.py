"""ExecDSL 运行时状态对象。"""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Optional

from ..exceptions import DSLExecutionError, ExecutionErrorCode


def _to_plain_data(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _to_plain_data(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_to_plain_data(item) for item in value]
    return value


@dataclass(frozen=True)
class ExecutedNodeTrace:
    """单个节点的执行轨迹。"""

    phase: str
    node_name: str
    datasource: Optional[str]
    result_mode: Optional[str]
    row_count: Optional[int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "phase": self.phase,
            "node_name": self.node_name,
            "datasource": self.datasource,
            "result_mode": self.result_mode,
            "row_count": self.row_count,
        }


@dataclass(frozen=True)
class NodeExecutionResult:
    """节点执行结果。"""

    raw_rows: Sequence[Mapping[str, Any]]
    exported_data: Any
    exported_fields: Sequence[str]

    def __post_init__(self) -> None:
        frozen_rows = tuple(MappingProxyType(dict(row)) for row in self.raw_rows)
        object.__setattr__(self, "raw_rows", frozen_rows)
        object.__setattr__(self, "exported_fields", tuple(self.exported_fields))

    def as_rows(self) -> Sequence[Mapping[str, Any]]:
        return self.raw_rows


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
    context: Mapping[str, Any]
    variables: Mapping[str, Any]
    steps: Mapping[str, Any]
    executed_nodes: Sequence[ExecutedNodeTrace] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "phase": self.phase,
            "failed_node": self.failed_node,
            "error_code": self.error_code,
            "error_detail": self.error_detail,
            "message_cn": self.message_cn,
            "message_en": self.message_en,
            "context": _to_plain_data(self.context),
            "variables": _to_plain_data(self.variables),
            "steps": _to_plain_data(self.steps),
            "executed_nodes": [item.to_dict() for item in self.executed_nodes],
        }


@dataclass
class ExecutionState:
    """执行过程中的可变状态。"""

    input_data: Mapping[str, Any]
    context_result: Optional[NodeExecutionResult] = None
    context_data: Mapping[str, Any] = field(default_factory=dict)
    variables_data: MutableMapping[str, Any] = field(default_factory=dict)
    step_results: MutableMapping[str, NodeExecutionResult] = field(default_factory=dict)
    step_data: MutableMapping[str, Any] = field(default_factory=dict)
    executed_nodes: list[ExecutedNodeTrace] = field(default_factory=list)

    @classmethod
    def new(cls, input_data: Mapping[str, Any]) -> "ExecutionState":
        return cls(input_data=input_data)

    def set_context_result(self, result: NodeExecutionResult) -> None:
        self.context_result = result
        self.context_data = dict(result.exported_data) if isinstance(result.exported_data, Mapping) else {}

    def set_step_result(self, step_name: str, result: NodeExecutionResult) -> None:
        self.step_results[step_name] = result
        self.step_data[step_name] = result.exported_data

    def record_node_execution(
        self,
        *,
        phase: str,
        node_name: str,
        datasource: Optional[str],
        result_mode: Optional[str],
        row_count: Optional[int],
    ) -> None:
        self.executed_nodes.append(
            ExecutedNodeTrace(
                phase=phase,
                node_name=node_name,
                datasource=datasource,
                result_mode=result_mode,
                row_count=row_count,
            )
        )

    def resolve_reference(self, reference: str) -> Any:
        parts = self._parse_reference_parts(reference)
        root = parts[0]
        if root == "input":
            return self._resolve_from_mapping(self.input_data, parts[1:], reference)
        if root == "context":
            return self._resolve_from_mapping(self.context_data, parts[1:], reference)
        if root == "variables":
            return self._resolve_from_mapping(self.variables_data, parts[1:], reference)
        if root == "steps":
            step_name = self._require_step_name(parts, reference)
            step_value = self._get_step_data(step_name, reference)
            return self._resolve_from_mapping_or_object(step_value, parts[2:], reference)
        raise DSLExecutionError(f"Unknown scope: {reference}", code=ExecutionErrorCode.EXECUTION_ERROR)

    def resolve_path(self, path: str) -> Any:
        return self.resolve_reference(path if path.startswith("$") else "$" + path)

    def get_consumable_rows(self, from_path: str) -> tuple[Sequence[Mapping[str, Any]], list[str]]:
        parts = self._parse_reference_parts(from_path)
        if parts == ["context"]:
            if self.context_result is None:
                raise DSLExecutionError(
                    "Context result is missing; cannot build consumes.",
                    code=ExecutionErrorCode.EXECUTION_ERROR,
                )
            return self._rows_and_fields(self.context_result)

        if parts[0] != "steps":
            raise DSLExecutionError(
                f"Unsupported consumes.from reference: {from_path}",
                code=ExecutionErrorCode.EXECUTION_ERROR,
            )

        if len(parts) != 2:
            raise DSLExecutionError(
                f"consumes.from only supports referencing whole step outputs: {from_path}",
                code=ExecutionErrorCode.EXECUTION_ERROR,
            )

        step_name = self._require_step_name(parts, from_path)
        if step_name not in self.step_results:
            raise DSLExecutionError(
                f"consumes.from references a non-existent step output: {from_path}",
                code=ExecutionErrorCode.EXECUTION_ERROR,
            )

        return self._rows_and_fields(self.step_results[step_name])

    @staticmethod
    def _rows_and_fields(result: NodeExecutionResult) -> tuple[Sequence[Mapping[str, Any]], list[str]]:
        rows = result.as_rows()
        fields = list(result.exported_fields)
        if not fields and rows:
            fields = list(rows[0].keys())
        return rows, fields

    @staticmethod
    def _resolve_from_mapping(mapping: Mapping[str, Any], parts: list[str], reference: str) -> Any:
        current: Any = mapping
        for part in parts:
            if not isinstance(current, Mapping):
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
            if isinstance(current, Mapping):
                if part not in current:
                    raise DSLExecutionError(
                        f"Referenced field does not exist: {reference}",
                        code=ExecutionErrorCode.EXECUTION_ERROR,
                    )
                current = current[part]
                continue

            if self._is_projectable_sequence(current):
                projected = []
                for item in current:
                    if not isinstance(item, Mapping):
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

    @staticmethod
    def _is_projectable_sequence(value: Any) -> bool:
        return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))

    @staticmethod
    def _parse_reference_parts(reference: str) -> list[str]:
        if not reference.startswith("$"):
            raise DSLExecutionError(f"Invalid reference path: {reference}", code=ExecutionErrorCode.EXECUTION_ERROR)
        return reference[1:].split(".")

    @staticmethod
    def _require_step_name(parts: list[str], reference: str) -> str:
        if len(parts) < 2 or not parts[1]:
            raise DSLExecutionError(
                f"Steps reference must include step name: {reference}",
                code=ExecutionErrorCode.EXECUTION_ERROR,
            )
        return parts[1]

    def _get_step_data(self, step_name: str, reference: str) -> Any:
        if step_name not in self.step_data:
            raise DSLExecutionError(f"Step execution result not found: {reference}", code=ExecutionErrorCode.EXECUTION_ERROR)
        return self.step_data[step_name]
