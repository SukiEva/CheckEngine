"""ExecDSL 运行时状态对象。"""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Optional, TypedDict

from ..exceptions import DSLExecutionError
from .reference_resolver import RuntimeReferenceResolver


def _to_plain_data(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _to_plain_data(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_to_plain_data(item) for item in value]
    return value


class _StatePayload(TypedDict):
    context: Mapping[str, Any]
    variables: Mapping[str, Any]
    steps: Mapping[str, Any]
    executed_nodes: Sequence[ExecutedNodeTrace]


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
    message_cn: Optional[str]
    message_en: Optional[str]
    context: Mapping[str, Any]
    variables: Mapping[str, Any]
    steps: Mapping[str, Any]
    executed_nodes: Sequence[ExecutedNodeTrace] = field(default_factory=tuple)

    @staticmethod
    def build_pass(state: "ExecutionState") -> "ExecutionResult":
        return ExecutionResult(
            passed=True,
            phase="pass",
            failed_node=None,
            message_cn=None,
            message_en=None,
            **ExecutionResult._state_payload(state),
        )

    @staticmethod
    def build_failure(
        phase: str,
        failed_node: str,
        message_cn: str,
        message_en: str,
        state: "ExecutionState",
    ) -> "ExecutionResult":
        return ExecutionResult(
            passed=False,
            phase=phase,
            failed_node=failed_node,
            message_cn=message_cn,
            message_en=message_en,
            **ExecutionResult._state_payload(state),
        )

    @staticmethod
    def build_runtime_failure(
        error: DSLExecutionError,
        state: "ExecutionState",
        *,
        failed_node: Optional[str] = None,
    ) -> "ExecutionResult":
        return ExecutionResult(
            passed=False,
            phase="runtime",
            failed_node=failed_node,
            message_cn=str(error),
            message_en=str(error),
            **ExecutionResult._state_payload(state),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "phase": self.phase,
            "failed_node": self.failed_node,
            "message_cn": self.message_cn,
            "message_en": self.message_en,
            "context": _to_plain_data(self.context),
            "variables": _to_plain_data(self.variables),
            "steps": _to_plain_data(self.steps),
            "executed_nodes": [item.to_dict() for item in self.executed_nodes],
        }

    @staticmethod
    def _state_payload(state: "ExecutionState") -> _StatePayload:
        return {
            "context": state.context_data,
            "variables": state.variables_data,
            "steps": state.step_data,
            "executed_nodes": tuple(state.executed_nodes),
        }


@dataclass
class ExecutionState:
    """执行过程中的可变状态。"""

    input_data: Mapping[str, Any]
    context_result: Optional[NodeExecutionResult] = None
    context_data: MutableMapping[str, Any] = field(default_factory=dict)
    variables_data: MutableMapping[str, Any] = field(default_factory=dict)
    prechecks_data: MutableMapping[str, Any] = field(default_factory=dict)
    step_results: MutableMapping[str, NodeExecutionResult] = field(default_factory=dict)
    step_data: MutableMapping[str, Any] = field(default_factory=dict)
    executed_nodes: list[ExecutedNodeTrace] = field(default_factory=list)
    reference_resolver: RuntimeReferenceResolver = field(init=False)

    def __post_init__(self) -> None:
        self.reference_resolver = RuntimeReferenceResolver(
            input_data=self.input_data,
            context_data=self.context_data,
            variables_data=self.variables_data,
            prechecks_data=self.prechecks_data,
            step_data=self.step_data,
        )

    @classmethod
    def new(cls, input_data: Mapping[str, Any]) -> "ExecutionState":
        return cls(input_data=input_data)

    def set_context_result(self, result: NodeExecutionResult) -> None:
        self.context_result = result
        self.context_data.clear()
        if isinstance(result.exported_data, Mapping):
            self.context_data.update(dict(result.exported_data))

    def set_step_result(self, step_name: str, result: NodeExecutionResult) -> None:
        self.step_results[step_name] = result
        self.step_data[step_name] = result.exported_data

    def set_precheck_result(self, precheck_name: str, result: NodeExecutionResult) -> None:
        self.prechecks_data[precheck_name] = result.exported_data

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

    def resolve_reference(self, reference: str, local_data: Optional[Any] = None) -> Any:
        self.reference_resolver.update_sources(
            input_data=self.input_data,
            context_data=self.context_data,
            variables_data=self.variables_data,
            prechecks_data=self.prechecks_data,
            step_data=self.step_data,
        )
        return self.reference_resolver.resolve_reference(reference, local_data=local_data)

    def resolve_path(self, path: str) -> Any:
        return self.resolve_reference(path if path.startswith("$") else "$" + path)

    def get_consumable_rows(self, from_path: str) -> tuple[Sequence[Mapping[str, Any]], list[str]]:
        parts = RuntimeReferenceResolver.parse_reference_parts(from_path)
        if parts == ["context"]:
            if self.context_result is None:
                raise DSLExecutionError(
                    "Context result is missing; cannot build consumes.",
                )
            return self._rows_and_fields(self.context_result)

        if parts[0] != "steps":
            raise DSLExecutionError(
                f"Unsupported consumes.from reference: {from_path}",
            )

        if len(parts) != 2:
            raise DSLExecutionError(
                f"consumes.from only supports referencing whole step outputs: {from_path}",
            )

        step_name = self._require_step_name(parts, from_path)
        if step_name not in self.step_results:
            raise DSLExecutionError(
                f"consumes.from references a non-existent step output: {from_path}",
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
    def _require_step_name(parts: list[str], reference: str) -> str:
        if len(parts) < 2 or not parts[1]:
            raise DSLExecutionError(
                f"Steps reference must include step name: {reference}",
            )
        return parts[1]
