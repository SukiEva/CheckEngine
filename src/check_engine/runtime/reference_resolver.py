"""运行时引用解析策略。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Optional

from ..exceptions import DSLExecutionError


class ScopeResolver(ABC):
    """作用域解析策略接口。"""

    @abstractmethod
    def resolve(self, parts: Sequence[str], reference: str) -> Any:
        """根据路径片段解析引用值。"""


@dataclass
class MappingScopeResolver(ScopeResolver):
    """基于 Mapping 的简单作用域解析器。"""

    source: Mapping[str, Any]

    def resolve(self, parts: Sequence[str], reference: str) -> Any:
        current: Any = self.source
        for part in parts:
            if not isinstance(current, Mapping):
                raise DSLExecutionError(
                    f"Cannot resolve reference path further: {reference}",
                )
            if part not in current:
                raise DSLExecutionError(f"Referenced field does not exist: {reference}")
            current = current[part]
        return current


@dataclass
class StepScopeResolver(ScopeResolver):
    """$steps 作用域解析器，支持序列投影。"""

    step_data: MutableMapping[str, Any]

    def resolve(self, parts: Sequence[str], reference: str) -> Any:
        step_name = self._require_step_name(parts, reference)
        if step_name not in self.step_data:
            raise DSLExecutionError(f"Step execution result not found: {reference}")

        current = self.step_data[step_name]
        for part in parts[1:]:
            current = self._resolve_next(current, part, reference)
        return current

    def _resolve_next(self, current: Any, part: str, reference: str) -> Any:
        if isinstance(current, Mapping):
            if part not in current:
                raise DSLExecutionError(
                    f"Referenced field does not exist: {reference}",
                )
            return current[part]

        if self._is_projectable_sequence(current):
            projected: list[Any] = []
            for item in current:
                if not isinstance(item, Mapping):
                    raise DSLExecutionError(
                        f"Cannot resolve reference path further: {reference}",
                    )
                if part not in item:
                    raise DSLExecutionError(
                        f"Referenced field does not exist: {reference}",
                    )
                projected.append(item[part])
            return projected

        raise DSLExecutionError(
            f"Cannot resolve reference path further: {reference}",
        )

    @staticmethod
    def _require_step_name(parts: Sequence[str], reference: str) -> str:
        if len(parts) < 1 or not parts[0]:
            raise DSLExecutionError(
                f"Steps reference must include step name: {reference}",
            )
        return parts[0]

    @staticmethod
    def _is_projectable_sequence(value: Any) -> bool:
        return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


@dataclass
class RuntimeReferenceResolver:
    """运行时引用解析入口，使用策略模式分发到不同作用域。"""

    input_data: Mapping[str, Any]
    context_data: Mapping[str, Any]
    variables_data: Mapping[str, Any]
    prechecks_data: MutableMapping[str, Any] = field(default_factory=dict)
    step_data: MutableMapping[str, Any] = field(default_factory=dict)
    _resolvers: dict[str, ScopeResolver] = field(init=False)

    def __post_init__(self) -> None:
        self._resolvers = self._build_resolvers()

    def update_sources(
        self,
        *,
        input_data: Mapping[str, Any],
        context_data: Mapping[str, Any],
        variables_data: Mapping[str, Any],
        prechecks_data: MutableMapping[str, Any],
        step_data: MutableMapping[str, Any],
    ) -> None:
        if (
            self.input_data is input_data
            and self.context_data is context_data
            and self.variables_data is variables_data
            and self.prechecks_data is prechecks_data
            and self.step_data is step_data
        ):
            return

        self.input_data = input_data
        self.context_data = context_data
        self.variables_data = variables_data
        self.prechecks_data = prechecks_data
        self.step_data = step_data
        self._resolvers = self._build_resolvers()

    def _build_resolvers(self) -> dict[str, ScopeResolver]:
        return {
            "input": MappingScopeResolver(self.input_data),
            "context": MappingScopeResolver(self.context_data),
            "variables": MappingScopeResolver(self.variables_data),
            "prechecks": StepScopeResolver(self.prechecks_data),
            "steps": StepScopeResolver(self.step_data),
        }

    def resolve_reference(self, reference: str, local_data: Optional[Any] = None) -> Any:
        if reference.startswith("$."):
            return self._resolve_local_reference(reference, local_data)

        parts = self.parse_reference_parts(reference)
        scope = parts[0]
        resolver = self._resolvers.get(scope)
        if resolver is None:
            raise DSLExecutionError(f"Unknown scope: {reference}")
        return resolver.resolve(parts[1:], reference)

    def _resolve_local_reference(self, reference: str, local_data: Optional[Any]) -> Any:
        if local_data is None:
            raise DSLExecutionError(f"Local scope is not available for reference: {reference}")

        suffix = reference[2:]
        if not suffix:
            raise DSLExecutionError(f"Invalid reference path: {reference}")

        parts = suffix.split(".")
        current: Any = local_data
        for part in parts:
            if not part:
                raise DSLExecutionError(f"Invalid reference path: {reference}")
            current = self._resolve_local_part(current, part, reference)
        return current

    def _resolve_local_part(self, current: Any, part: str, reference: str) -> Any:
        if isinstance(current, Mapping):
            if part not in current:
                raise DSLExecutionError(f"Referenced field does not exist: {reference}")
            return current[part]

        if StepScopeResolver._is_projectable_sequence(current):
            projected: list[Any] = []
            for item in current:
                if not isinstance(item, Mapping):
                    raise DSLExecutionError(
                        f"Cannot resolve reference path further: {reference}",
                    )
                if part not in item:
                    raise DSLExecutionError(f"Referenced field does not exist: {reference}")
                projected.append(item[part])
            return projected

        raise DSLExecutionError(
            f"Cannot resolve reference path further: {reference}",
        )

    @staticmethod
    def parse_reference_parts(reference: str) -> list[str]:
        if not reference.startswith("$"):
            raise DSLExecutionError(f"Invalid reference path: {reference}")
        if reference.startswith("$."):
            raise DSLExecutionError(
                f"Local reference requires runtime local scope and cannot be parsed globally: {reference}",
            )
        return reference[1:].split(".")
