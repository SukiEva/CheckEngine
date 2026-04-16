"""运行时引用解析策略。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Optional

from check_engine.exceptions import DSLExecutionError
from check_engine.reference_parser import ReferenceParser, ReferenceSpec


def _is_projectable_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _resolve_path_part(current: Any, part: str, reference: str) -> Any:
    """将单个路径片段应用于当前值，支持 Mapping 查字段和 Sequence 投影。"""
    if isinstance(current, Mapping):
        if part not in current:
            raise DSLExecutionError(f"Referenced field does not exist: {reference}")
        return current[part]

    if _is_projectable_sequence(current):
        projected: list[Any] = []
        for item in current:
            if not isinstance(item, Mapping):
                raise DSLExecutionError(f"Cannot resolve reference path further: {reference}")
            if part not in item:
                raise DSLExecutionError(f"Referenced field does not exist: {reference}")
            projected.append(item[part])
        return projected

    raise DSLExecutionError(f"Cannot resolve reference path further: {reference}")


class ScopeResolver(ABC):
    """作用域解析策略接口。"""

    @abstractmethod
    def resolve(self, reference_spec: ReferenceSpec) -> Any:
        """根据路径片段解析引用值。"""


@dataclass
class MappingScopeResolver(ScopeResolver):
    """基于 Mapping 的简单作用域解析器。"""

    source: Mapping[str, Any]

    def resolve(self, reference_spec: ReferenceSpec) -> Any:
        current: Any = self.source
        for part in reference_spec.path:
            if not isinstance(current, Mapping):
                raise DSLExecutionError(
                    f"Cannot resolve reference path further: {reference_spec.explicit}",
                )
            if part not in current:
                raise DSLExecutionError(f"Referenced field does not exist: {reference_spec.explicit}")
            current = current[part]
        return current


@dataclass
class StepScopeResolver(ScopeResolver):
    """$steps 作用域解析器，支持序列投影。"""

    step_data: MutableMapping[str, Any]

    def resolve(self, reference_spec: ReferenceSpec) -> Any:
        step_name = self._require_step_name(reference_spec)
        if step_name not in self.step_data:
            raise DSLExecutionError(f"Step execution result not found: {reference_spec.explicit}")

        current = self.step_data[step_name]
        for part in reference_spec.path[1:]:
            current = _resolve_path_part(current, part, reference_spec.explicit)
        return current

    @staticmethod
    def _require_step_name(reference_spec: ReferenceSpec) -> str:
        if not reference_spec.path or not reference_spec.path[0]:
            raise DSLExecutionError(
                f"Steps reference must include step name: {reference_spec.explicit}",
            )
        return reference_spec.path[0]


@dataclass
class RuntimeReferenceResolver:
    """运行时引用解析入口，使用策略模式分发到不同作用域。"""

    input_data: Mapping[str, Any]
    variables_data: Mapping[str, Any]
    step_data: MutableMapping[str, Any] = field(default_factory=dict)
    reference_parser: ReferenceParser = field(default_factory=ReferenceParser)
    _resolvers: dict[str, ScopeResolver] = field(init=False)

    def __post_init__(self) -> None:
        self._resolvers = self._build_resolvers()

    def update_sources(
        self,
        *,
        input_data: Mapping[str, Any],
        variables_data: Mapping[str, Any],
        step_data: MutableMapping[str, Any],
    ) -> None:
        if (
            self.input_data is input_data
            and self.variables_data is variables_data
            and self.step_data is step_data
        ):
            return

        self.input_data = input_data
        self.variables_data = variables_data
        self.step_data = step_data
        self._resolvers = self._build_resolvers()

    def _build_resolvers(self) -> dict[str, ScopeResolver]:
        return {
            "input": MappingScopeResolver(self.input_data),
            "variables": MappingScopeResolver(self.variables_data),
            "steps": StepScopeResolver(self.step_data),
        }

    def resolve_reference(self, reference: str, local_data: Optional[Any] = None) -> Any:
        reference_spec = self.reference_parser.parse(reference)
        if reference_spec.is_local:
            return self._resolve_local_reference(reference_spec, local_data)

        scope = reference_spec.scope
        resolver = self._resolvers.get(scope)
        if resolver is None:
            raise DSLExecutionError(f"Unknown scope: {reference_spec.explicit}")
        return resolver.resolve(reference_spec)

    def _resolve_local_reference(self, reference_spec: ReferenceSpec, local_data: Optional[Any]) -> Any:
        if local_data is None:
            raise DSLExecutionError(
                f"Local scope is not available for reference: {reference_spec.explicit}",
            )

        if not reference_spec.path:
            return local_data

        current: Any = local_data
        for part in reference_spec.path:
            current = self._resolve_local_part(current, part, reference_spec.explicit)
        return current

    def _resolve_local_part(self, current: Any, part: str, reference: str) -> Any:
        return _resolve_path_part(current, part, reference)

    def parse_reference(self, reference: str) -> ReferenceSpec:
        return self.reference_parser.parse(reference)
