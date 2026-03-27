"""数据源注册表。"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager
from typing import Any, Protocol, Union, runtime_checkable

from ..exceptions import DSLExecutionError


@runtime_checkable
class SessionLike(Protocol):
    """最小 SQLAlchemy Session 协议。"""

    def execute(self, statement: Any, params: Mapping[str, Any]) -> Any:
        ...


@runtime_checkable
class DatasourceLike(Protocol):
    """可供执行器使用的数据源协议。"""

    def get_session(self) -> Union[AbstractContextManager[SessionLike], Iterator[SessionLike]]:
        ...


@runtime_checkable
class DatasourceRegistry(Protocol):
    """数据源注册表协议。"""

    def get(self, name: str) -> DatasourceLike:
        ...


class StaticDatasourceRegistry:
    """基于固定映射的数据源注册表。"""

    def __init__(self, mapping: Mapping[str, DatasourceLike]) -> None:
        self._mapping = dict(mapping)

    def get(self, name: str) -> DatasourceLike:
        if name not in self._mapping:
            raise DSLExecutionError(f"Datasource not found: {name}")
        return self._mapping[name]
