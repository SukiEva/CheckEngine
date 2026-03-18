"""数据源注册表。"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager
from typing import Any, Protocol, runtime_checkable

from ..exceptions import DSLExecutionError, ExecutionErrorCode


@runtime_checkable
class SessionLike(Protocol):
    """最小 SQLAlchemy Session 协议。"""

    def execute(self, statement: Any, params: Mapping[str, Any]) -> Any:
        """执行 SQL。"""


@runtime_checkable
class DatasourceLike(Protocol):
    """可供执行器使用的数据源协议。"""

    def get_session(self) -> AbstractContextManager[SessionLike] | Iterator[SessionLike]:
        """返回 Session 上下文管理器或可包装的生成器。"""


@runtime_checkable
class DatasourceRegistry(Protocol):
    """数据源注册表协议。"""

    def get(self, name: str) -> DatasourceLike:
        """根据名称返回数据源对象。"""


class StaticDatasourceRegistry:
    """基于固定映射的数据源注册表。"""

    def __init__(self, mapping: Mapping[str, DatasourceLike]) -> None:
        self._mapping = dict(mapping)

    def get(self, name: str) -> DatasourceLike:
        if name not in self._mapping:
            raise DSLExecutionError(f"Datasource not found: {name}", code=ExecutionErrorCode.DATASOURCE_NOT_FOUND)
        return self._mapping[name]
