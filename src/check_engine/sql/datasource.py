"""数据源注册表。"""

from __future__ import annotations

from typing import Any, Mapping, Protocol

from check_engine.exceptions import DSLExecutionError


class DatasourceRegistry(Protocol):
    """数据源注册表协议。"""

    def get(self, name: str) -> Any:
        """根据名称返回数据源对象。"""


class StaticDatasourceRegistry:
    """基于固定映射的数据源注册表。"""

    def __init__(self, mapping: Mapping[str, Any]) -> None:
        self._mapping = dict(mapping)

    def get(self, name: str) -> Any:
        if name not in self._mapping:
            raise DSLExecutionError(f"未找到数据源: {name}")
        return self._mapping[name]
