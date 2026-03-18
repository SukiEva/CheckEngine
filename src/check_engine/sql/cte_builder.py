"""根据 consumes 生成 CTE。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from typing import Protocol

from ..dsl import ConsumeSpec
from ..exceptions import DSLExecutionError


class ConsumableRowsState(Protocol):
    """CTE 构造依赖的最小状态协议。"""

    def get_consumable_rows(self, from_path: str) -> tuple[Sequence[Mapping[str, Any]], list[str]]:
        ...


class CteBuilder:
    """将前序结果集转换为当前 SQL 可消费的 CTE。"""

    def build(self, consumes: Sequence[ConsumeSpec], state: ConsumableRowsState) -> tuple[str, dict[str, Any]]:
        if not consumes:
            return "", {}

        fragments = []
        params: dict[str, Any] = {}

        for consume in consumes:
            rows, fields = state.get_consumable_rows(consume.from_path)
            fragment, fragment_params = self._build_single_cte(consume.alias, rows, fields)
            fragments.append(fragment)
            params.update(fragment_params)

        return "WITH " + ", ".join(fragments), params

    def _build_single_cte(
        self,
        alias: str,
        rows: Sequence[Mapping[str, Any]],
        fields: list[str],
    ) -> tuple[str, dict[str, Any]]:
        if not fields:
            raise DSLExecutionError(f"CTE {alias} cannot infer column names; please configure outputs explicitly.")

        params: dict[str, Any] = {}
        column_sql = ", ".join(fields)

        if not rows:
            null_columns = ", ".join(["NULL AS {0}".format(field) for field in fields])
            return "{0}({1}) AS (SELECT {2} WHERE 1=0)".format(alias, column_sql, null_columns), params

        value_rows = []
        for row_index, row in enumerate(rows):
            placeholders = []
            for field in fields:
                param_name = "__cte_{0}_{1}_{2}".format(alias, row_index, field)
                params[param_name] = self._preserve_parameter_value(row.get(field))
                placeholders.append(":{0}".format(param_name))
            value_rows.append("(" + ", ".join(placeholders) + ")")

        sql = "{0}({1}) AS (VALUES {2})".format(alias, column_sql, ", ".join(value_rows))
        return sql, params

    @staticmethod
    def _preserve_parameter_value(value: Any) -> Any:
        """保留参数原始对象，避免提前字符串化导致精度或格式变化。"""

        return value
