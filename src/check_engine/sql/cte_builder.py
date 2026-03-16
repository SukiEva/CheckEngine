"""根据 consumes 生成 CTE。"""

from __future__ import annotations

from typing import Any

from check_engine.dsl.models import ConsumeSpec
from check_engine.exceptions import DSLExecutionError
from check_engine.runtime.state import ExecutionState


class CteBuilder:
    """将前序结果集转换为当前 SQL 可消费的 CTE。"""

    def build(self, consumes: list[ConsumeSpec], state: ExecutionState) -> tuple[str, dict[str, Any]]:
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
        rows: list[dict[str, Any]],
        fields: list[str],
    ) -> tuple[str, dict[str, Any]]:
        if not fields:
            raise DSLExecutionError(f"CTE {alias} 无法推断列名，请显式配置 outputs。")

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
                params[param_name] = row.get(field)
                placeholders.append(":{0}".format(param_name))
            value_rows.append("(" + ", ".join(placeholders) + ")")

        sql = "{0}({1}) AS (VALUES {2})".format(alias, column_sql, ", ".join(value_rows))
        return sql, params
