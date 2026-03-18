"""SQL 节点执行器。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from typing import Any, Optional, Protocol

from ..dsl import SqlNode, StepNode
from ..exceptions import DSLExecutionError, ExecutionErrorCode
from ..runtime import NodeExecutionResult
from .cte_builder import CteBuilder
from .datasource import DatasourceLike, DatasourceRegistry, SessionLike


class ExecutionStateLike(Protocol):
    """执行器依赖的最小运行时状态协议。"""

    def resolve_reference(self, reference: str) -> Any:
        """解析运行时路径。"""

    def get_consumable_rows(self, from_path: str) -> tuple[Sequence[Mapping[str, Any]], list[str]]:
        """返回可供 consumes 构造 CTE 的行和字段。"""


class SqlExecutor:
    """执行 context、precheck、step 中的 SQL 节点。"""

    def __init__(self, cte_builder: Optional[CteBuilder] = None) -> None:
        self.cte_builder = cte_builder or CteBuilder()

    def execute_node(
        self,
        node: SqlNode,
        state: ExecutionStateLike,
        datasource_registry: DatasourceRegistry,
        node_name: str,
    ) -> NodeExecutionResult:
        resolved_params = self._resolve_sql_params(node.sql_params, state)
        consumes = node.consumes if isinstance(node, StepNode) else []
        cte_sql, cte_params = self.cte_builder.build(consumes, state)
        final_sql = self._merge_with_clause(cte_sql, node.sql_template)
        final_params = {**cte_params, **resolved_params}

        try:
            datasource = datasource_registry.get(node.datasource)
            rows = self._run_sql(datasource, final_sql, final_params, node.result_mode)
            exported_data, exported_fields = self._project_outputs(node, node_name, rows)
        except Exception as exc:  # noqa: BLE001
            if isinstance(exc, DSLExecutionError):
                raise
            raise DSLExecutionError(
                f"SQL node execution failed: {node_name}",
                code=ExecutionErrorCode.SQL_EXECUTION_FAILED,
            ) from exc

        return NodeExecutionResult(
            raw_rows=rows,
            exported_data=exported_data,
            exported_fields=exported_fields,
        )

    @staticmethod
    def _resolve_sql_params(sql_params: Mapping[str, Any], state: ExecutionStateLike) -> dict[str, Any]:
        resolved: dict[str, Any] = {}
        for key, value in sql_params.items():
            resolved[key] = state.resolve_reference(value) if isinstance(value, str) and value.startswith("$") else value
        return resolved

    def _merge_with_clause(self, cte_sql: str, sql_template: str) -> str:
        if not cte_sql:
            return sql_template

        leading_prefix, stripped = self._split_leading_comments(sql_template)
        lowered = stripped.lower()
        cte_definitions = cte_sql[4:].lstrip()
        if lowered.startswith("with recursive"):
            remainder = stripped[len("with recursive"):].lstrip()
            return f"{leading_prefix}WITH RECURSIVE {cte_definitions}, {remainder}"
        if lowered.startswith("with"):
            remainder = stripped[4:].lstrip()
            return f"{leading_prefix}WITH {cte_definitions}, {remainder}"
        return cte_sql + " " + sql_template

    @staticmethod
    def _split_leading_comments(sql: str) -> tuple[str, str]:
        index = 0
        length = len(sql)
        while index < length:
            if sql[index].isspace():
                index += 1
                continue
            if sql.startswith("--", index):
                line_end = sql.find("\n", index)
                index = length if line_end == -1 else line_end + 1
                continue
            if sql.startswith("/*", index):
                comment_end = sql.find("*/", index + 2)
                if comment_end == -1:
                    return sql, ""
                index = comment_end + 2
                continue
            break
        return sql[:index], sql[index:]

    def _run_sql(
        self,
        datasource: DatasourceLike,
        sql: str,
        params: dict[str, Any],
        result_mode: str = "records",
    ) -> list[dict[str, Any]]:
        if not hasattr(datasource, "get_session"):
            raise DSLExecutionError(
                "Datasource must provide get_session and execute SQL through a SQLAlchemy Session.",
                code=ExecutionErrorCode.DATASOURCE_NOT_FOUND,
            )

        from sqlalchemy import text as sqlalchemy_text

        session_cm = self._open_session(datasource)
        with session_cm as session:
            result = session.execute(sqlalchemy_text(sql), params)
            mappings = result.mappings()
            if result_mode == "record":
                return [dict(row) for row in self._fetch_record_rows(mappings)]
            iterable_rows = mappings if hasattr(mappings, "__iter__") else mappings.all()
            return [dict(row) for row in iterable_rows]

    @staticmethod
    def _fetch_record_rows(mappings: Any) -> list[Any]:
        if hasattr(mappings, "fetchmany"):
            return list(mappings.fetchmany(2))

        iterable_rows = mappings if hasattr(mappings, "__iter__") else mappings.all()
        rows = []
        for row in iterable_rows:
            rows.append(row)
            if len(rows) == 2:
                break
        return rows

    @staticmethod
    def _open_session(datasource: DatasourceLike) -> Any:
        session_or_cm = datasource.get_session()
        if hasattr(session_or_cm, "__enter__") and hasattr(session_or_cm, "__exit__"):
            return session_or_cm
        return contextmanager(datasource.get_session)()

    def _project_outputs(self, node: SqlNode, node_name: str, rows: list[dict[str, Any]]) -> tuple[Any, list[str]]:
        if node.result_mode == "record" and len(rows) != 1:
            raise DSLExecutionError(
                "record mode must return exactly one row.",
                code=self._result_mismatch_code(node_name),
            )

        fields = list(node.outputs)
        if not fields and rows:
            fields = list(rows[0].keys())

        if node.result_mode == "record":
            row = rows[0] if rows else {}
            self._ensure_output_columns_exist(row, fields)
            return ({field: row[field] for field in fields} if fields else {}), fields

        if not fields:
            return rows, fields

        projected_rows = []
        for row in rows:
            self._ensure_output_columns_exist(row, fields)
            projected_rows.append({field: row[field] for field in fields})
        return projected_rows, fields

    @staticmethod
    def _ensure_output_columns_exist(row: Mapping[str, Any], fields: list[str]) -> None:
        if not fields:
            return
        missing_fields = [field for field in fields if field not in row]
        if missing_fields:
            raise DSLExecutionError(
                "Declared outputs do not match returned columns: {0}".format(", ".join(missing_fields)),
                code=ExecutionErrorCode.OUTPUT_COLUMN_MISMATCH,
            )

    @staticmethod
    def _result_mismatch_code(node_name: str) -> ExecutionErrorCode:
        if node_name == "context":
            return ExecutionErrorCode.CONTEXT_RESULT_MISMATCH
        return ExecutionErrorCode.STEP_RESULT_MISMATCH
