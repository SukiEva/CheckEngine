"""SQL 节点执行器。"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from typing import Any, Optional

from ..dsl.models import SqlNode, StepNode
from ..exceptions import DSLExecutionError, ExecutionErrorCode
from ..runtime.state import NodeExecutionResult
from .cte_builder import CteBuilder


class SqlExecutor:
    """执行 context、precheck、step 中的 SQL 节点。"""

    def __init__(self, cte_builder: Optional[CteBuilder] = None) -> None:
        self.cte_builder = cte_builder or CteBuilder()

    def execute_node(
        self,
        node: SqlNode,
        state: Any,
        datasource_registry: Any,
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

    def _resolve_sql_params(self, sql_params: Mapping[str, Any], state: Any) -> dict[str, Any]:
        resolved: dict[str, Any] = {}
        for key, value in sql_params.items():
            resolved[key] = state.resolve_reference(value) if isinstance(value, str) and value.startswith("$") else value
        return resolved

    def _merge_with_clause(self, cte_sql: str, sql_template: str) -> str:
        if not cte_sql:
            return sql_template
        stripped = sql_template.lstrip()
        if stripped[:4].lower() == "with":
            return cte_sql + ", " + stripped[4:].lstrip()
        return cte_sql + " " + sql_template

    def _run_sql(self, datasource: Any, sql: str, params: dict[str, Any], result_mode: str = "records") -> list[dict[str, Any]]:
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

    def _fetch_record_rows(self, mappings: Any) -> list[Any]:
        if hasattr(mappings, "fetchmany"):
            return list(mappings.fetchmany(2))

        iterable_rows = mappings if hasattr(mappings, "__iter__") else mappings.all()
        rows = []
        for row in iterable_rows:
            rows.append(row)
            if len(rows) == 2:
                break
        return rows

    def _open_session(self, datasource: Any) -> Any:
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

    def _ensure_output_columns_exist(self, row: dict[str, Any], fields: list[str]) -> None:
        if not fields:
            return
        missing_fields = [field for field in fields if field not in row]
        if missing_fields:
            raise DSLExecutionError(
                "Declared outputs do not match returned columns: {0}".format(", ".join(missing_fields)),
                code=ExecutionErrorCode.OUTPUT_COLUMN_MISMATCH,
            )

    def _result_mismatch_code(self, node_name: str) -> ExecutionErrorCode:
        if node_name == "context":
            return ExecutionErrorCode.CONTEXT_RESULT_MISMATCH
        return ExecutionErrorCode.STEP_RESULT_MISMATCH
