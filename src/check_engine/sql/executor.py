"""SQL 节点执行器。"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any, Optional

from ..dsl.models import SqlNode, StepNode
from ..exceptions import DSLExecutionError
from ..runtime.state import ExecutionTrace, NodeExecutionResult
from .cte_builder import CteBuilder

class SqlExecutor:
    """执行 context、precheck、step 中的 SQL 节点。"""

    def __init__(self, cte_builder: Optional[CteBuilder] = None) -> None:
        self.cte_builder = cte_builder or CteBuilder()

    def execute_node(
        self,
        node: SqlNode,
        phase: str,
        state: Any,
        datasource_registry: Any,
        node_name: str,
    ) -> tuple[NodeExecutionResult, ExecutionTrace]:
        resolved_params = self._resolve_sql_params(node.sql_params, state)
        consumes = node.consumes if isinstance(node, StepNode) else []
        cte_sql, cte_params = self.cte_builder.build(consumes, state)
        final_sql = self._merge_with_clause(cte_sql, node.sql_template)
        final_params: dict[str, Any] = {}
        final_params.update(cte_params)
        final_params.update(resolved_params)

        start = time.perf_counter()
        try:
            datasource = datasource_registry.get(node.datasource)
            rows = self._run_sql(datasource, final_sql, final_params)
            exported_data, exported_fields = self._project_outputs(node, rows)
        except Exception as exc:  # noqa: BLE001
            elapsed_ms = (time.perf_counter() - start) * 1000
            error = exc if isinstance(exc, DSLExecutionError) else DSLExecutionError(f"SQL node execution failed: {node_name}")
            trace = ExecutionTrace(
                phase=phase,
                node_name=node_name,
                node_kind=node.type,
                datasource=node.datasource,
                sql=final_sql,
                params=final_params,
                result_mode=node.result_mode,
                row_count=0,
                success=False,
                elapsed_ms=elapsed_ms,
                error=str(error),
            )
            raise error from exc

        elapsed_ms = (time.perf_counter() - start) * 1000
        result = NodeExecutionResult(
            node_name=node_name,
            result_mode=node.result_mode,
            raw_rows=rows,
            exported_data=exported_data,
            exported_fields=exported_fields,
            datasource=node.datasource,
            sql=final_sql,
            params=final_params,
            elapsed_ms=elapsed_ms,
        )
        trace = ExecutionTrace(
            phase=phase,
            node_name=node_name,
            node_kind=node.type,
            datasource=node.datasource,
            sql=final_sql,
            params=final_params,
            result_mode=node.result_mode,
            row_count=len(rows),
            success=True,
            elapsed_ms=elapsed_ms,
        )
        return result, trace

    def _resolve_sql_params(self, sql_params: dict[str, Any], state: Any) -> dict[str, Any]:
        resolved = {}
        for key, value in sql_params.items():
            if isinstance(value, str) and value.startswith("$"):
                resolved[key] = state.resolve_reference(value)
            else:
                resolved[key] = value
        return resolved

    def _merge_with_clause(self, cte_sql: str, sql_template: str) -> str:
        if not cte_sql:
            return sql_template
        stripped = sql_template.lstrip()
        if stripped[:4].lower() == "with":
            return cte_sql + ", " + stripped[4:].lstrip()
        return cte_sql + " " + sql_template

    def _run_sql(self, datasource: Any, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if not hasattr(datasource, "get_session"):
            raise DSLExecutionError("Datasource must provide get_session and execute SQL through a SQLAlchemy Session.")

        from sqlalchemy import text as sqlalchemy_text

        session_factory = contextmanager(datasource.get_session)
        with session_factory() as session:
            result = session.execute(sqlalchemy_text(sql), params)
        keys = list(result.keys())
        return [dict(zip(keys, row)) for row in result.fetchall()]

    def _project_outputs(self, node: SqlNode, rows: list[dict[str, Any]]) -> tuple[Any, list[str]]:
        if node.result_mode == "record" and len(rows) > 1:
            raise DSLExecutionError("record mode returned multiple rows.")

        fields = list(node.outputs)
        if not fields and rows:
            fields = list(rows[0].keys())

        if node.result_mode == "record":
            row = rows[0] if rows else {}
            if not fields:
                return {}, []
            return {field: row.get(field) for field in fields}, fields

        projected = []
        for row in rows:
            if fields:
                projected.append({field: row.get(field) for field in fields})
            else:
                projected.append(dict(row))
        return projected, fields
