"""SQL 执行器测试。"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine.dsl.models import SqlNode
from check_engine.exceptions import DSLExecutionError, ExecutionErrorCode
from check_engine.sql.executor import SqlExecutor


class _FakeMappingsResult:
    def __init__(self, rows):
        self._rows = rows
        self.fetchmany_calls = []

    def all(self):
        return self._rows

    def fetchmany(self, size):
        self.fetchmany_calls.append(size)
        return self._rows[:size]


class _FakeExecuteResult:
    def __init__(self, rows):
        self._rows = rows
        self.mappings_result = _FakeMappingsResult(self._rows)

    def mappings(self):
        return self.mappings_result


class _FakeSession:
    def __init__(self, rows):
        self._rows = rows
        self.last_result = None

    def execute(self, _sql, _params):
        self.last_result = _FakeExecuteResult(self._rows)
        return self.last_result


class _FakeDatasource:
    def __init__(self, rows):
        self._rows = rows
        self.last_session = None

    def get_session(self):
        session = _FakeSession(self._rows)
        self.last_session = session
        try:
            yield session
        finally:
            pass


class _MissingSessionDatasource:
    pass


class _StaticRegistry:
    def __init__(self, mapping):
        self._mapping = mapping

    def get(self, name):
        return self._mapping[name]


class _FailingSqlExecutor(SqlExecutor):
    def _run_sql(self, datasource, sql, params, result_mode="records"):
        raise RuntimeError("boom")


@unittest.skipUnless(importlib.util.find_spec("sqlalchemy") is not None, "需要安装 sqlalchemy")
class SqlExecutorTestCase(unittest.TestCase):
    def test_run_sql_uses_mappings_all(self) -> None:
        executor = SqlExecutor()
        datasource = _FakeDatasource([
            {"amount": 10, "code": "A"},
            {"amount": 20, "code": "B"},
        ])

        rows = executor._run_sql(datasource, "SELECT 1", {})

        self.assertEqual(rows, [{"amount": 10, "code": "A"}, {"amount": 20, "code": "B"}])

    def test_run_sql_record_mode_only_fetches_two_rows(self) -> None:
        executor = SqlExecutor()
        datasource = _FakeDatasource([
            {"amount": 10},
            {"amount": 20},
            {"amount": 30},
        ])

        rows = executor._run_sql(datasource, "SELECT 1", {}, result_mode="record")

        self.assertEqual(rows, [{"amount": 10}, {"amount": 20}])
        self.assertEqual(datasource.last_session.last_result.mappings_result.fetchmany_calls, [2])


    def test_merge_with_clause_supports_existing_recursive_with_and_comments(self) -> None:
        executor = SqlExecutor()

        merged = executor._merge_with_clause(
            "WITH ctx(amount) AS (VALUES (:v))",
            "/* leading comment */\nWITH RECURSIVE base AS (SELECT 1 AS amount) SELECT amount FROM base",
        )

        self.assertTrue(merged.startswith("/* leading comment */\nWITH RECURSIVE ctx(amount) AS (VALUES (:v)), base AS"))


class SqlExecutorRuntimeErrorTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.executor = SqlExecutor()

    def test_project_outputs_record_mode_requires_exactly_one_row_for_context(self) -> None:
        node = SqlNode(
            type="sql",
            datasource="db",
            result_mode="record",
            sql_template="select 1",
            outputs=["amount"],
        )

        with self.assertRaises(DSLExecutionError) as ctx:
            self.executor._project_outputs(node, "context", [])
        self.assertEqual(ctx.exception.code, ExecutionErrorCode.CONTEXT_RESULT_MISMATCH)

    def test_project_outputs_record_mode_requires_exactly_one_row_for_step(self) -> None:
        node = SqlNode(
            type="sql",
            datasource="db",
            result_mode="record",
            sql_template="select 1",
            outputs=["amount"],
        )

        with self.assertRaises(DSLExecutionError) as ctx:
            self.executor._project_outputs(node, "step_a", [{"amount": 1}, {"amount": 2}])
        self.assertEqual(ctx.exception.code, ExecutionErrorCode.STEP_RESULT_MISMATCH)

    def test_project_outputs_requires_declared_columns_to_exist(self) -> None:
        node = SqlNode(
            type="sql",
            datasource="db",
            result_mode="records",
            sql_template="select 1",
            outputs=["amount", "currency"],
        )

        with self.assertRaises(DSLExecutionError) as ctx:
            self.executor._project_outputs(node, "step_a", [{"amount": 1}])
        self.assertEqual(ctx.exception.code, ExecutionErrorCode.OUTPUT_COLUMN_MISMATCH)

    def test_run_sql_without_datasource_session_raises_error_code(self) -> None:
        with self.assertRaises(DSLExecutionError) as ctx:
            self.executor._run_sql(_MissingSessionDatasource(), "SELECT 1", {})
        self.assertEqual(ctx.exception.code, ExecutionErrorCode.DATASOURCE_NOT_FOUND)

    def test_execute_node_wraps_unknown_sql_errors_with_error_code(self) -> None:
        executor = _FailingSqlExecutor()
        node = SqlNode(
            type="sql",
            datasource="db",
            result_mode="records",
            sql_template="select 1",
            outputs=[],
        )
        registry = _StaticRegistry({"db": object()})

        with self.assertRaises(DSLExecutionError) as ctx:
            executor.execute_node(node, state=object(), datasource_registry=registry, node_name="step_a")
        self.assertEqual(ctx.exception.code, ExecutionErrorCode.SQL_EXECUTION_FAILED)


if __name__ == "__main__":
    unittest.main()
