"""主引擎运行时失败结果测试。"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from types import MappingProxyType
from typing import Any, Optional, cast
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine.engine import DslEngine
from check_engine.sql import DatasourceRegistry, SqlExecutor
from check_engine.renderer import MessageRenderer
from check_engine.exceptions import DSLExecutionError, DSLValidationError
from check_engine.runtime.state import ExecutionResult, NodeExecutionResult


class _FailingSqlExecutor:
    def execute_node(self, node: Any, state: Any, datasource_registry: Any, node_name: str) -> NodeExecutionResult:
        del node, state, datasource_registry, node_name
        raise DSLExecutionError("SQL node execution failed: step_a")


class _PassingSqlExecutor:
    def execute_node(self, node: Any, state: Any, datasource_registry: Any, node_name: str) -> NodeExecutionResult:
        del state, datasource_registry, node_name
        return NodeExecutionResult(
            raw_rows=[{"v": 1}],
            exported_data={"v": 1},
            exported_fields=["v"],
            executed_sql=node.sql_template,
        )


class _FailingMessageRenderer:
    def render(self, policy: Any, state: Any, rows: Optional[Any] = None) -> tuple[str, str]:
        raise DSLExecutionError("Message render failed")


class _UnusedRegistry(DatasourceRegistry):
    def get(self, name: str) -> Any:
        raise AssertionError(f"unexpected datasource lookup: {name}")


class EngineRuntimeResultTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.dsl_text = json.dumps(
            {
                "steps": [
                    {
                        "name": "step_a",
                        "type": "sql",
                        "datasource": "db",
                        "result_mode": "record",
                        "sql_template": "select 1 as v",
                        "sql_params": {},
                        "outputs": ["v"],
                    }
                ],
                "on_fail": {
                    "decision": "$steps.step_a.v > 10",
                    "mode": "single",
                    "message_cn": "ok",
                    "message_en": "ok",
                },
            }
        )

    def test_execute_returns_runtime_failure_result_with_message(self) -> None:
        logger_mock = Mock(spec=logging.Logger)
        engine = DslEngine(logger=cast(logging.Logger, logger_mock))
        engine.sql_executor = cast(SqlExecutor, _FailingSqlExecutor())
        registry = cast(DatasourceRegistry, _UnusedRegistry())

        result = engine.execute(self.dsl_text, {}, datasource_registry=registry)

        self.assertFalse(result.passed)
        self.assertEqual(result.phase, "runtime")
        self.assertEqual(result.failed_node, "step_a")
        self.assertIsNone(result.message_cn)
        self.assertIsNone(result.message_en)
        self.assertEqual(result.error_message, "SQL node execution failed: step_a")
        self.assertTrue(result.runtime_exception)
        logger_mock.error.assert_called_once()

    def test_execute_returns_runtime_failure_result_with_on_fail_node(self) -> None:
        engine = DslEngine()
        engine.sql_executor = cast(SqlExecutor, _PassingSqlExecutor())
        engine.message_renderer = cast(MessageRenderer, _FailingMessageRenderer())
        registry = cast(DatasourceRegistry, _UnusedRegistry())
        dsl_text = json.dumps(
            {
                "steps": [
                    {
                        "name": "step_a",
                        "type": "sql",
                        "datasource": "db",
                        "result_mode": "record",
                        "sql_template": "select 1 as v",
                        "sql_params": {},
                        "outputs": ["v"],
                    }
                ],
                "on_fail": {
                    "decision": "$steps.step_a.v == 1",
                    "mode": "single",
                    "message_cn": "ok",
                    "message_en": "ok",
                },
            }
        )

        result = engine.execute(dsl_text, {}, datasource_registry=registry)

        self.assertFalse(result.passed)
        self.assertEqual(result.phase, "runtime")
        self.assertEqual(result.failed_node, "on_fail")
        self.assertIsNone(result.message_cn)
        self.assertIsNone(result.message_en)
        self.assertEqual(result.error_message, "Message render failed")
        self.assertTrue(result.runtime_exception)

    def test_set_context_result_accepts_mapping_view(self) -> None:
        from check_engine.runtime.state import ExecutionState

        state = ExecutionState.new({})
        result = NodeExecutionResult(
            raw_rows=[{"v": 1}],
            exported_data=MappingProxyType({"v": 1}),
            exported_fields=["v"],
        )

        state.set_context_result(result)

        self.assertEqual(state.resolve_reference("$context.v"), 1)

    def test_set_context_result_reuses_resolver_after_context_update(self) -> None:
        from check_engine.runtime.state import ExecutionState

        state = ExecutionState.new({})
        first = NodeExecutionResult(
            raw_rows=[{"v": 1}],
            exported_data={"v": 1},
            exported_fields=["v"],
        )
        second = NodeExecutionResult(
            raw_rows=[{"v": 2}],
            exported_data={"v": 2},
            exported_fields=["v"],
        )

        state.set_context_result(first)
        self.assertEqual(state.resolve_reference("$context.v"), 1)

        state.set_context_result(second)
        self.assertEqual(state.resolve_reference("$context.v"), 2)

    def test_execute_pass_result_has_empty_runtime_error_fields(self) -> None:
        engine = DslEngine()
        engine.sql_executor = cast(SqlExecutor, _PassingSqlExecutor())
        registry = cast(DatasourceRegistry, _UnusedRegistry())

        result = engine.execute(self.dsl_text, {}, datasource_registry=registry)

        self.assertTrue(result.passed)
        self.assertEqual(result.phase, "pass")
        self.assertIsNone(result.error_message)
        self.assertFalse(result.runtime_exception)

    def test_execute_multiple_times_without_state_leak(self) -> None:
        class _InputDrivenSqlExecutor:
            def execute_node(
                self,
                node: Any,
                state: Any,
                datasource_registry: Any,
                node_name: str,
            ) -> NodeExecutionResult:
                del node, datasource_registry, node_name
                value = state.input_data["amount"]
                return NodeExecutionResult(
                    raw_rows=[{"v": value}],
                    exported_data={"v": value},
                    exported_fields=["v"],
                )

        engine = DslEngine(compile_cache_size=2)
        engine.sql_executor = cast(SqlExecutor, _InputDrivenSqlExecutor())
        registry = cast(DatasourceRegistry, _UnusedRegistry())

        first = engine.execute(self.dsl_text, {"amount": 5}, datasource_registry=registry)
        second = engine.execute(self.dsl_text, {"amount": 20}, datasource_registry=registry)

        self.assertTrue(first.passed)
        self.assertFalse(second.passed)
        self.assertEqual(first.steps["step_a"]["v"], 5)
        self.assertEqual(second.steps["step_a"]["v"], 20)

    def test_execute_logs_exception_stack_when_expression_invalid(self) -> None:
        logger_mock = Mock(spec=logging.Logger)
        engine = DslEngine(logger=cast(logging.Logger, logger_mock))
        registry = cast(DatasourceRegistry, _UnusedRegistry())
        invalid_dsl_text = json.dumps(
            {
                "steps": [
                    {
                        "name": "step_a",
                        "type": "sql",
                        "datasource": "db",
                        "result_mode": "record",
                        "sql_template": "select 1 as v",
                        "sql_params": {},
                        "outputs": ["v"],
                    }
                ],
                "on_fail": {
                    "decision": "$steps.step_a.v >",
                    "mode": "single",
                    "message_cn": "ok",
                    "message_en": "ok",
                },
            }
        )

        with self.assertRaisesRegex(DSLValidationError, "on_fail.decision"):
            engine.execute(invalid_dsl_text, {}, datasource_registry=registry)

        logger_mock.error.assert_called_once()
        logged_args = logger_mock.error.call_args[0]
        self.assertEqual(logged_args[0], "DslEngine %s failed: %s\n%s")
        self.assertEqual(logged_args[1], "compile")

    def test_execution_result_to_dict_normalizes_mapping_views(self) -> None:
        result = ExecutionResult(
            passed=False,
            phase="final",
            failed_node="on_fail",
            message_cn="x",
            message_en="y",
            error_message=None,
            runtime_exception=False,
            context=MappingProxyType({"flow": "f1"}),
            variables=MappingProxyType({"threshold": 1000}),
            steps=MappingProxyType({"step_a": MappingProxyType({"values": (1, 2)})}),
        )

        payload = result.to_dict()

        self.assertEqual(
            payload,
            {
                "passed": False,
                "phase": "final",
                "failed_node": "on_fail",
                "message_cn": "x",
                "message_en": "y",
                "error_message": None,
                "runtime_exception": False,
                "context": {"flow": "f1"},
                "variables": {"threshold": 1000},
                "steps": {"step_a": {"values": [1, 2]}},
                "executed_nodes": [],
            },
        )

    def test_execution_result_to_dict_output_is_json_serializable(self) -> None:
        result = ExecutionResult(
            passed=True,
            phase="pass",
            failed_node=None,
            message_cn=None,
            message_en=None,
            error_message=None,
            runtime_exception=False,
            context=MappingProxyType({"flow": "f1"}),
            variables=MappingProxyType({"threshold": 1000}),
            steps=MappingProxyType({"step_a": MappingProxyType({"values": (1, 2)})}),
        )

        payload = result.to_dict()

        self.assertEqual(
            json.loads(json.dumps(payload)),
            {
                "passed": True,
                "phase": "pass",
                "failed_node": None,
                "message_cn": None,
                "message_en": None,
                "error_message": None,
                "runtime_exception": False,
                "context": {"flow": "f1"},
                "variables": {"threshold": 1000},
                "steps": {"step_a": {"values": [1, 2]}},
                "executed_nodes": [],
            },
        )

    def test_node_execution_result_rows_are_read_only_views(self) -> None:
        result = NodeExecutionResult(
            raw_rows=[{"v": 1}],
            exported_data={"v": 1},
            exported_fields=["v"],
        )

        rows = result.as_rows()

        self.assertIsInstance(rows, tuple)
        with self.assertRaises(TypeError):
            cast(Any, rows[0])["v"] = 2



    def test_execute_result_records_executed_nodes_trace(self) -> None:
        engine = DslEngine()
        engine.sql_executor = cast(SqlExecutor, _PassingSqlExecutor())
        registry = cast(DatasourceRegistry, _UnusedRegistry())

        result = engine.execute(self.dsl_text, {}, datasource_registry=registry)

        self.assertEqual(
            [
                {
                    "phase": item.phase,
                    "node_name": item.node_name,
                    "datasource": item.datasource,
                    "result_mode": item.result_mode,
                    "row_count": item.row_count,
                    "executed_sql": item.executed_sql,
                }
                for item in result.executed_nodes
            ],
            [
                {
                    "phase": "step",
                    "node_name": "step_a",
                    "datasource": "db",
                    "result_mode": "record",
                    "row_count": 1,
                    "executed_sql": "select 1 as v",
                }
            ],
        )

class ExecutionStateReferenceResolutionTestCase(unittest.TestCase):
    def test_resolve_reference_when_step_data_mapping_reassigned(self) -> None:
        from check_engine.runtime.state import ExecutionState

        state = ExecutionState.new({})
        state.step_data = {"step_a": {"value": 10}}

        self.assertEqual(state.resolve_reference("$steps.step_a.value"), 10)

    def test_resolve_step_path_from_read_only_sequence(self) -> None:
        from check_engine.runtime.state import ExecutionState

        state = ExecutionState.new({})
        cast(Any, state.step_data)["step_a"] = (
            MappingProxyType({"code": "A"}),
            MappingProxyType({"code": "B"}),
        )

        self.assertEqual(state.resolve_reference("$steps.step_a.code"), ["A", "B"])


if __name__ == "__main__":
    unittest.main()
