"""主引擎运行时失败结果测试。"""

from __future__ import annotations

import json
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine.engine import DslEngine
from check_engine.exceptions import DSLExecutionError, ExecutionErrorCode
from check_engine.runtime.state import NodeExecutionResult


class _FailingSqlExecutor:
    def execute_node(self, node, state, datasource_registry, node_name):
        raise DSLExecutionError("SQL node execution failed: step_a", code=ExecutionErrorCode.SQL_EXECUTION_FAILED)


class _PassingSqlExecutor:
    def execute_node(self, node, state, datasource_registry, node_name):
        return NodeExecutionResult(
            raw_rows=[{"v": 1}],
            exported_data={"v": 1},
            exported_fields=["v"],
        )


class _FailingMessageRenderer:
    def render(self, policy, state, rows=None):
        raise DSLExecutionError("Message render failed", code=ExecutionErrorCode.TEMPLATE_RENDER_FAILED)


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

    def test_execute_returns_runtime_failure_result_with_error_code(self) -> None:
        engine = DslEngine(sql_executor=_FailingSqlExecutor())

        result = engine.execute(self.dsl_text, {}, datasource_registry=object())

        self.assertFalse(result.passed)
        self.assertEqual(result.phase, "runtime")
        self.assertEqual(result.failed_node, "step_a")
        self.assertEqual(result.error_code, ExecutionErrorCode.SQL_EXECUTION_FAILED.value)
        self.assertEqual(result.error_detail, "SQL node execution failed: step_a")
        self.assertEqual(result.message_cn, "SQL node execution failed: step_a")
        self.assertEqual(result.message_en, "SQL node execution failed: step_a")

    def test_execute_returns_runtime_failure_result_with_on_fail_node(self) -> None:
        engine = DslEngine(
            sql_executor=_PassingSqlExecutor(),
            message_renderer=_FailingMessageRenderer(),
        )
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

        result = engine.execute(dsl_text, {}, datasource_registry=object())

        self.assertFalse(result.passed)
        self.assertEqual(result.phase, "runtime")
        self.assertEqual(result.failed_node, "on_fail")
        self.assertEqual(result.error_code, ExecutionErrorCode.TEMPLATE_RENDER_FAILED.value)

    def test_execute_pass_result_has_empty_runtime_error_fields(self) -> None:
        engine = DslEngine(sql_executor=_PassingSqlExecutor())

        result = engine.execute(self.dsl_text, {}, datasource_registry=object())

        self.assertTrue(result.passed)
        self.assertEqual(result.phase, "pass")
        self.assertIsNone(result.error_code)
        self.assertIsNone(result.error_detail)


if __name__ == "__main__":
    unittest.main()
