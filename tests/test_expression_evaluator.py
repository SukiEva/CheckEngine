"""表达式求值器测试。"""

from __future__ import annotations

import sys
from pathlib import Path
from types import MappingProxyType
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine.expression import ExpressionEvaluator
from check_engine.runtime.state import ExecutionState


class ExpressionEvaluatorTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.evaluator = ExpressionEvaluator()
        self.state = ExecutionState.new({"source_object_id": "HDR001"})
        self.state.context_data = {"flow": "flow1", "scenario": "scenario1"}
        self.state.variables_data = {"threshold": 1000}
        self.state.step_data = {"exchange_rate": {"final_amount": 1200}}

    def test_evaluate_boolean_expression(self) -> None:
        expression = "$context.flow == 'flow1' and $context.scenario in ('scenario1', 'scenario2')"
        self.assertTrue(self.evaluator.evaluate(expression, self.state))

    def test_evaluate_with_null(self) -> None:
        expression = "$input.source_object_id != null and $variables.threshold >= 1000"
        self.assertTrue(self.evaluator.evaluate(expression, self.state))

    def test_evaluate_exists_function(self) -> None:
        expression = "exists($steps.exchange_rate.final_amount)"
        self.assertTrue(self.evaluator.evaluate(expression, self.state))

    def test_evaluate_exists_function_on_step_records_field(self) -> None:
        self.state.step_data["duplicate_lines"] = [{"duplicate_entry_lines": 10}, {"duplicate_entry_lines": 20}]
        expression = "exists($steps.duplicate_lines.duplicate_entry_lines)"
        self.assertTrue(self.evaluator.evaluate(expression, self.state))

    def test_evaluate_exists_function_on_empty_list(self) -> None:
        self.state.step_data["empty_step"] = []
        expression = "exists($steps.empty_step)"
        self.assertFalse(self.evaluator.evaluate(expression, self.state))

    def test_evaluate_exists_function_on_empty_mapping_proxy(self) -> None:
        self.state.context_data = MappingProxyType({})

        expression = "exists($context)"

        self.assertFalse(self.evaluator.evaluate(expression, self.state))

    def test_evaluate_final_failure_expression(self) -> None:
        expression = "$steps.exchange_rate.final_amount > $variables.threshold"
        self.assertTrue(self.evaluator.evaluate(expression, self.state))

    def test_evaluate_compiled_expression(self) -> None:
        compiled = self.evaluator.compile("$steps.exchange_rate.final_amount > $variables.threshold")

        self.assertTrue(self.evaluator.evaluate_compiled(compiled, self.state))

    def test_compile_deduplicates_repeated_references(self) -> None:
        compiled = self.evaluator.compile(
            "$steps.exchange_rate.final_amount > 0 and $steps.exchange_rate.final_amount > $variables.threshold"
        )

        self.assertEqual(
            compiled.references,
            ("$steps.exchange_rate.final_amount", "$variables.threshold"),
        )


if __name__ == "__main__":
    unittest.main()
