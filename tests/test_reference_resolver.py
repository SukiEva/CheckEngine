"""运行时引用解析策略测试。"""

from __future__ import annotations

import sys
from pathlib import Path
from types import MappingProxyType
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine.exceptions import DSLExecutionError
from check_engine.runtime.reference_resolver import RuntimeReferenceResolver


class RuntimeReferenceResolverTestCase(unittest.TestCase):
    def test_resolve_mapping_scope_value(self) -> None:
        resolver = RuntimeReferenceResolver(
            input_data={"amount": 100},
            context_data={"rate": 6.8},
            variables_data={"flag": True},
            prechecks_data={},
            step_data={"step_a": {"result": 10}},
        )

        self.assertEqual(resolver.resolve_reference("$input.amount"), 100)
        self.assertEqual(resolver.resolve_reference("$context.rate"), 6.8)
        self.assertTrue(resolver.resolve_reference("$variables.flag"))

    def test_resolve_steps_sequence_projection(self) -> None:
        resolver = RuntimeReferenceResolver(
            input_data={},
            context_data={},
            variables_data={},
            prechecks_data={},
            step_data={
                "step_a": (
                    MappingProxyType({"code": "A"}),
                    MappingProxyType({"code": "B"}),
                )
            },
        )

        self.assertEqual(resolver.resolve_reference("$steps.step_a.code"), ["A", "B"])

    def test_resolve_unknown_scope_raises_error(self) -> None:
        resolver = RuntimeReferenceResolver(
            input_data={},
            context_data={},
            variables_data={},
            prechecks_data={},
            step_data={},
        )

        with self.assertRaisesRegex(DSLExecutionError, "Unknown scope"):
            resolver.resolve_reference("$unknown.value")

    def test_resolve_prechecks_sequence_projection(self) -> None:
        resolver = RuntimeReferenceResolver(
            input_data={},
            context_data={},
            variables_data={},
            prechecks_data={
                "check_a": (
                    MappingProxyType({"line_no": 1}),
                    MappingProxyType({"line_no": 2}),
                )
            },
            step_data={},
        )

        self.assertEqual(resolver.resolve_reference("$prechecks.check_a.line_no"), [1, 2])


if __name__ == "__main__":
    unittest.main()
