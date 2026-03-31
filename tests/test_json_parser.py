"""JSON 解析器测试。"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import cast
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine.exceptions import DSLParseError
from check_engine.dsl import VariableStepNode
from check_engine.parser import JsonDslParser


class JsonDslParserTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = JsonDslParser()
        self.example_path = Path(__file__).resolve().parents[1] / "references" / "example.json"

    def test_parse_example_json(self) -> None:
        document = self.parser.parse(self.example_path.read_text(encoding="utf-8"))
        self.assertEqual(document.variables["threshold"].default, 500)
        self.assertEqual(len(document.steps), 5)
        self.assertEqual(document.steps[3].consumes[0].alias, "am")
        self.assertEqual(document.steps[4].type, "variable")
        self.assertEqual(document.on_fail.mode, "single")

    def test_parse_invalid_json_raises(self) -> None:
        with self.assertRaises(DSLParseError):
            self.parser.parse("{invalid json}")

    def test_parse_missing_required_top_level_block_raises(self) -> None:
        with self.assertRaises(DSLParseError):
            self.parser.parse('{"steps": []}')

    def test_parse_optional_top_level_blocks(self) -> None:
        document = self.parser.parse(
            '{"steps": [{"name": "s1", "type": "sql", "datasource": "db", "result_mode": "record", "sql_template": "select 1", "sql_params": {}, "outputs": ["v"]}], "on_fail": {"decision": "false", "mode": "single", "message_cn": "x", "message_en": "y"}}'
        )

        self.assertEqual(dict(document.variables), {})
        
    def test_parse_constant_variable_with_empty_when(self) -> None:
        document = self.parser.parse(
            '{"variables": {"threshold": {"when": [], "default": 888}}, "steps": [{"name": "s1", "type": "sql", "datasource": "db", "result_mode": "record", "sql_template": "select 1 as v", "sql_params": {}, "outputs": ["v"]}], "on_fail": {"decision": "$variables.threshold > 100", "mode": "single", "message_cn": "x", "message_en": "y"}}'
        )

        self.assertEqual(document.variables["threshold"].when, ())
        self.assertEqual(document.variables["threshold"].default, 888)

    def test_parse_step_on_pass(self) -> None:
        document = self.parser.parse(
            '{"steps": [{"name": "p1", "type": "sql", "datasource": "db", "result_mode": "records", "sql_template": "select 1 as v", "sql_params": {}, "outputs": ["v"], "on_pass": {"decision": "not exists($.v)"}}], "on_fail": {"decision": "false", "mode": "single", "message_cn": "x", "message_en": "y"}}'
        )

        if document.steps[0].on_pass is None:
            self.fail("step on_pass should be parsed")
        self.assertEqual(document.steps[0].on_pass.decision, "not exists($.v)")
        self.assertIsNone(document.steps[0].on_fail)

    def test_parse_variable_step(self) -> None:
        document = self.parser.parse(
            '{"steps": [{"name": "threshold_step", "type": "variable", "when": [{"condition": "$variables.base > 10", "value": 100}], "default": 50}], "variables": {"base": {"when": [], "default": 20}}, "on_fail": {"decision": "$steps.threshold_step > 90", "mode": "single", "message_cn": "x", "message_en": "y"}}'
        )

        self.assertIsInstance(document.steps[0], VariableStepNode)
        variable_step = cast(VariableStepNode, document.steps[0])
        self.assertEqual(variable_step.type, "variable")
        self.assertEqual(variable_step.default, 50)



if __name__ == "__main__":
    unittest.main()
