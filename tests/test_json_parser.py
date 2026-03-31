"""JSON 解析器测试。"""

from __future__ import annotations

import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine.exceptions import DSLParseError
from check_engine.parser import JsonDslParser


class JsonDslParserTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = JsonDslParser()
        self.example_path = Path(__file__).resolve().parents[1] / "references" / "example.json"

    def test_parse_example_json(self) -> None:
        document = self.parser.parse(self.example_path.read_text(encoding="utf-8"))
        if document.context is None:
            self.fail("example JSON should define context block")

        self.assertEqual(document.context.datasource, "saas_db")
        self.assertEqual(document.context.outputs, ("flow", "scenario"))
        self.assertEqual(document.variables["threshold"].default, 500)
        self.assertEqual(len(document.prechecks), 2)
        self.assertEqual(document.steps[1].consumes[0].alias, "am")
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

        self.assertIsNone(document.context)
        self.assertEqual(dict(document.variables), {})
        self.assertEqual(document.prechecks, ())

    def test_parse_constant_variable_with_empty_when(self) -> None:
        document = self.parser.parse(
            '{"variables": {"threshold": {"when": [], "default": 888}}, "steps": [{"name": "s1", "type": "sql", "datasource": "db", "result_mode": "record", "sql_template": "select 1 as v", "sql_params": {}, "outputs": ["v"]}], "on_fail": {"decision": "$variables.threshold > 100", "mode": "single", "message_cn": "x", "message_en": "y"}}'
        )

        self.assertEqual(document.variables["threshold"].when, ())
        self.assertEqual(document.variables["threshold"].default, 888)

    def test_parse_precheck_on_pass(self) -> None:
        document = self.parser.parse(
            '{"prechecks": [{"name": "p1", "type": "sql", "datasource": "db", "result_mode": "records", "sql_template": "select 1 as v", "sql_params": {}, "outputs": ["v"], "on_pass": {"decision": "not exists($prechecks.p1.v)"}}], "steps": [{"name": "s1", "type": "sql", "datasource": "db", "result_mode": "record", "sql_template": "select 1 as v", "sql_params": {}, "outputs": ["v"]}], "on_fail": {"decision": "false", "mode": "single", "message_cn": "x", "message_en": "y"}}'
        )

        if document.prechecks[0].on_pass is None:
            self.fail("precheck on_pass should be parsed")
        self.assertEqual(document.prechecks[0].on_pass.decision, "not exists($prechecks.p1.v)")
        self.assertIsNone(document.prechecks[0].on_fail)



if __name__ == "__main__":
    unittest.main()
