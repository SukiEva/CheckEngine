"""编译缓存测试。"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, cast
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine import DslEngine, DSLValidationError
from check_engine.dsl import DslDocument
from check_engine.sql import SqlExecutor
from check_engine.parser import JsonDslParser
from check_engine.runtime import NodeExecutionResult
from check_engine.validator import DslValidator


class _CountingParser(JsonDslParser):
    def __init__(self) -> None:
        super().__init__()
        self.parse_count = 0

    def parse(self, dsl_text: str) -> DslDocument:
        self.parse_count += 1
        return super().parse(dsl_text)


class _CountingValidator(DslValidator):
    def __init__(self) -> None:
        super().__init__()
        self.validate_count = 0

    def validate(self, document: DslDocument) -> None:
        self.validate_count += 1
        super().validate(document)


class _PassingSqlExecutor:
    def execute_node(
        self,
        node: Any,
        state: Any,
        datasource_registry: Any,
        node_name: str,
    ) -> NodeExecutionResult:
        del node, state, datasource_registry, node_name
        return NodeExecutionResult(raw_rows=[{"amount": 1}], exported_data={"amount": 1}, exported_fields=["amount"])


class _UnusedRegistry:
    def get(self, name: str) -> Any:
        raise AssertionError(f"unexpected datasource lookup: {name}")


class EngineCompileCacheTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.example_path = Path(__file__).resolve().parents[1] / "references" / "example.json"
        self.dsl_text = self.example_path.read_text(encoding="utf-8")

    def test_validate_only_runs_parse_and_rule_validation(self) -> None:
        parser = _CountingParser()
        validator = _CountingValidator()
        engine = DslEngine(compile_cache_size=2)
        engine.parser = parser
        engine.validator = validator

        first = engine.validate(self.dsl_text)
        second = engine.validate(self.dsl_text)

        self.assertIsNone(first)
        self.assertIsNone(second)
        self.assertEqual(parser.parse_count, 2)
        self.assertEqual(validator.validate_count, 2)

    def test_execute_reuses_cached_compilation_without_rule_validation(self) -> None:
        parser = _CountingParser()
        validator = _CountingValidator()
        dsl_text = json.dumps(
            {
                "steps": [
                    {
                        "name": "step_a",
                        "type": "sql",
                        "datasource": "db",
                        "result_mode": "record",
                        "sql_template": "select 1 as amount",
                        "sql_params": {},
                        "outputs": ["amount"],
                    }
                ],
                "on_fail": {
                    "decision": "False",
                    "mode": "single",
                    "message_cn": "ok",
                    "message_en": "ok",
                },
            }
        )
        engine = DslEngine(
            compile_cache_size=2,
        )
        engine.parser = parser
        engine.validator = validator
        engine.sql_executor = cast(SqlExecutor, _PassingSqlExecutor())
        registry = _UnusedRegistry()

        first = engine.execute(dsl_text, {}, datasource_registry=registry)
        second = engine.execute(dsl_text, {}, datasource_registry=registry)

        self.assertTrue(first.passed)
        self.assertTrue(second.passed)
        self.assertEqual(parser.parse_count, 1)
        self.assertEqual(validator.validate_count, 0)

    def test_execute_compile_cache_can_be_disabled(self) -> None:
        parser = _CountingParser()
        validator = _CountingValidator()
        engine = DslEngine(
            compile_cache_size=0,
        )
        engine.parser = parser
        engine.validator = validator
        engine.sql_executor = cast(SqlExecutor, _PassingSqlExecutor())
        registry = _UnusedRegistry()

        engine.execute(self.dsl_text, {}, datasource_registry=registry)
        engine.execute(self.dsl_text, {}, datasource_registry=registry)

        self.assertEqual(parser.parse_count, 2)
        self.assertEqual(validator.validate_count, 0)

    def test_execute_compile_cache_respects_lru_eviction(self) -> None:
        parser = _CountingParser()
        validator = _CountingValidator()
        engine = DslEngine(
            compile_cache_size=1,
        )
        engine.parser = parser
        engine.validator = validator
        engine.sql_executor = cast(SqlExecutor, _PassingSqlExecutor())
        registry = _UnusedRegistry()
        other = json.loads(self.dsl_text)
        other["on_fail"]["decision"] = "$variables.threshold > 999999"
        other_text = json.dumps(other)

        engine.execute(self.dsl_text, {}, datasource_registry=registry)
        engine.execute(other_text, {}, datasource_registry=registry)
        engine.execute(self.dsl_text, {}, datasource_registry=registry)

        self.assertEqual(parser.parse_count, 3)
        self.assertEqual(validator.validate_count, 0)

    def test_validate_raises_when_on_fail_has_invalid_reference(self) -> None:
        engine = DslEngine(compile_cache_size=1)
        invalid_dsl_text = (
            '{"steps": [], "on_fail": {"decision": "$steps.not_exists.value > 0", '
            '"mode": "single", "message_cn": "x", "message_en": "y"}}'
        )

        with self.assertRaisesRegex(DSLValidationError, "on_fail.decision"):
            engine.validate(invalid_dsl_text)
