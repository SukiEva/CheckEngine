"""编译缓存测试。"""

from __future__ import annotations

import json
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine import DslEngine, DSLValidationError
from check_engine.parser import JsonDslParser
from check_engine.validator import DslValidator


class _CountingParser(JsonDslParser):
    def __init__(self) -> None:
        super().__init__()
        self.parse_count = 0

    def parse(self, dsl_text: str):
        self.parse_count += 1
        return super().parse(dsl_text)


class _CountingValidator(DslValidator):
    def __init__(self) -> None:
        super().__init__()
        self.validate_count = 0

    def validate(self, document) -> None:
        self.validate_count += 1
        super().validate(document)


class EngineCompileCacheTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.example_path = Path(__file__).resolve().parents[1] / "references" / "example.json"
        self.dsl_text = self.example_path.read_text(encoding="utf-8")

    def test_compile_reuses_cached_parse_and_validate_result(self) -> None:
        parser = _CountingParser()
        validator = _CountingValidator()
        engine = DslEngine(parser=parser, validator=validator, compile_cache_size=2)

        first = engine.compile(self.dsl_text)
        second = engine.compile(self.dsl_text)

        self.assertEqual(parser.parse_count, 1)
        self.assertEqual(validator.validate_count, 1)
        self.assertIsNot(first, second)
        self.assertIsNot(first.document, second.document)
        self.assertEqual(engine.compile_cache_info().hits, 1)

    def test_compile_cache_can_be_disabled(self) -> None:
        parser = _CountingParser()
        validator = _CountingValidator()
        engine = DslEngine(parser=parser, validator=validator, compile_cache_size=0)

        engine.compile(self.dsl_text)
        engine.compile(self.dsl_text)

        self.assertEqual(parser.parse_count, 2)
        self.assertEqual(validator.validate_count, 2)
        self.assertIsNone(engine.compile_cache_info())

    def test_compile_cache_respects_lru_eviction(self) -> None:
        parser = _CountingParser()
        validator = _CountingValidator()
        engine = DslEngine(parser=parser, validator=validator, compile_cache_size=1)
        other = json.loads(self.dsl_text)
        other["on_fail"]["decision"] = "$variables.threshold > 999999"
        other_text = json.dumps(other)

        engine.compile(self.dsl_text)
        engine.compile(other_text)
        engine.compile(self.dsl_text)

        self.assertEqual(parser.parse_count, 3)
        self.assertEqual(validator.validate_count, 3)

    def test_execute_document_validates_before_execution(self) -> None:
        engine = DslEngine(compile_cache_size=1)
        invalid_document = JsonDslParser().parse(
            '{"steps": [{"name": "s1", "type": "sql", "datasource": "db", "result_mode": "record", "sql_template": "select 1 as value", "sql_params": {}, "outputs": ["value"]}], "on_fail": {"decision": "exists", "mode": "single", "message_cn": "x", "message_en": "y"}}'
        )

        with self.assertRaisesRegex(DSLValidationError, "on_fail.decision"):
            engine.execute_document(invalid_document, {"source_object_id": "x"}, datasource_registry=None)
