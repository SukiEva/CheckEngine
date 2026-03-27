"""编译缓存测试。"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine import DslEngine, DSLValidationError
from check_engine.dsl import DslDocument
from check_engine.parser import JsonDslParser
from check_engine.sql import DatasourceRegistry
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


class _UnusedRegistry:
    def get(self, name: str) -> Any:
        raise AssertionError(f"unexpected datasource lookup: {name}")


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
        self.assertIs(first, second)
        self.assertIs(first.document, second.document)
        cache_info = engine.compile_cache_info()
        if cache_info is None:
            self.fail("compile cache info should not be None when cache is enabled")
        self.assertEqual(cache_info.hits, 1)

    def test_compile_cache_key_does_not_store_raw_dsl_text(self) -> None:
        engine = DslEngine(compile_cache_size=2)

        engine.compile(self.dsl_text)
        cache_backend = getattr(engine, "_compile_cache_backend")
        cache_keys = cache_backend.debug_keys()
        if len(cache_keys) != 1:
            self.fail("compile cache should contain exactly one key")
        cache_key = cache_keys[0]

        self.assertIsInstance(cache_key, str)
        self.assertNotEqual(cache_key, self.dsl_text)
        self.assertLess(len(cache_key), len(self.dsl_text))

    def test_compile_returns_immutable_cached_document_content(self) -> None:
        engine = DslEngine(compile_cache_size=2)

        compiled = engine.compile(self.dsl_text)

        self.assertIsInstance(compiled.document.raw, MappingProxyType)
        self.assertIsInstance(compiled.document.steps, tuple)
        self.assertIsInstance(compiled.document.variables, MappingProxyType)
        self.assertIsInstance(compiled.document.steps[0].outputs, tuple)
        self.assertIsInstance(compiled.document.steps[0].sql_params, MappingProxyType)
        with self.assertRaises(AttributeError):
            cast(Any, compiled.document.steps[0].outputs).append("x")
        with self.assertRaises(TypeError):
            cast(Any, compiled.document.variables)["threshold"] = object()


    def test_compile_freezes_variable_default_nested_value(self) -> None:
        engine = DslEngine(compile_cache_size=2)
        dsl_text = json.dumps(
            {
                "variables": {
                    "thresholds": {
                        "when": [],
                        "default": {"levels": [100, 200], "flags": {"strict": True}},
                    }
                },
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
                    "decision": "false",
                    "mode": "single",
                    "message_cn": "ok",
                    "message_en": "ok",
                },
            }
        )

        compiled = engine.compile(dsl_text)
        default_value = compiled.document.variables["thresholds"].default

        self.assertIsInstance(default_value, MappingProxyType)
        self.assertIsInstance(default_value["levels"], tuple)
        self.assertIsInstance(default_value["flags"], MappingProxyType)
        with self.assertRaises(TypeError):
            default_value["flags"]["strict"] = False

    def test_compile_freezes_variable_condition_nested_value(self) -> None:
        engine = DslEngine(compile_cache_size=2)
        dsl_text = json.dumps(
            {
                "variables": {
                    "thresholds": {
                        "when": [
                            {
                                "condition": "$input.kind == 'special'",
                                "value": {"levels": [100, 200], "meta": {"currency": "CNY"}},
                            }
                        ],
                        "default": 0,
                    }
                },
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
                    "decision": "false",
                    "mode": "single",
                    "message_cn": "ok",
                    "message_en": "ok",
                },
            }
        )

        compiled = engine.compile(dsl_text)
        value = compiled.document.variables["thresholds"].when[0].value

        self.assertIsInstance(compiled.document.variables["thresholds"].when, tuple)
        self.assertIsInstance(value, MappingProxyType)
        self.assertIsInstance(value["levels"], tuple)
        self.assertIsInstance(value["meta"], MappingProxyType)
        with self.assertRaises(TypeError):
            value["meta"]["currency"] = "USD"

    def test_compile_freezes_step_consumes_sequence(self) -> None:
        engine = DslEngine(compile_cache_size=2)
        dsl_text = json.dumps(
            {
                "context": {
                    "type": "sql",
                    "datasource": "db",
                    "result_mode": "records",
                    "sql_template": "select 1 as amount",
                    "sql_params": {},
                    "outputs": ["amount"],
                },
                "steps": [
                    {
                        "name": "step_a",
                        "type": "sql",
                        "datasource": "db",
                        "result_mode": "records",
                        "sql_template": "select amount from ctx",
                        "sql_params": {},
                        "outputs": ["amount"],
                        "consumes": [{"from": "$context", "alias": "ctx"}],
                    }
                ],
                "on_fail": {
                    "decision": "false",
                    "mode": "single",
                    "message_cn": "ok",
                    "message_en": "ok",
                },
            }
        )

        compiled = engine.compile(dsl_text)
        consumes = compiled.document.steps[0].consumes

        self.assertIsInstance(consumes, tuple)
        self.assertEqual(consumes[0].from_path, "$context")
        with self.assertRaises(AttributeError):
            cast(Any, consumes).append(object())

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

    def test_execute_document_skips_validation_by_default(self) -> None:
        engine = DslEngine(compile_cache_size=1)
        invalid_document = JsonDslParser().parse(
            '{"steps": [], "on_fail": {"decision": "$steps.not_exists.value > 0", "mode": "single", "message_cn": "x", "message_en": "y"}}'
        )
        registry = cast(DatasourceRegistry, _UnusedRegistry())

        result = engine.execute_document(invalid_document, {"source_object_id": "x"}, datasource_registry=registry)

        self.assertFalse(result.passed)
        self.assertEqual(result.phase, "runtime")
        self.assertEqual(result.failed_node, "on_fail")

    def test_validate_document_can_be_used_before_execution(self) -> None:
        engine = DslEngine(compile_cache_size=1)
        invalid_document = JsonDslParser().parse(
            '{"steps": [], "on_fail": {"decision": "$steps.not_exists.value > 0", "mode": "single", "message_cn": "x", "message_en": "y"}}'
        )

        with self.assertRaisesRegex(DSLValidationError, "on_fail.decision"):
            engine.validate_document(invalid_document)
