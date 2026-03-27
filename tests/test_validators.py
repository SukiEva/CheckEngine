"""DSL 校验器测试。"""

from __future__ import annotations

import json
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine.exceptions import DSLParseError, DSLValidationError
from check_engine.parser import JsonDslParser
from check_engine.validator import DslValidator, ReferenceValidator, StructureValidator


class ValidatorTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = JsonDslParser()
        self.structure_validator = StructureValidator()
        self.reference_validator = ReferenceValidator()
        self.validator = DslValidator(self.structure_validator, self.reference_validator)
        example_path = Path(__file__).resolve().parents[1] / "references" / "example.json"
        self.example_data = json.loads(example_path.read_text(encoding="utf-8"))

    def test_validate_example_json(self) -> None:
        document = self.parser.parse(json.dumps(self.example_data))
        self.validator.validate(document)

    def test_validate_without_optional_blocks(self) -> None:
        data = {
            "steps": [
                {
                    "name": "s1",
                    "type": "sql",
                    "datasource": "db",
                    "result_mode": "record",
                    "sql_template": "select 1 as v",
                    "sql_params": {},
                    "outputs": ["v"],
                }
            ],
            "on_fail": {
                "decision": "false",
                "mode": "single",
                "message_cn": "ok",
                "message_en": "ok",
            },
        }
        document = self.parser.parse(json.dumps(data))
        self.validator.validate(document)

    def test_reference_context_without_context_block_raises(self) -> None:
        data = {
            "variables": {
                "threshold": {
                    "when": [{"condition": "$context.flow == 'flow1'", "value": 1}],
                    "default": 0,
                }
            },
            "steps": [
                {
                    "name": "s1",
                    "type": "sql",
                    "datasource": "db",
                    "result_mode": "record",
                    "sql_template": "select 1 as v",
                    "sql_params": {},
                    "outputs": ["v"],
                }
            ],
            "on_fail": {
                "decision": "false",
                "mode": "single",
                "message_cn": "ok",
                "message_en": "ok",
            },
        }
        document = self.parser.parse(json.dumps(data))

        with self.assertRaises(DSLValidationError):
            self.reference_validator.validate(document)


    def test_structure_validator_accepts_frozen_document_collections(self) -> None:
        document = self.parser.parse(json.dumps(self.example_data))

        self.assertIsInstance(document.steps, tuple)
        self.assertIsInstance(document.prechecks, tuple)
        self.assertIsInstance(document.steps[0].outputs, tuple)
        self.assertIsInstance(document.steps[0].consumes, tuple)

        self.structure_validator.validate(document)

    def test_reference_validator_accepts_mapping_collections(self) -> None:
        document = self.parser.parse(json.dumps(self.example_data))

        self.assertIsInstance(document.variables, dict)
        self.assertIsInstance(document.steps[0].sql_params, dict)
        if document.prechecks:
            self.assertIsInstance(document.prechecks[0].sql_params, dict)

        self.reference_validator.validate(document)

    def test_validate_on_fail_exists_call(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["on_fail"]["decision"] = "exists($steps.exchange_rate.final_amount)"
        document = self.parser.parse(json.dumps(data))
        self.validator.validate(document)

    def test_validate_constant_variable(self) -> None:
        data = {
            "variables": {
                "threshold": {
                    "when": [],
                    "default": 700,
                }
            },
            "steps": [
                {
                    "name": "s1",
                    "type": "sql",
                    "datasource": "db",
                    "result_mode": "record",
                    "sql_template": "select 1 as v",
                    "sql_params": {},
                    "outputs": ["v"],
                }
            ],
            "on_fail": {
                "decision": "$variables.threshold > 100",
                "mode": "single",
                "message_cn": "ok",
                "message_en": "ok",
            },
        }
        document = self.parser.parse(json.dumps(data))
        self.validator.validate(document)

    def test_validate_variable_with_empty_condition_raises(self) -> None:
        data = {
            "variables": {
                "threshold": {
                    "when": [{"condition": "   ", "value": 1}],
                }
            },
            "steps": [
                {
                    "name": "s1",
                    "type": "sql",
                    "datasource": "db",
                    "result_mode": "record",
                    "sql_template": "select 1 as v",
                    "sql_params": {},
                    "outputs": ["v"],
                }
            ],
            "on_fail": {
                "decision": "$variables.threshold > 100",
                "mode": "single",
                "message_cn": "ok",
                "message_en": "ok",
            },
        }
        with self.assertRaises(DSLParseError):
            self.parser.parse(json.dumps(data))

    def test_validate_on_fail_bare_exists_raises(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["on_fail"]["decision"] = "exists"
        document = self.parser.parse(json.dumps(data))

        with self.assertRaises(DSLValidationError):
            self.structure_validator.validate(document)

    def test_validate_on_fail_invalid_exists_syntax_raises(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["on_fail"]["decision"] = "exists($steps.exchange_rate.final_amount, $variables.threshold)"
        document = self.parser.parse(json.dumps(data))

        with self.assertRaises(DSLValidationError):
            self.structure_validator.validate(document)

    def test_invalid_sub_repeat_template_raises(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["prechecks"][0]["on_fail"]["message_cn"] = "存在汇率为空的记录: 记录{func}-{txn}-{rate_date}"
        document = self.parser.parse(json.dumps(data))

        with self.assertRaises(DSLValidationError):
            self.structure_validator.validate(document)

    def test_sub_repeat_with_divider_cn_en_is_valid(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["prechecks"][0]["on_fail"].pop("divider", None)
        data["prechecks"][0]["on_fail"]["divider_cn"] = "；"
        data["prechecks"][0]["on_fail"]["divider_en"] = " | "
        document = self.parser.parse(json.dumps(data))

        self.structure_validator.validate(document)

    def test_sub_repeat_missing_divider_en_raises(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["prechecks"][0]["on_fail"].pop("divider", None)
        data["prechecks"][0]["on_fail"]["divider_cn"] = "；"
        data["prechecks"][0]["on_fail"].pop("divider_en", None)
        document = self.parser.parse(json.dumps(data))

        with self.assertRaises(DSLValidationError):
            self.structure_validator.validate(document)

    def test_invalid_step_reference_raises(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["on_fail"]["decision"] = "$steps.not_exists.final_amount > $variables.threshold"
        document = self.parser.parse(json.dumps(data))

        with self.assertRaises(DSLValidationError):
            self.reference_validator.validate(document)

    def test_future_step_reference_in_step_sql_params_raises(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["steps"][0]["sql_params"]["bad_ref"] = "$steps.exchange_rate.final_amount"
        document = self.parser.parse(json.dumps(data))

        with self.assertRaises(DSLValidationError):
            self.reference_validator.validate(document)

    def test_unknown_top_level_field_raises(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["unexpected"] = {}
        document = self.parser.parse(json.dumps(data))

        with self.assertRaises(DSLValidationError) as ctx:
            self.structure_validator.validate(document)
        self.assertIn("Unknown top-level fields", str(ctx.exception))

    def test_on_fail_full_repeat_is_valid(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["on_fail"]["mode"] = "full_repeat"
        data["on_fail"]["message_cn"] = "金额{$steps.query_aggregate_amount.total_amount}超过阈值{$variables.threshold}"
        data["on_fail"]["message_en"] = "Amount {$steps.query_aggregate_amount.total_amount} exceeds threshold {$variables.threshold}."
        document = self.parser.parse(json.dumps(data))

        self.validator.validate(document)

    def test_on_fail_single_mode_cannot_reference_records_output(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["on_fail"]["decision"] = "exists($steps.query_aggregate_amount.total_amount)"
        data["on_fail"]["message_cn"] = "金额{$steps.query_aggregate_amount.total_amount}超过阈值{$variables.threshold}"
        data["on_fail"]["message_en"] = "Amount {$steps.query_aggregate_amount.total_amount} exceeds threshold {$variables.threshold}."
        document = self.parser.parse(json.dumps(data))

        with self.assertRaises(DSLValidationError):
            self.reference_validator.validate(document)

    def test_on_fail_sub_repeat_can_reference_records_output(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["on_fail"]["decision"] = "exists($steps.query_aggregate_amount.total_amount)"
        data["on_fail"]["mode"] = "sub_repeat"
        data["on_fail"]["divider"] = "，"
        data["on_fail"]["message_cn"] = "超限金额: [{$steps.query_aggregate_amount.func}-{$steps.query_aggregate_amount.total_amount}]"
        data["on_fail"]["message_en"] = "Exceeded amounts: [{$steps.query_aggregate_amount.func}-{$steps.query_aggregate_amount.total_amount}]"
        document = self.parser.parse(json.dumps(data))

        self.validator.validate(document)

    def test_reserved_step_name_raises(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["steps"][0]["name"] = "context"
        document = self.parser.parse(json.dumps(data))

        with self.assertRaises(DSLValidationError) as ctx:
            self.structure_validator.validate(document)
        self.assertIn("reserved node name", str(ctx.exception))

    def test_variable_default_is_required(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["variables"]["threshold"].pop("default")
        document = self.parser.parse(json.dumps(data))

        with self.assertRaises(DSLValidationError):
            self.structure_validator.validate(document)

    def test_variable_cannot_reference_later_variable(self) -> None:
        data = {
            "variables": {
                "threshold": {
                    "when": [{"condition": "$variables.limit > 0", "value": 1}],
                    "default": 0,
                },
                "limit": {
                    "when": [],
                    "default": 10,
                },
            },
            "steps": [
                {
                    "name": "s1",
                    "type": "sql",
                    "datasource": "db",
                    "result_mode": "record",
                    "sql_template": "select 1 as v",
                    "sql_params": {},
                    "outputs": ["v"],
                }
            ],
            "on_fail": {
                "decision": "$variables.threshold > 100",
                "mode": "single",
                "message_cn": "ok",
                "message_en": "ok",
            },
        }
        document = self.parser.parse(json.dumps(data))

        with self.assertRaises(DSLValidationError) as ctx:
            self.reference_validator.validate(document)
        self.assertIn("references a variable not available", str(ctx.exception))

    def test_consumed_step_without_outputs_raises(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["steps"][0].pop("outputs")
        document = self.parser.parse(json.dumps(data))

        with self.assertRaises(DSLValidationError) as ctx:
            self.reference_validator.validate(document)
        self.assertIn("outputs that are not declared", str(ctx.exception))

    def test_duplicate_consume_alias_raises(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["steps"][1]["consumes"].append(
            {
                "from": "$steps.query_aggregate_amount",
                "alias": "am",
            }
        )
        document = self.parser.parse(json.dumps(data))

        with self.assertRaises(DSLValidationError) as ctx:
            self.structure_validator.validate(document)
        self.assertIn("alias is duplicated", str(ctx.exception))

    def test_on_fail_single_mode_disallows_records_output_reference(self) -> None:
        data = {
            "steps": [
                {
                    "name": "s1",
                    "type": "sql",
                    "datasource": "db",
                    "result_mode": "records",
                    "sql_template": "select 1 as amount",
                    "sql_params": {},
                    "outputs": ["amount"],
                }
            ],
            "on_fail": {
                "decision": "false",
                "mode": "single",
                "message_cn": "金额{$steps.s1.amount}",
                "message_en": "Amount {$steps.s1.amount}",
            },
        }
        document = self.parser.parse(json.dumps(data))

        with self.assertRaises(DSLValidationError) as ctx:
            self.reference_validator.validate(document)
        self.assertIn("cannot reference array outputs in single mode", str(ctx.exception))

    def test_non_readonly_sql_raises_with_error(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["steps"][0]["sql_template"] = "update t set c = 1"
        document = self.parser.parse(json.dumps(data))

        with self.assertRaises(DSLValidationError) as ctx:
            self.validator.validate(document)
        self.assertTrue(
            "only SELECT/WITH queries are allowed." in str(ctx.exception)
            or "contains non-read-only SQL keyword." in str(ctx.exception)
        )


    def test_context_sql_params_invalid_reference_raises(self) -> None:
        data = {
            "context": {
                "type": "sql",
                "datasource": "db",
                "result_mode": "record",
                "sql_template": "select 1 as flow",
                "sql_params": {"source_object_id": "$steps.step_a.v"},
                "outputs": ["flow"],
            },
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
                "decision": "false",
                "mode": "single",
                "message_cn": "ok",
                "message_en": "ok",
            },
        }
        document = self.parser.parse(json.dumps(data))

        with self.assertRaises(DSLValidationError) as ctx:
            self.reference_validator.validate(document)
        self.assertIn("references a step not available", str(ctx.exception))

    def test_context_sql_params_only_allow_input_scope(self) -> None:
        data = {
            "context": {
                "type": "sql",
                "datasource": "db",
                "result_mode": "record",
                "sql_template": "select 1 as flow",
                "sql_params": {"source_object_id": "$input.source_object_id"},
                "outputs": ["flow"],
            },
            "variables": {
                "threshold": {
                    "when": [],
                    "default": 1,
                }
            },
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
                "decision": "false",
                "mode": "single",
                "message_cn": "ok",
                "message_en": "ok",
            },
        }
        document = self.parser.parse(json.dumps(data))

        self.reference_validator.validate(document)

    def test_compile_invalid_expression_returns_validation_error(self) -> None:
        data = {
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
                "decision": "lambda: true",
                "mode": "single",
                "message_cn": "x",
                "message_en": "y",
            },
        }

        from check_engine.engine import DslEngine

        with self.assertRaises(DSLValidationError) as ctx:
            DslEngine().compile(json.dumps(data))
        self.assertIn("is invalid", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
