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


if __name__ == "__main__":
    unittest.main()
