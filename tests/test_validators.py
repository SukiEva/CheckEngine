"""DSL 校验器测试。"""

from __future__ import annotations

import json
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine.exceptions import DSLValidationError
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

    def test_invalid_sub_repeat_template_raises(self) -> None:
        data = json.loads(json.dumps(self.example_data))
        data["prechecks"][0]["on_fail"]["message_cn"] = "存在汇率为空的记录: 记录{func}-{txn}-{rate_date}"
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
