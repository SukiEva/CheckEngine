"""SQL 安全校验器边界测试。"""

from __future__ import annotations

import json
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine.parser import JsonDslParser
from check_engine.validator.sql_validator import SqlSafetyValidator


class SqlSafetyValidatorEdgeCaseTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = JsonDslParser()
        self.validator = SqlSafetyValidator()

    def test_allows_sql_with_leading_comments(self) -> None:
        document = self.parser.parse(
            json.dumps(
                {
                    "steps": [
                        {
                            "name": "step_a",
                            "type": "sql",
                            "datasource": "db",
                            "result_mode": "record",
                            "sql_template": "-- note\n/* detail */\nSELECT 1 AS value",
                            "sql_params": {},
                            "outputs": ["value"],
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
        )

        self.validator.validate(document)

    def test_ignores_forbidden_keywords_inside_string_literals(self) -> None:
        document = self.parser.parse(
            json.dumps(
                {
                    "steps": [
                        {
                            "name": "step_a",
                            "type": "sql",
                            "datasource": "db",
                            "result_mode": "record",
                            "sql_template": "SELECT 'delete keyword' AS value",
                            "sql_params": {},
                            "outputs": ["value"],
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
        )

        self.validator.validate(document)


if __name__ == "__main__":
    unittest.main()
