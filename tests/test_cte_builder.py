"""CTE 构造器测试。"""

from __future__ import annotations

from decimal import Decimal
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine.sql.cte_builder import CteBuilder


class CteBuilderTestCase(unittest.TestCase):
    def test_build_single_cte_preserves_decimal_params(self) -> None:
        builder = CteBuilder()
        rows = [
            {"amount": Decimal("12345678901234567890.123456789")},
            {"amount": Decimal("0.000000000000000000123456789")},
        ]

        sql, params = builder._build_single_cte("am", rows, ["amount"])

        self.assertIn("VALUES", sql)
        self.assertEqual(params["__cte_am_0_amount"], Decimal("12345678901234567890.123456789"))
        self.assertEqual(params["__cte_am_1_amount"], Decimal("0.000000000000000000123456789"))
        self.assertIsInstance(params["__cte_am_0_amount"], Decimal)
        self.assertIsInstance(params["__cte_am_1_amount"], Decimal)

    def test_build_single_cte_keeps_identifiers_raw_and_sanitizes_param_names(self) -> None:
        builder = CteBuilder()
        rows = [{"Total Amount": Decimal("100.01"), "select": "usd"}]

        sql, params = builder._build_single_cte("source-data", rows, ["Total Amount", "select"])

        self.assertIn("source-data(Total Amount, select) AS (VALUES", sql)
        self.assertIn(":__cte_source_data_0_Total_Amount", sql)
        self.assertIn(":__cte_source_data_0_select", sql)
        self.assertEqual(params["__cte_source_data_0_Total_Amount"], Decimal("100.01"))
        self.assertEqual(params["__cte_source_data_0_select"], "usd")

    def test_build_single_cte_for_empty_rows_keeps_identifiers_raw(self) -> None:
        builder = CteBuilder()
        sql, params = builder._build_single_cte("am", [], ["Total Amount"])

        self.assertEqual(sql, "am(Total Amount) AS (SELECT NULL AS Total Amount WHERE 1=0)")
        self.assertEqual(params, {})


if __name__ == "__main__":
    unittest.main()
