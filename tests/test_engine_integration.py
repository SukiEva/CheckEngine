"""主引擎集成测试。"""

from __future__ import annotations

import sqlite3
import sys
from datetime import date
from pathlib import Path
from typing import Optional
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine import DslEngine, StaticDatasourceRegistry


class DslEngineIntegrationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        example_path = Path(__file__).resolve().parents[1] / "references" / "example.json"
        self.dsl_text = example_path.read_text(encoding="utf-8")
        self.saas_db = sqlite3.connect(":memory:")
        self.data_db = sqlite3.connect(":memory:")
        self.registry = StaticDatasourceRegistry(
            {
                "saas_db": self.saas_db,
                "data_db": self.data_db,
            }
        )
        self.engine = DslEngine()
        self._create_schema()

    def tearDown(self) -> None:
        self.saas_db.close()
        self.data_db.close()

    def test_execute_returns_pass(self) -> None:
        self._insert_header("PASS_1", "flow1", "scenario1")
        self._insert_journal("PASS_1", "USD", "1", "user", "2024-01-01", 1.0, 400)
        self._insert_journal("PASS_1", "CNY", "2", "user", "2024-01-01", 1.0, 500)
        self._insert_rate("CNY", 1.0)

        result = self.engine.execute(self.dsl_text, {"source_object_id": "PASS_1"}, self.registry)

        self.assertTrue(result.passed)
        self.assertEqual(result.phase, "pass")
        self.assertEqual(result.variables["threshold"], 1000)
        self.assertEqual(result.steps["exchange_rate"]["final_amount"], 900)
        self.assertEqual(len(result.trace), 5)

    def test_execute_returns_final_failure(self) -> None:
        self._insert_header("FAIL_FINAL", "flow1", "scenario1")
        self._insert_journal("FAIL_FINAL", "USD", "1", "user", "2024-01-01", 1.0, 600)
        self._insert_journal("FAIL_FINAL", "CNY", "2", "user", "2024-01-01", 1.0, 700)
        self._insert_rate("CNY", 1.0)

        result = self.engine.execute(self.dsl_text, {"source_object_id": "FAIL_FINAL"}, self.registry)

        self.assertFalse(result.passed)
        self.assertEqual(result.phase, "final")
        self.assertEqual(result.failed_node, "on_fail")
        self.assertIn("超过阈值1000", result.message_cn)
        self.assertIn("exceeds the threshold 1000", result.message_en)
        self.assertEqual(result.steps["exchange_rate"]["final_amount"], 1300)

    def test_execute_returns_precheck_failure(self) -> None:
        self._insert_header("FAIL_PRECHECK", "flow1", "scenario1")
        self._insert_journal("FAIL_PRECHECK", "USD", "1", "user", "2024-01-01", None, 100)
        self._insert_rate("USD", 1.0)

        result = self.engine.execute(self.dsl_text, {"source_object_id": "FAIL_PRECHECK"}, self.registry)

        self.assertFalse(result.passed)
        self.assertEqual(result.phase, "precheck")
        self.assertEqual(result.failed_node, "check_rate_null")
        self.assertEqual(result.message_cn, "存在汇率为空的记录: 记录USD-1-2024-01-01")
        self.assertIn("null exchange rates", result.message_en)
        self.assertEqual(len(result.trace), 2)

    def _create_schema(self) -> None:
        self.saas_db.execute(
            """
            CREATE TABLE header (
                header_id TEXT PRIMARY KEY,
                flow TEXT NOT NULL,
                scenario TEXT NOT NULL
            )
            """
        )
        self.saas_db.execute(
            """
            CREATE TABLE jounrnal (
                header_id TEXT NOT NULL,
                func TEXT NOT NULL,
                txn TEXT NOT NULL,
                rate_type TEXT NOT NULL,
                rate_date TEXT NOT NULL,
                rate REAL,
                amount REAL NOT NULL
            )
            """
        )
        self.data_db.execute(
            """
            CREATE TABLE exchange_rate (
                func TEXT NOT NULL,
                rate REAL NOT NULL,
                rate_date TEXT NOT NULL
            )
            """
        )

    def _insert_header(self, header_id: str, flow: str, scenario: str) -> None:
        self.saas_db.execute(
            "INSERT INTO header(header_id, flow, scenario) VALUES (:header_id, :flow, :scenario)",
            {"header_id": header_id, "flow": flow, "scenario": scenario},
        )
        self.saas_db.commit()

    def _insert_journal(
        self,
        header_id: str,
        func: str,
        txn: str,
        rate_type: str,
        rate_date: str,
        rate: Optional[float],
        amount: float,
    ) -> None:
        self.saas_db.execute(
            """
            INSERT INTO jounrnal(header_id, func, txn, rate_type, rate_date, rate, amount)
            VALUES (:header_id, :func, :txn, :rate_type, :rate_date, :rate, :amount)
            """,
            {
                "header_id": header_id,
                "func": func,
                "txn": txn,
                "rate_type": rate_type,
                "rate_date": rate_date,
                "rate": rate,
                "amount": amount,
            },
        )
        self.saas_db.commit()

    def _insert_rate(self, func: str, rate: float) -> None:
        self.data_db.execute(
            """
            INSERT INTO exchange_rate(func, rate, rate_date)
            VALUES (:func, :rate, :rate_date)
            """,
            {
                "func": func,
                "rate": rate,
                "rate_date": date.today().isoformat(),
            },
        )
        self.data_db.commit()


if __name__ == "__main__":
    unittest.main()
