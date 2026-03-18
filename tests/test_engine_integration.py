"""主引擎集成测试。"""

from __future__ import annotations

import importlib.util
import json
import sys
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Optional
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine import DslEngine, StaticDatasourceRegistry


@unittest.skipUnless(importlib.util.find_spec("sqlalchemy") is not None, "需要安装 sqlalchemy")
class DslEngineIntegrationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        example_path = Path(__file__).resolve().parents[1] / "references" / "example.json"
        self.dsl_text = example_path.read_text(encoding="utf-8")
        self._text = self._load_text()

        class _SqlAlchemyDatasource:
            def __init__(self, db_url: str) -> None:
                self.engine = create_engine(db_url)
                self._session_factory = sessionmaker(bind=self.engine)

            @contextmanager
            def get_session(self):
                session = self._session_factory()
                try:
                    yield session
                finally:
                    session.close()

            def dispose(self) -> None:
                self.engine.dispose()

        self.saas_db = _SqlAlchemyDatasource("sqlite+pysqlite:///:memory:")
        self.data_db = _SqlAlchemyDatasource("sqlite+pysqlite:///:memory:")
        self.registry = StaticDatasourceRegistry(
            {
                "saas_db": self.saas_db,
                "data_db": self.data_db,
            }
        )
        self.engine = DslEngine()
        self._create_schema()

    def _load_text(self):
        from sqlalchemy import text

        return text

    def tearDown(self) -> None:
        self.saas_db.dispose()
        self.data_db.dispose()

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

    def test_execute_returns_final_failure(self) -> None:
        self._insert_header("FAIL_FINAL", "flow1", "scenario1")
        self._insert_journal("FAIL_FINAL", "USD", "1", "user", "2024-01-01", 1.0, 600)
        self._insert_journal("FAIL_FINAL", "CNY", "2", "user", "2024-01-01", 1.0, 700)
        self._insert_rate("CNY", 1.0)

        result = self.engine.execute(self.dsl_text, {"source_object_id": "FAIL_FINAL"}, self.registry)

        self.assertFalse(result.passed)
        self.assertEqual(result.phase, "final")
        self.assertEqual(result.failed_node, "on_fail")
        if result.message_cn is None or result.message_en is None:
            self.fail("final failure should include both Chinese and English messages")
        self.assertIn("超过阈值1000", result.message_cn)
        self.assertIn("exceeds the threshold 1000", result.message_en)
        self.assertEqual(result.steps["exchange_rate"]["final_amount"], 1300)

    def test_execute_returns_final_failure_with_exists_function(self) -> None:
        self._insert_header("FAIL_EXISTS", "flow1", "scenario1")
        self._insert_journal("FAIL_EXISTS", "USD", "1", "user", "2024-01-01", 1.0, 400)
        self._insert_rate("USD", 1.0)

        dsl_data = json.loads(self.dsl_text)
        dsl_data["on_fail"]["decision"] = "exists($steps.exchange_rate.final_amount)"
        result = self.engine.execute(json.dumps(dsl_data), {"source_object_id": "FAIL_EXISTS"}, self.registry)

        self.assertFalse(result.passed)
        self.assertEqual(result.phase, "final")
        self.assertEqual(result.failed_node, "on_fail")

    def test_execute_with_constant_variable(self) -> None:
        self._insert_header("FAIL_CONSTANT", "flow1", "scenario1")
        self._insert_journal("FAIL_CONSTANT", "USD", "1", "user", "2024-01-01", 1.0, 400)
        self._insert_journal("FAIL_CONSTANT", "CNY", "2", "user", "2024-01-01", 1.0, 700)
        self._insert_rate("CNY", 1.0)

        dsl_data = json.loads(self.dsl_text)
        dsl_data["variables"] = {
            "threshold": {
                "when": [],
                "default": 800,
            }
        }
        result = self.engine.execute(json.dumps(dsl_data), {"source_object_id": "FAIL_CONSTANT"}, self.registry)

        self.assertFalse(result.passed)
        self.assertEqual(result.phase, "final")
        if result.message_cn is None:
            self.fail("final failure should include Chinese message")
        self.assertIn("阈值800", result.message_cn)

    def test_engine_allows_configuring_compile_cache_size(self) -> None:
        engine = DslEngine(compile_cache_size=1)

        self.assertEqual(engine.compile_cache_size, 1)

    def test_execute_on_fail_exists_with_records_field_reference(self) -> None:
        self._insert_header("FAIL_RECORDS_EXISTS", "flow1", "scenario1")
        self._insert_journal("FAIL_RECORDS_EXISTS", "USD", "1", "user", "2024-01-01", 1.0, 321)

        dsl_data = {
            "steps": [
                {
                    "name": "query_duplicate_entry_lines",
                    "description": "查询重复的行",
                    "type": "sql",
                    "datasource": "saas_db",
                    "result_mode": "records",
                    "sql_template": "SELECT amount AS duplicate_entry_lines FROM jounrnal WHERE header_id = :source_object_id",
                    "sql_params": {"source_object_id": "$input.source_object_id"},
                    "outputs": ["duplicate_entry_lines"],
                }
            ],
            "on_fail": {
                "decision": "exists($steps.query_duplicate_entry_lines.duplicate_entry_lines)",
                "mode": "single",
                "message_cn": "存在疑似重复行",
                "message_en": "Suspected Duplicate Line Found",
            },
        }

        result = self.engine.execute(json.dumps(dsl_data), {"source_object_id": "FAIL_RECORDS_EXISTS"}, self.registry)

        self.assertFalse(result.passed)
        self.assertEqual(result.phase, "final")
        self.assertEqual(result.failed_node, "on_fail")

    def test_execute_returns_precheck_failure(self) -> None:
        self._insert_header("FAIL_PRECHECK", "flow1", "scenario1")
        self._insert_journal("FAIL_PRECHECK", "USD", "1", "user", "2024-01-01", None, 100)
        self._insert_rate("USD", 1.0)

        result = self.engine.execute(self.dsl_text, {"source_object_id": "FAIL_PRECHECK"}, self.registry)

        self.assertFalse(result.passed)
        self.assertEqual(result.phase, "precheck")
        self.assertEqual(result.failed_node, "check_rate_null")
        self.assertEqual(result.message_cn, "存在汇率为空的记录: 记录USD-1-2024-01-01")
        if result.message_en is None:
            self.fail("precheck failure should include English message")
        self.assertIn("null exchange rates", result.message_en)

    def _create_schema(self) -> None:
        with self.saas_db.get_session() as session:
            session.execute(
                self._text(
                    """
                    CREATE TABLE header (
                        header_id TEXT PRIMARY KEY,
                        flow TEXT NOT NULL,
                        scenario TEXT NOT NULL
                    )
                    """
                )
            )
            session.execute(
                self._text(
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
            )
            session.commit()

        with self.data_db.get_session() as session:
            session.execute(
                self._text(
                    """
                    CREATE TABLE exchange_rate (
                        func TEXT NOT NULL,
                        rate REAL NOT NULL,
                        rate_date TEXT NOT NULL
                    )
                    """
                )
            )
            session.commit()

    def _insert_header(self, header_id: str, flow: str, scenario: str) -> None:
        with self.saas_db.get_session() as session:
            session.execute(
                self._text("INSERT INTO header(header_id, flow, scenario) VALUES (:header_id, :flow, :scenario)"),
                {"header_id": header_id, "flow": flow, "scenario": scenario},
            )
            session.commit()

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
        with self.saas_db.get_session() as session:
            session.execute(
                self._text(
                    """
                    INSERT INTO jounrnal(header_id, func, txn, rate_type, rate_date, rate, amount)
                    VALUES (:header_id, :func, :txn, :rate_type, :rate_date, :rate, :amount)
                    """
                ),
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
            session.commit()

    def _insert_rate(self, func: str, rate: float) -> None:
        with self.data_db.get_session() as session:
            session.execute(
                self._text(
                    """
                    INSERT INTO exchange_rate(func, rate, rate_date)
                    VALUES (:func, :rate, :rate_date)
                    """
                ),
                {
                    "func": func,
                    "rate": rate,
                    "rate_date": date.today().isoformat(),
                },
            )
            session.commit()


if __name__ == "__main__":
    unittest.main()
