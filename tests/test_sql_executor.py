"""SQL 执行器测试。"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine.sql.executor import SqlExecutor


class _FakeMappingsResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeExecuteResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return _FakeMappingsResult(self._rows)


class _FakeSession:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, _sql, _params):
        return _FakeExecuteResult(self._rows)


class _FakeDatasource:
    def __init__(self, rows):
        self._rows = rows

    def get_session(self):
        session = _FakeSession(self._rows)
        try:
            yield session
        finally:
            pass


@unittest.skipUnless(importlib.util.find_spec("sqlalchemy") is not None, "需要安装 sqlalchemy")
class SqlExecutorTestCase(unittest.TestCase):
    def test_run_sql_uses_mappings_all(self) -> None:
        executor = SqlExecutor()
        datasource = _FakeDatasource([
            {"amount": 10, "code": "A"},
            {"amount": 20, "code": "B"},
        ])

        rows = executor._run_sql(datasource, "SELECT 1", {})

        self.assertEqual(rows, [{"amount": 10, "code": "A"}, {"amount": 20, "code": "B"}])


if __name__ == "__main__":
    unittest.main()
