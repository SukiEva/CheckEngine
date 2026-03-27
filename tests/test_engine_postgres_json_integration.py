"""基于 PostgreSQL + 外部 JSON 路径的 ExecDSL 集成测试。"""

from __future__ import annotations

import importlib.util
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine import DslEngine, StaticDatasourceRegistry


def _json_path() -> Path:
    path = os.environ.get("EXEC_DSL_JSON_PATH")
    if path:
        return Path(path)
    return Path(__file__).resolve().parents[1] / "references" / "example.json"


def _postgres_url(prefix: str) -> str:
    specific = os.environ.get(f"{prefix}_POSTGRES_URL")
    if specific:
        return specific
    shared = os.environ.get("POSTGRES_TEST_URL")
    if shared:
        return shared
    raise unittest.SkipTest(f"缺少数据库连接配置: {prefix}_POSTGRES_URL 或 POSTGRES_TEST_URL")


def _source_object_id() -> str:
    value = os.environ.get("EXEC_DSL_SOURCE_OBJECT_ID")
    if value:
        return value
    raise unittest.SkipTest("缺少执行输入: EXEC_DSL_SOURCE_OBJECT_ID")


@unittest.skipUnless(importlib.util.find_spec("sqlalchemy") is not None, "需要安装 sqlalchemy")
@unittest.skipUnless(importlib.util.find_spec("psycopg2") is not None, "需要安装 psycopg2-binary（可选依赖 postgres）")
class DslEnginePostgresJsonIntegrationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        class DbManager:
            def __init__(self, db_url: str) -> None:
                self.engine = create_engine(db_url)
                self.session_creator = sessionmaker(bind=self.engine)

            @contextmanager
            def get_session(self) -> Generator[Any, None, None]:
                session = self.session_creator()
                try:
                    yield session
                finally:
                    session.close()

            def dispose(self) -> None:
                self.engine.dispose()

        self.dsl_text = _json_path().read_text(encoding="utf-8")
        self.saas_db = DbManager(_postgres_url("SAAS"))
        self.data_db = DbManager(_postgres_url("DATA"))
        self.registry = StaticDatasourceRegistry(
            {
                "saas_db": self.saas_db,
                "data_db": self.data_db,
            }
        )
        self.engine = DslEngine()

    def tearDown(self) -> None:
        self.saas_db.dispose()
        self.data_db.dispose()

    def test_execute_from_json_path_runs_full_flow(self) -> None:
        result = self.engine.execute(
            self.dsl_text,
            {"source_object_id": _source_object_id()},
            self.registry,
        )

        self.assertIn(result.phase, ("pass", "precheck", "final"))
        self.assertIsNotNone(result.executed_nodes)
        self.assertGreaterEqual(len(result.executed_nodes), 1)


if __name__ == "__main__":
    unittest.main()
