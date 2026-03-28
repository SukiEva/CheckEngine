"""FastAPI 版 ExecDSL Playground。"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from check_engine import DSLExecutionError, DslEngine, StaticDatasourceRegistry


@dataclass
class DatasourceConfig:
    """前端提交的数据源连接配置。"""

    name: str
    db_url: str


_SQLITE_FILE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "playground.db")
)
PGDialect_psycopg2._get_server_version_info = lambda *args: (9, 2)


class SqlAlchemyDatasource:
    """供 check_engine 使用的 SQLAlchemy 数据源包装。"""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._session_factory = sessionmaker(bind=self.engine)

    @contextmanager
    def get_session(self) -> Session:
        session = self._session_factory()
        try:
            yield session
        finally:
            session.close()


def create_app() -> FastAPI:
    """创建 FastAPI 应用。"""
    app = FastAPI(title="ExecDSL Playground")
    dsl_engine = DslEngine()
    templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

    @app.on_event("startup")
    def init_datasource_storage() -> None:
        _init_datasource_table()

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request) -> Any:
        return templates.TemplateResponse(
            request=request,
            name="exec_dsl_flow_designer.html",
            context={"request": request},
        )

    @app.post("/api/run-dsl")
    async def run_dsl(request: Request) -> dict[str, Any]:
        try:
            payload = await request.json()
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="请求体必须是 JSON 对象。") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="请求体必须是 JSON 对象。")

        dsl_text = payload.get("dsl_text")
        input_data = payload.get("input_data", {})
        datasources = payload.get("datasources", [])

        if not isinstance(dsl_text, str) or not dsl_text.strip():
            raise HTTPException(status_code=400, detail="dsl_text 不能为空。")
        if not isinstance(input_data, dict):
            raise HTTPException(status_code=400, detail="input_data 必须是 JSON 对象。")
        if not isinstance(datasources, list) or len(datasources) == 0:
            raise HTTPException(status_code=400, detail="datasources 至少需要一个数据源配置。")

        created_engines: Optional[list[Engine]] = None
        try:
            datasource_configs = _parse_datasource_configs(datasources)
            registry, created_engines = _build_registry(datasource_configs)
            result = dsl_engine.execute(dsl_text, input_data, registry)
            return {"result": result.to_dict()}
        except (ValueError, DSLExecutionError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        finally:
            _dispose_engines(created_engines)

    @app.get("/api/datasource-configs")
    def list_datasource_configs() -> dict[str, list[dict[str, str]]]:
        return {"datasources": _load_datasource_configs()}

    @app.put("/api/datasource-configs")
    async def save_datasource_configs(request: Request) -> dict[str, Any]:
        try:
            payload = await request.json()
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="请求体必须是 JSON 对象。") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="请求体必须是 JSON 对象。")
        raw_datasources = payload.get("datasources")
        if not isinstance(raw_datasources, list):
            raise HTTPException(status_code=400, detail="datasources 必须是数组。")
        try:
            datasource_configs = _parse_datasource_configs(raw_datasources)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        _save_datasource_configs(datasource_configs)
        return {
            "saved": len(datasource_configs),
            "datasources": [
                {"name": item.name, "db_url": item.db_url}
                for item in datasource_configs
            ],
        }

    return app


def _parse_datasource_configs(raw_datasources: list[object]) -> list[DatasourceConfig]:
    configs: list[DatasourceConfig] = []
    for index, item in enumerate(raw_datasources):
        if not isinstance(item, dict):
            raise ValueError(f"第 {index + 1} 个 datasource 配置必须是对象。")

        name = item.get("name")
        db_url = item.get("db_url")

        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"第 {index + 1} 个 datasource 缺少 name。")
        if not isinstance(db_url, str) or not db_url.strip():
            raise ValueError(f"第 {index + 1} 个 datasource 缺少 db_url。")
        if not _is_postgres_url(db_url):
            raise ValueError(
                f"第 {index + 1} 个 datasource 的 db_url 不是 PostgreSQL 连接串，当前仅支持 postgresql+psycopg2。"
            )

        configs.append(
            DatasourceConfig(
                name=name.strip(),
                db_url=db_url.strip(),
            )
        )
    return configs


def _build_registry(configs: list[DatasourceConfig]) -> tuple[StaticDatasourceRegistry, list[Engine]]:
    datasource_mapping: dict[str, SqlAlchemyDatasource] = {}
    engines: list[Engine] = []
    for config in configs:
        if config.name in datasource_mapping:
            raise ValueError(f"重复的数据源名称: {config.name}")
        sql_engine = create_engine(config.db_url, pool_pre_ping=True)
        _validate_connection(sql_engine, config.name)
        engines.append(sql_engine)
        datasource_mapping[config.name] = SqlAlchemyDatasource(sql_engine)
    return StaticDatasourceRegistry(datasource_mapping), engines


def _validate_connection(sql_engine: Engine, datasource_name: str) -> None:
    try:
        with sql_engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"数据源 {datasource_name} 连接失败，请检查 PostgreSQL 连接信息。") from exc


def _is_postgres_url(db_url: str) -> bool:
    lowered = db_url.strip().lower()
    return lowered.startswith("postgresql://") or lowered.startswith("postgresql+psycopg2://")


def _dispose_engines(engines: Optional[list[Engine]]) -> None:
    if engines is None:
        return
    for sql_engine in engines:
        sql_engine.dispose()


def _init_datasource_table() -> None:
    os.makedirs(os.path.dirname(_SQLITE_FILE), exist_ok=True)
    with sqlite3.connect(_SQLITE_FILE) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS datasource_configs (
              name TEXT PRIMARY KEY,
              db_url TEXT NOT NULL,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.commit()


def _load_datasource_configs() -> list[dict[str, str]]:
    _init_datasource_table()
    with sqlite3.connect(_SQLITE_FILE) as connection:
        cursor = connection.execute(
            "SELECT name, db_url FROM datasource_configs ORDER BY name ASC"
        )
        rows = cursor.fetchall()
    if not rows:
        return [
            {
                "name": "saas_db",
                "db_url": "postgresql+psycopg2://user:password@127.0.0.1:5432/saas_db",
            },
            {
                "name": "data_db",
                "db_url": "postgresql+psycopg2://user:password@127.0.0.1:5432/data_db",
            },
        ]
    return [{"name": row[0], "db_url": row[1]} for row in rows]


def _save_datasource_configs(configs: list[DatasourceConfig]) -> None:
    _init_datasource_table()
    with sqlite3.connect(_SQLITE_FILE) as connection:
        connection.execute("DELETE FROM datasource_configs")
        connection.executemany(
            """
            INSERT INTO datasource_configs (name, db_url, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            """,
            [(item.name, item.db_url) for item in configs],
        )
        connection.commit()


def main() -> None:
    app_host = os.getenv("PLAYGROUND_HOST", "0.0.0.0")
    app_port = int(os.getenv("PLAYGROUND_PORT", "5001"))
    app_reload = os.getenv("PLAYGROUND_DEBUG", "false").lower() == "true"
    uvicorn.run(
        "playground_app:create_app",
        host=app_host,
        port=app_port,
        reload=app_reload,
        factory=True,
    )


__all__ = ["create_app", "main"]
