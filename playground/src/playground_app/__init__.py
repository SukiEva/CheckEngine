"""FastAPI 版 ExecDSL Playground。"""

from __future__ import annotations

import os
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
