"""Flask 版 ExecDSL Playground。"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Optional

from flask import Flask, jsonify, render_template, request
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from check_engine import DSLExecutionError, DslEngine, StaticDatasourceRegistry


@dataclass
class DatasourceConfig:
    """前端提交的数据源连接配置。"""

    name: str
    db_url: str


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


def create_app() -> Flask:
    """创建 Flask 应用。"""
    app = Flask(__name__)
    dsl_engine = DslEngine()

    @app.get("/")
    def index() -> str:
        return render_template("exec_dsl_flow_designer.html")

    @app.post("/api/run-dsl")
    def run_dsl() -> Any:
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify({"error": "请求体必须是 JSON 对象。"}), 400

        dsl_text = payload.get("dsl_text")
        input_data = payload.get("input_data", {})
        datasources = payload.get("datasources", [])

        if not isinstance(dsl_text, str) or not dsl_text.strip():
            return jsonify({"error": "dsl_text 不能为空。"}), 400
        if not isinstance(input_data, dict):
            return jsonify({"error": "input_data 必须是 JSON 对象。"}), 400
        if not isinstance(datasources, list) or len(datasources) == 0:
            return jsonify({"error": "datasources 至少需要一个数据源配置。"}), 400

        created_engines: Optional[list[Engine]] = None
        try:
            datasource_configs = _parse_datasource_configs(datasources)
            registry, created_engines = _build_registry(datasource_configs)
            result = dsl_engine.execute(dsl_text, input_data, registry)
            return jsonify({"result": result.to_dict()})
        except (ValueError, DSLExecutionError) as exc:
            return jsonify({"error": str(exc)}), 400
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
    app = create_app()
    app.run(host="127.0.0.1", port=5001, debug=True)


if __name__ == "__main__":
    main()
