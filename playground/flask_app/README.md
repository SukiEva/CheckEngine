# Flask Playground（独立子项目）

该子项目基于 `exec_dsl_flow_designer.html` 提供可执行闭环：

- 通过流程图方式生成 DSL
- 配置真实 PostgreSQL 数据源连接
- 直接调用 `check-engine` 运行并查看标准结果

## 目录结构

```text
playground/flask_app/
  pyproject.toml
  README.md
  src/playground_app/
    __init__.py
    app.py
    templates/exec_dsl_flow_designer.html
```

## 运行方式（使用 uv）

```bash
cd playground/flask_app
uv sync
uv run playground-server
```

启动后访问：<http://127.0.0.1:5001>

## 数据源配置要求

运行面板中的数据源配置仅支持 PostgreSQL：

- `postgresql://user:password@host:5432/dbname`
- `postgresql+psycopg2://user:password@host:5432/dbname`

后端会在执行前对每个数据源执行 `SELECT 1` 进行连通性校验。

## 接口说明

- `POST /api/run-dsl`
- 请求体示例：

```json
{
  "dsl_text": "{\"steps\": [], \"on_fail\": {\"decision\": \"false\", \"message_cn\": \"\", \"message_en\": \"\"}}",
  "input_data": {"source_object_id": "DOC_1001"},
  "datasources": [
    {
      "name": "saas_db",
      "db_url": "postgresql+psycopg2://user:password@127.0.0.1:5432/saas_db"
    },
    {
      "name": "data_db",
      "db_url": "postgresql+psycopg2://user:password@127.0.0.1:5432/data_db"
    }
  ]
}
```
