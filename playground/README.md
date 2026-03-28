# FastAPI Playground（独立子项目）

该子项目基于 `exec_dsl_flow_designer.html` 提供可执行闭环：

- 通过流程图方式生成 DSL
- 配置真实 PostgreSQL 数据源连接
- 直接调用 `check-engine` 运行并查看标准结果

## 目录结构

```text
playground/
  pyproject.toml
  README.md
  src/playground_app/
    __init__.py
    templates/exec_dsl_flow_designer.html
```

## 运行方式（使用 uv）

```bash
cd playground
uv sync
uv run playground-server
```

默认会监听 `0.0.0.0:5001`，方便容器/远程环境端口转发。

- 本机浏览器访问：<http://127.0.0.1:5001>
- 远程开发环境请使用对应的端口转发地址

可用环境变量覆盖：

- `PLAYGROUND_HOST`（默认 `0.0.0.0`）
- `PLAYGROUND_PORT`（默认 `5001`）
- `PLAYGROUND_DEBUG`（默认 `false`）

## 数据源配置要求

运行面板中的数据源配置仅支持 PostgreSQL：

- `postgresql://user:password@host:5432/dbname`
- `postgresql+psycopg2://user:password@host:5432/dbname`

后端会在执行前对每个数据源执行 `SELECT 1` 进行连通性校验。

## 前端数据源配置与运行

- 运行弹窗新增「从 SQLite 载入」「保存数据源配置」「执行当前 DSL」按钮。
- 数据源配置会持久化到 SQLite 文件：`playground/data/playground.db`。
- 点击「执行当前 DSL」时会使用当前页面配置作为 `datasources` 请求参数。

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
