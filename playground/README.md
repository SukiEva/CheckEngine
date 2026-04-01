# FastAPI Playground（独立子项目）

该子项目基于 `index.html` 提供可执行闭环：

- 通过流程图方式生成 DSL
- 配置真实 PostgreSQL 数据源连接
- 直接调用 `check-engine` 运行并查看标准结果
- Step 节点支持配置 `on_fail` / `on_pass` 短路策略（含 `not exists($path)` 决策）
- 顶层仅支持 `variables / steps / on_fail`；导入含 `context / prechecks` 的旧 DSL 时会直接提示收敛

## 目录结构

```text
playground/
  pyproject.toml
  README.md
  src/playground_app/
    __init__.py
    templates/index.html
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
- DSL JSON 生成时分隔符字段规则：
  - 若 `divider` 为非空字符串，优先仅输出 `divider`；
  - 仅当 `divider` 为空字符串时，才按非空值输出 `divider_cn` / `divider_en`；
  - `divider` / `divider_cn` / `divider_en` 均不允许空字符串（允许仅空格）。
- `outputs` 每项必须是合法 SQL 标识符：以字母或下划线开头，且仅包含字母、数字、下划线。

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
