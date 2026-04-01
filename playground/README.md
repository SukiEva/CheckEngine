# FastAPI Playground

这个子项目提供一个基于 FastAPI 的可视化 Playground，用来编辑、校验和运行当前版本的 ExecDSL。

当前已落地能力：

- 通过页面编排 `variables / steps / on_fail`
- 支持 `sql` step 与 `variable` step
- 支持为 step 配置 `on_fail` / `on_pass` 短路策略
- 通过真实 PostgreSQL 数据源运行 DSL
- 调用 `check-engine` 并展示标准 `ExecutionResult`
- 在浏览器本地缓存 DSL 画布、数据源配置和运行输入

当前 playground 不支持旧版顶层 `context / prechecks`；导入这类 DSL 时，前端校验会直接提示先收敛到当前模型。

## 目录结构

```text
playground/
  pyproject.toml
  README.md
  src/playground_app/
    __init__.py
    static/
    templates/
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

- `PLAYGROUND_HOST`：默认 `0.0.0.0`
- `PLAYGROUND_PORT`：默认 `5001`
- `PLAYGROUND_DEBUG`：默认 `false`

## 数据源配置要求

运行面板中的数据源配置当前仅支持 PostgreSQL：

- `postgresql://user:password@host:5432/dbname`
- `postgresql+psycopg2://user:password@host:5432/dbname`

后端会在执行前对每个数据源执行 `SELECT 1` 进行连通性校验。

## 前端本地存储

前端使用浏览器 `localStorage` 持久化以下内容：

- 流程图画布状态
- 数据源配置
- 运行输入参数

数据源配置不是保存在 SQLite 文件里，而是保存在当前浏览器本地。

数据源面板行为：

- 页面加载时会优先从本地浏览器读取数据源配置
- 若本地没有配置，会初始化默认数据源项
- 修改数据源名称或连接串后会自动保存到本地浏览器
- 点击运行时，页面会把当前数据源配置作为 `datasources` 请求参数提交给后端

DSL JSON 生成时，前端会先做一轮基础校验，重点包括：

- 顶层只允许 `variables / steps / on_fail`
- `outputs` 每项必须是合法标识符
- `divider` 优先于 `divider_cn / divider_en`
- `divider` / `divider_cn` / `divider_en` 不允许空字符串

## 接口说明

- `POST /api/validate-dsl`
- `POST /api/run-dsl`

### `POST /api/validate-dsl`

请求体示例：

```json
{
  "dsl_text": "{\"steps\": [], \"on_fail\": {\"decision\": \"false\", \"mode\": \"single\", \"message_cn\": \"x\", \"message_en\": \"y\"}}"
}
```

成功返回：

```json
{
  "ok": true
}
```

### `POST /api/run-dsl`

请求体示例：

```json
{
  "dsl_text": "{\"steps\": [], \"on_fail\": {\"decision\": \"false\", \"mode\": \"single\", \"message_cn\": \"x\", \"message_en\": \"y\"}}",
  "input_data": {
    "source_object_id": "DOC_1001"
  },
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

成功时返回：

```json
{
  "result": {
    "passed": true,
    "phase": "pass",
    "failed_node": null,
    "message_cn": null,
    "message_en": null,
    "error_message": null,
    "runtime_exception": false,
    "input": {},
    "variables": {},
    "steps": {},
    "executed_nodes": []
  }
}
```
