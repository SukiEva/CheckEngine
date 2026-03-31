# ExecDSL v0.1 规范草案（2026-03-31 重构版）

## 1. 顶层结构

当前生效的顶层字段仅为：

- `variables`（可选）
- `steps`（必填）
- `on_fail`（必填）

```json
{
  "variables": {},
  "steps": [],
  "on_fail": {}
}
```

> 已移除：`context`、`prechecks`。

## 2. 执行顺序

1. 绑定 `$input`
2. 计算 `variables`
3. 顺序执行 `steps`
   - 每个 `step` 可选声明 `on_fail`
   - 每个 `step` 可选声明 `on_pass`
   - 命中 `on_fail` 立即失败返回
   - 命中 `on_pass` 立即成功返回
4. 所有 `steps` 完成后，计算顶层 `on_fail.decision`
5. 命中则失败，否则返回 `pass`

## 3. steps 说明

`steps[]` 节点结构：

```json
{
  "name": "step_name",
  "type": "sql",
  "datasource": "saas_db",
  "result_mode": "record | records",
  "sql_template": "SELECT ...",
  "sql_params": {},
  "outputs": [],
  "consumes": [],
  "on_fail": {},
  "on_pass": {}
}
```

说明：

- `on_fail`、`on_pass` 都是**可选**。
- 不配置策略时，步骤只执行 SQL 并继续到下一个步骤。
- `on_fail` 支持消息模板渲染（`single/sub_repeat/full_repeat`）。
- `on_pass` 当前仅使用 `decision` 做短路成功判定。

## 4. 运行时作用域

- `$input`
- `$variables`
- `$steps`
- `$.<field>`（仅在当前 step 的 `on_fail`/`on_pass` 中可用）

> 不再支持 `$context`、`$prechecks`。

## 5. 顶层 on_fail

顶层 `on_fail` 仅在步骤全部完成后判定，结构保持不变：

- `decision`
- `mode`
- `message_cn`
- `message_en`
- `divider` / `divider_cn` / `divider_en`（按模式可选）
