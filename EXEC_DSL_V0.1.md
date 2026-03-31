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

`steps[]` 节点支持两种 `type`：

- `sql`：与现有行为一致
- `variable`：步骤内变量计算节点

`type: sql` 结构：

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

`type: variable` 结构：

```json
{
  "name": "final_threshold",
  "type": "variable",
  "when": [
    {
      "condition": "$steps.exchange_rate.final_amount > $variables.threshold",
      "value": 900
    }
  ],
  "default": 1000
}
```

说明：

- `on_fail`、`on_pass` 都是**可选**。
- 不配置策略时，步骤只执行 SQL 并继续到下一个步骤。
- `variable.when/default` 语义与顶层 `variables` 一致。
- `variable.when` 可以引用已执行步骤输出与全局 `$variables`。
- `variable` 节点执行后通过 `$steps.<step_name>` 引用，例如 `$steps.final_threshold`。
- `on_fail` 支持消息模板渲染（`single/sub_repeat/full_repeat`）。
- `on_pass` 当前仅使用 `decision` 做短路成功判定。

## 4. 运行时作用域

- `$input`
- `$variables`
- `$steps`
- `$.<field>`（仅在当前 step 的 `on_fail`/`on_pass` 中可用）

> 不再支持 `$context`、`$prechecks`。

补充约束：

- `exists(...)` / `not exists(...)` 除了支持 `$steps.<step_name>.<output>`，也支持直接传入 `$steps.<step_name>`。
- 当 `<step_name>` 是 `result_mode: records` 的 SQL 步骤时，`$steps.<step_name>` 会按“对象数组”语义参与判断（即整行对象列表），不会先打平成单字段值数组。
- 在 step 级 `on_fail` / `on_pass` 中，`exists($.)` / `not exists($.)` 允许直接判断当前本地作用域整体（通常为当前步骤输出对象数组）。

## 5. 顶层 on_fail

顶层 `on_fail` 仅在步骤全部完成后判定，结构保持不变：

- `decision`
- `mode`
- `message_cn`
- `message_en`
- `divider` / `divider_cn` / `divider_en`（按模式可选）
