# ExecDSL v0.1 规范草案

本文档基于当前样例 [`example.json`](./references/example.json) 整理，用于约束 `ExecDSL` 的解析与执行行为。

## 1. 顶层结构

`ExecDSL` 顶层包含以下块，其中 `steps` 与 `on_fail` 为必填，`context` / `variables` / `prechecks` 为可选：

```json
{
  "steps": [],
  "on_fail": {},
  "context": {},
  "variables": {},
  "prechecks": []
}
```

最小可执行 DSL 形态：

```json
{
  "steps": [],
  "on_fail": {
    "decision": "false",
    "mode": "single",
    "message_cn": "",
    "message_en": ""
  }
}
```

说明：

- `context`：执行前上下文获取
- `variables`：运行期变量计算
- `prechecks`：前置检查
- `steps`：主执行步骤
- `on_fail`：最终失败判定与失败消息

## 2. 执行顺序

执行顺序固定为：

1. 绑定 `$input`
2. 若存在 `context`，执行 `context`
3. 若存在 `variables`，计算 `variables`
4. 若存在 `prechecks`，顺序执行 `prechecks`
5. 若某个 `precheck` 失败，立即停止并返回失败
6. 顺序执行 `steps`
7. 计算顶层 `on_fail.decision`
8. 若命中则返回失败，否则返回 `pass`

补充规则：

- `prechecks` 是短路执行，失败一个就停止
- 顶层 `on_fail` 只在 `steps` 全部执行完成后参与判定
- 成功时不返回错误信息，直接视为 `pass`

## 3. 运行时作用域

运行时可引用的作用域如下：

- `$input`
- `$context`
- `$variables`
- `$steps`

标准引用形式：

- `$input.source_object_id`
- `$context.flow`
- `$variables.threshold`
- `$steps.exchange_rate.final_amount`

约束：

- 不支持扁平引用，例如 `$steps.final_amount`
- 步骤输出必须通过 `$steps.<step_name>.<field>` 访问

## 4. outputs 语义

`outputs` 是“对外暴露字段白名单”，用于：

- 跨顶层块引用
- `consumes` 生成 CTE 列定义

规则：

- `context.outputs`（当 `context` 存在时）：可被 `variables`、`prechecks`、`steps`、`on_fail` 引用
- `steps[].outputs`：可被后续 `steps` 和顶层 `on_fail` 引用
- 不做全局字段提升
- 只允许带命名空间引用

建议约束：

- 若声明了 `context`，则必须声明 `outputs`
- 被 `consumes` 引用的 `step` 必须声明 `outputs`
- 被表达式引用的 `step` 应声明 `outputs`

## 5. consumes 语义

`consumes` 固定通过 `CTE` 实现。

示例：

```json
"consumes": [
  {
    "from": "$steps.query_aggregate_amount",
    "alias": "am"
  }
]
```

执行语义：

- `from` 只能引用 `context` 或已执行完成的 `steps`
- 执行器读取被消费节点结果
- 按该节点的 `outputs` 生成 CTE 列
- 在当前 SQL 中以 `alias` 作为临时表使用

当前 step 的 SQL 可以直接写：

```sql
FROM am
```

该设计支持跨数据源，由执行器统一控制中间数据流。

## 6. 节点定义

### 6.1 context

`context` 当前按“特殊 SQL 节点”处理。

建议字段：

```json
{
  "type": "sql",
  "datasource": "saas_db",
  "result_mode": "record",
  "sql_template": "SELECT ...",
  "sql_params": {
    "source_object_id": "$input.source_object_id"
  },
  "outputs": ["flow", "scenario"]
}
```

字段说明：

- `type`：当前第一版固定为 `sql`
- `datasource`：数据源标识
- `result_mode`：结果模式
- `sql_template`：SQL 模板
- `sql_params`：SQL 参数映射
- `outputs`：导出字段

### 6.2 variables

`variables` 当前只支持赋值语义：按 `when` 条件顺序匹配；若 `when` 为空则视为常量。

示例：

```json
{
  "threshold": {
    "when": [
      {
        "condition": "$context.flow == 'flow1'",
        "value": 1000
      }
    ],
    "default": 500
  }
}
```

语义：

- 按顺序匹配 `when`
- 命中第一条即取其 `value`
- 若都不命中，取 `default`

常量示例（`when` 为空）：

```json
{
  "threshold": {
    "when": [],
    "default": 800
  }
}
```

常量语义：

- 不进行条件匹配
- 直接返回 `default`

### 6.3 prechecks[]

前置检查节点。

示例结构：

```json
{
  "name": "check_rate_null",
  "description": "检查是否存在汇率为空的记录",
  "type": "sql",
  "datasource": "saas_db",
  "result_mode": "records",
  "sql_template": "SELECT ...",
  "sql_params": {
    "source_object_id": "$input.source_object_id"
  },
  "on_fail": {
    "decision": "exists",
    "mode": "sub_repeat",
    "divider": ",",
    "message_cn": "存在汇率为空的记录: [记录{func}-{txn}-{rate_date}]",
    "message_en": "There are records with null exchange rates: [Record{func}-{txn}-{rate_date}]."
  }
}
```

语义：

- 每个 `precheck` 顺序执行
- 若 `on_fail.decision` 命中，则立即失败返回
- 不再继续执行后续 `prechecks`、`steps`、顶层 `on_fail`

### 6.4 steps[]

主执行步骤节点。

示例结构：

```json
{
  "name": "exchange_rate",
  "description": "换算美元汇率汇总金额",
  "type": "sql",
  "datasource": "data_db",
  "result_mode": "record",
  "consumes": [
    {
      "from": "$steps.query_aggregate_amount",
      "alias": "am"
    }
  ],
  "sql_template": "SELECT ...",
  "sql_params": {},
  "outputs": ["final_amount"]
}
```

语义：

- `steps` 按顺序执行
- 可通过 `consumes` 使用前序节点结果
- 导出字段供后续步骤和顶层 `on_fail` 使用

### 6.5 顶层 on_fail

最终失败判定节点。

示例结构：

```json
{
  "decision": "$steps.exchange_rate.final_amount > $variables.threshold",
  "mode": "single",
  "message_cn": "金额{$steps.exchange_rate.final_amount}超过阈值{$variables.threshold}",
  "message_en": "The amount {$steps.exchange_rate.final_amount} exceeds the threshold {$variables.threshold}."
}
```

语义：

- `steps` 全部执行完成后求值
- 若 `decision` 为真，则整条规则失败
- 若 `decision` 为假，则整条规则通过
- 顶层 `on_fail.decision` 允许使用 `exists($path)` 判断某个路径是否“非空存在”

## 7. decision 表达式规则

当前建议支持：

- 比较：`==` `!=` `>` `>=` `<` `<=`
- 逻辑：`and` `or` `not`
- 集合：`in`
- 空值：`null`

特例：

- `prechecks[].on_fail.decision` 支持关键字 `exists`（兼容语义：当前 SQL 有结果即失败）
- `prechecks[].on_fail.decision` 与顶层 `on_fail.decision` 都支持 `exists($path)`
- 顶层 `on_fail.decision` **不支持** 裸 `exists`，必须写成 `exists($path)`

建议约束：

- `v0.1` 仅支持内置函数 `exists(...)`
- `v0.1` 不支持其它函数调用
- `v0.1` 不支持复杂脚本表达式
- `v0.1` 不支持自定义 Python 逻辑

## 8. 消息渲染规则

支持两类占位符：

- 行级字段：`{field}`
- 全局路径：`{$variables.threshold}`、`{$steps.exchange_rate.final_amount}`

### 8.1 sub_repeat

规则：

- `[]` 只是重复片段标记，最终输出不保留
- 仅对 `[]` 内模板逐行渲染
- 使用 `divider` 连接
- `[]` 外文本保留一份
- 若 `[]` 内使用全局路径占位符（例如 `{$steps.a.out1}`）且该路径值为数组，则按数组下标逐项渲染
- 当 `[]` 内有多个数组占位符时，所有数组长度必须一致

示例：

```json
{
  "mode": "sub_repeat",
  "divider": ",",
  "message_cn": "存在汇率为空的记录: [记录{func}-{txn}-{rate_date}]"
}
```

若结果有两行，输出为：

```text
存在汇率为空的记录: 记录A-1-2024-01-01,记录B-2-2024-01-02
```

若使用步骤数组路径占位符：

```json
{
  "mode": "sub_repeat",
  "divider": ",",
  "message_cn": "结果是：[{$steps.a.out1}-{$steps.a.out2}]"
}
```

当 `$steps.a.out1=[100,200]` 且 `$steps.a.out2=["USD","CNY"]` 时，输出为：

```text
结果是：100-USD,200-CNY
```

建议约束：

- `sub_repeat` 必须且只能出现一段 `[]`

### 8.2 full_repeat

规则：

- 整条模板按结果集逐行渲染
- 中文默认用 `；` 拼接
- 英文默认用空格 `" "` 拼接

可选扩展字段：

- `divider_cn`
- `divider_en`

默认值：

- `divider_cn = "；"`
- `divider_en = " "`

### 8.3 single

规则：

- 只渲染一次
- 若结果集有多行，建议直接报运行时错误

## 9. 结果模式

当前第一版只支持：

- `record`：单行对象
- `records`：多行对象数组

建议约束：

- `record` 返回多行时报错
- `single` 配合多行 `records` 时，报运行时错误

## 10. 标准执行结果

成功：

```json
{
  "passed": true,
  "phase": "pass",
  "failed_node": null,
  "message_cn": null,
  "message_en": null
}
```

前置检查失败：

```json
{
  "passed": false,
  "phase": "precheck",
  "failed_node": "check_rate_null",
  "message_cn": "存在汇率为空的记录: ...",
  "message_en": "There are records with null exchange rates: ..."
}
```

最终业务失败：

```json
{
  "passed": false,
  "phase": "final",
  "failed_node": "on_fail",
  "message_cn": "金额1000超过阈值800",
  "message_en": "The amount 1000 exceeds the threshold 800."
}
```

## 11. 建议的 v0.1 边界

为保证第一版尽快落地，建议先收窄能力边界：

- 只支持只读 SQL
- `context` 和 `steps` 当前只支持 `type: sql`
- `variables` 当前只支持赋值语义：`when` 有条件分支；`when` 为空表示常量（取 `default`）
- `prechecks.on_fail.decision` 支持兼容关键字 `exists`，并支持 `exists($path)`
- 顶层 `on_fail.decision` 支持表达式与 `exists($path)`（不支持裸 `exists`）
- 不支持循环、自定义脚本与除 `exists(...)` 外的函数调用
- 不支持无作用域的字段引用
