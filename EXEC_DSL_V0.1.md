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

硬规则：

- 顶层只允许以下五个字段：`context`、`variables`、`prechecks`、`steps`、`on_fail`
- 不允许出现未知顶层字段
- `steps` 必须存在且必须为数组
- `on_fail` 必须存在且必须为对象
- `steps` 可以为空数组，但 `on_fail` 不允许为空对象
- 顶层最小可执行结构必须满足本节“最小可执行 DSL 形态”

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
- 所有路径引用必须能在校验阶段静态解析，禁止悬空引用

命名硬规则：

- `prechecks[].name` 与 `steps[].name` 必须全局唯一
- 节点名称不得与保留作用域名冲突，例如：`input`、`context`、`variables`、`steps`、`on_fail`
- 所有对节点输出的引用都必须通过带命名空间的路径完成

## 4. outputs 语义

`outputs` 是“对外暴露字段白名单”，用于：

- 跨顶层块引用
- `consumes` 生成 CTE 列定义

规则：

- `context.outputs`（当 `context` 存在时）：可被 `variables`、`prechecks`、`steps`、`on_fail` 引用
- `steps[].outputs`：可被后续 `steps` 和顶层 `on_fail` 引用
- 不做全局字段提升
- 只允许带命名空间引用

硬规则：

- 若声明了 `context`，则必须声明 `outputs`
- 被 `consumes` 引用的节点必须声明 `outputs`
- 被表达式、`sql_params`、消息模板引用的字段，必须在对应节点的 `outputs` 白名单中
- `outputs` 必须为非空数组
- `outputs` 中的字段名不得重复

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
- CTE 参数绑定保持原始 Python 值（例如 `Decimal`），不做提前字符串化
- 在当前 SQL 中以 `alias` 作为临时表使用

当前 step 的 SQL 可以直接写：

```sql
FROM am
```

该设计支持跨数据源，由执行器统一控制中间数据流。

硬规则：

- `consumes[].from` 只能引用 `context` 或当前步骤之前已经执行完成的 `steps`
- 禁止前向引用、禁止自引用、禁止形成循环依赖
- `consumes[].alias` 在当前步骤内必须唯一
- `consumes[].alias` 必须是合法 SQL 标识符：仅允许字母、数字、下划线，且不能以数字开头
- CTE 列顺序由被消费节点的 `outputs` 顺序决定，不允许依赖数据库返回列顺序

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

硬规则：

- `context` 若存在，则必须同时声明 `type`、`datasource`、`result_mode`、`sql_template`、`sql_params`、`outputs`

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

硬规则：

- `variables` 中每个变量名必须唯一
- `when` 必须为数组
- `when` 中每一项必须同时包含 `condition` 与 `value`
- `default` 为必填
- `variables.when[].condition` 仅允许使用本规范第 7 节定义的受限表达式语法

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

硬规则：

- 每个 `precheck` 必须声明 `name`、`type`、`datasource`、`result_mode`、`sql_template`、`sql_params`、`on_fail`
- `prechecks[].on_fail.decision` 只允许 `exists` 或 `exists($path)`，不允许其它函数调用
- `prechecks[].on_fail.mode` 必须是 `single`、`sub_repeat`、`full_repeat` 之一

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

硬规则：

- 每个 `step` 必须声明 `name`、`type`、`datasource`、`result_mode`、`sql_template`、`sql_params`
- 若 `step` 被后续节点引用、被 `consumes` 消费、被表达式引用、或被消息模板引用，则必须声明 `outputs`

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

硬规则：

- 顶层 `on_fail` 必须声明 `decision`、`mode`、`message_cn`、`message_en`
- 顶层 `on_fail.mode` 当前固定为 `single`
- 顶层 `on_fail.decision` 不允许使用裸 `exists`，必须写成 `exists($path)`

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

硬规则：

- 表达式中只允许使用：比较运算 `==` `!=` `>` `>=` `<` `<=`，逻辑运算 `and` `or` `not`，集合运算 `in`，空值字面量 `null`，以及内置函数 `exists($path)`
- 除 `exists(...)` 外，禁止任意函数调用
- 禁止任意脚本执行能力与自定义 Python 逻辑
- 表达式中的所有路径必须能静态解析
- `exists` 语义固定如下：
  - 对 `records` 结果：非空结果集为真
  - 对数组路径：非空数组为真
  - 对标量路径：非 `null` 为真
  - 对不存在路径：视为校验失败，不允许隐式按 `false` 处理

## 8. 消息渲染规则

支持两类占位符：

- 行级字段：`{field}`
- 全局路径：`{$variables.threshold}`、`{$steps.exchange_rate.final_amount}`

支持格式化占位符（Python format 语法）：

- `f{$path:format_spec}`
- 示例：`f{$steps.a.out1:,}`（千分位）、`f{$steps.a.out2:,.0f}`（千分位并保留 0 位小数）
- `format_spec` 遵循 Python `format` 迷你语言

### 8.1 sub_repeat

规则：

- `[]` 只是重复片段标记，最终输出不保留
- 仅对 `[]` 内模板逐行渲染
- 分隔符规则：
  - 若配置 `divider`，中英文都使用 `divider`
  - 若未配置 `divider`，则必须同时配置 `divider_cn` 与 `divider_en`
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

硬规则：

- `sub_repeat` 必须且只能出现一段 `[]`
- `single` 模式下不得引用数组路径
- `message_cn` 与 `message_en` 必须在校验期通过模板语法检查
- 模板中的所有 `{$path}` 必须能静态解析
- 行级字段占位符 `{field}` 只能用于具备行上下文的渲染场景

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

硬规则：

- `result_mode = record` 时，SQL 结果必须恰好返回 1 行；返回 0 行或多于 1 行都视为运行时错误
- `result_mode = records` 时，SQL 结果允许返回 0 到 N 行
- `record` 节点的导出字段在运行时固定为标量
- `records` 节点的导出字段在运行时固定为按列暴露的数组，且所有导出字段数组长度必须一致

## 10. 标准执行结果

成功：

```json
{
  "passed": true,
  "phase": "pass",
  "failed_node": null,
  "error_code": null,
  "error_detail": null,
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
  "error_code": null,
  "error_detail": null,
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
  "error_code": null,
  "error_detail": null,
  "message_cn": "金额1000超过阈值800",
  "message_en": "The amount 1000 exceeds the threshold 800."
}
```

运行时失败：

```json
{
  "passed": false,
  "phase": "runtime",
  "failed_node": "exchange_rate",
  "error_code": "E2007_SQL_EXECUTION_FAILED",
  "error_detail": "SQL node execution failed: exchange_rate",
  "message_cn": "SQL node execution failed: exchange_rate",
  "message_en": "SQL node execution failed: exchange_rate"
}
```

补充说明：

- `pass` / `precheck` / `final` 三类结果中，`error_code` 与 `error_detail` 固定为 `null`
- `runtime` 结果表示执行器、SQL、引用解析、消息渲染等执行期错误
- `runtime.failed_node` 应尽量标识出错节点，例如：`context`、具体 `step.name`、具体 `precheck.name`、`variables.<name>`、`on_fail`

## 11. 建议的 v0.1 边界

为保证第一版尽快落地，建议先收窄能力边界：

- 只支持只读 SQL
- `context` 和 `steps` 当前只支持 `type: sql`
- `variables` 当前只支持赋值语义：`when` 有条件分支；`when` 为空表示常量（取 `default`）
- `prechecks.on_fail.decision` 支持兼容关键字 `exists`，并支持 `exists($path)`
- 顶层 `on_fail.decision` 支持表达式与 `exists($path)`（不支持裸 `exists`）
- 不支持循环、自定义脚本与除 `exists(...)` 外的函数调用
- 不支持无作用域的字段引用

硬规则：

- `sql_template` 只允许只读 SQL，即 `SELECT` 或 `WITH ... SELECT`
- 禁止 `INSERT`、`UPDATE`、`DELETE`、DDL 与多语句 SQL
- 执行器不得自动将只读节点改写为写操作

## 12. 引用合法矩阵

为避免解析器、校验器与执行器对“哪些位置可以引用哪些作用域”产生歧义，`v0.1` 固定采用以下引用合法矩阵。

### 12.1 引用矩阵

| 使用位置 | 允许引用的作用域 |
| --- | --- |
| `context.sql_params` | `$input` |
| `variables.when[].condition` | `$input`、`$context`、已完成求值的 `$variables` |
| `variables.when[].value` / `variables.default` | JSON 标量、数组、对象字面量；若实现支持路径引用，则仅允许 `$input`、`$context`、已完成求值的 `$variables` |
| `prechecks[].sql_params` | `$input`、`$context`、`$variables` |
| `prechecks[].on_fail.decision` | 裸 `exists`；或 `$input`、`$context`、`$variables`、当前 `precheck` 结果路径上的 `exists($path)` |
| `prechecks[].on_fail.message_*` | 当前 `precheck` 行级字段、`$input`、`$context`、`$variables` |
| `steps[].sql_params` | `$input`、`$context`、`$variables`、前序 `steps.outputs` |
| `steps[].consumes[].from` | `$context`、前序 `steps` |
| 顶层 `on_fail.decision` | `$input`、`$context`、`$variables`、全部 `steps.outputs` |
| 顶层 `on_fail.message_*` | `$input`、`$context`、`$variables`、全部 `steps.outputs` |

### 12.2 补充硬规则

- `variables` 按声明顺序求值；后声明变量可以引用先声明变量，反之不允许
- `prechecks` 之间不建立运行时结果作用域；后续 `precheck` 不允许直接引用前一个 `precheck` 的 SQL 结果
- `steps` 之间的字段引用必须通过显式命名空间路径完成；若当前步骤需要在 SQL 中使用前序步骤结果，优先通过 `consumes` 声明数据依赖
- 顶层 `on_fail` 不允许直接引用 `prechecks` 的结果

## 13. 建议的错误码

为提升执行结果可观测性，建议在 `v0.1` 内部实现中区分“校验错误码”和“运行时错误码”。以下错误码可作为推荐保留字。

### 13.1 校验错误码（建议）

| 错误码 | 含义 |
| --- | --- |
| `E1001_UNKNOWN_TOP_LEVEL_FIELD` | 出现未知顶层字段 |
| `E1002_MISSING_REQUIRED_FIELD` | 缺少必填字段 |
| `E1003_INVALID_FIELD_TYPE` | 字段类型非法 |
| `E1004_DUPLICATE_NODE_NAME` | `precheck` 或 `step` 名称重复 |
| `E1005_RESERVED_NODE_NAME` | 节点名称使用保留作用域名 |
| `E1006_UNRESOLVED_PATH` | 路径无法静态解析 |
| `E1007_MISSING_OUTPUTS` | 被引用或被消费节点未声明 `outputs` |
| `E1008_INVALID_CONSUMES_REF` | `consumes` 引用了非法节点、前向节点或形成循环 |
| `E1009_INVALID_CONSUMES_ALIAS` | `consumes.alias` 非法或重复 |
| `E1010_INVALID_EXPRESSION` | 表达式使用了不支持的语法 |
| `E1011_INVALID_MESSAGE_TEMPLATE` | 消息模板语法不合法 |
| `E1012_INVALID_RESULT_MODE` | `result_mode` 非 `record` / `records` |
| `E1013_NON_READONLY_SQL` | `sql_template` 不是只读 SQL |

### 13.2 运行时错误码（建议）

| 错误码 | 含义 |
| --- | --- |
| `E2001_CONTEXT_RESULT_MISMATCH` | `context` 返回结果与 `result_mode` 不匹配 |
| `E2002_STEP_RESULT_MISMATCH` | `step` 返回结果与 `result_mode` 不匹配 |
| `E2003_OUTPUT_COLUMN_MISMATCH` | 实际返回列与 `outputs` 不一致 |
| `E2004_ARRAY_LENGTH_MISMATCH` | `records` 导出字段按列展开后数组长度不一致 |
| `E2005_SINGLE_MODE_MULTI_ROWS` | `single` 模式下出现多行结果 |
| `E2006_TEMPLATE_RENDER_FAILED` | 消息模板渲染失败 |
| `E2007_SQL_EXECUTION_FAILED` | SQL 执行报错 |
| `E2008_DATASOURCE_NOT_FOUND` | 数据源不存在或不可用 |

### 13.3 错误结果建议字段

若执行器要增强可观测性，建议在标准结果之外补充以下调试字段：

- `error_code`
- `error_detail`
- `executed_nodes`
- `decision_snapshot`

## 14. Validator 检查清单（建议实现顺序）

为便于后续实现 `parser -> validator -> executor` 的最小闭环，建议按以下顺序实现 validator。

### 14.1 结构校验

- 顶层字段集合是否合法
- `steps`、`on_fail` 是否存在且类型正确
- `context`、`variables`、`prechecks` 的类型是否正确
- 各节点必填字段是否齐全

### 14.2 命名与引用校验

- `prechecks[].name`、`steps[].name` 是否唯一
- 是否使用保留作用域名
- 所有路径是否能静态解析
- 各使用位置是否满足第 12 节引用合法矩阵

### 14.3 outputs / consumes 校验

- 被引用、被消费节点是否声明 `outputs`
- `outputs` 是否为空、是否重复
- `consumes.from` 是否只引用合法前序节点
- `consumes.alias` 是否合法且唯一

### 14.4 表达式与模板校验

- `decision` 与 `variables.when[].condition` 是否只使用受限表达式语法
- `exists` 的使用位置是否符合约束
- `message_cn` / `message_en` 模板语法是否合法
- `single` / `sub_repeat` / `full_repeat` 是否与结果形状兼容

### 14.5 SQL 与结果模式校验

- `sql_template` 是否为只读 SQL
- `result_mode` 是否属于 `record` / `records`
- `record` / `records` 与消息模式、导出字段形状的组合是否合法
