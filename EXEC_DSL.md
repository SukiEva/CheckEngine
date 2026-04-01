# ExecDSL 规范

本文档以当前仓库中的真实实现为准，覆盖 `src/check_engine`、测试用例和 `references/example.json` 已经落地的能力。

## 1. 顶层结构

当前只支持 3 个顶层字段：

- `variables`：可选，顶层变量定义
- `steps`：必填，顺序执行的步骤列表
- `on_fail`：必填，所有步骤执行完成后的最终失败判定

```json
{
  "variables": {},
  "steps": [],
  "on_fail": {
    "decision": "false",
    "mode": "single",
    "message_cn": "校验失败",
    "message_en": "Validation failed"
  }
}
```

当前实现中已经移除独立的 `context` / `prechecks` 顶层块。

## 2. 执行顺序

执行链路固定如下：

1. 解析 DSL JSON
2. 校验结构、引用和 SQL 安全性
3. 预编译表达式
4. 绑定 `$input`
5. 按声明顺序计算 `variables`
6. 按顺序执行 `steps`
7. 每个 step 执行后先判定 `on_fail`，再判定 `on_pass`
8. 若所有 steps 都未短路，再判定顶层 `on_fail`
9. 未命中失败时返回 `pass`

补充说明：

- `on_fail` 命中时立即失败返回，`phase = "step"` 或 `phase = "final"`。
- `on_pass` 命中时立即成功返回，`phase = "pass"`。
- 若运行期发生异常，返回 `phase = "runtime"`。
- 如果同一个 step 的 `on_fail` 和 `on_pass` 同时为真，`on_fail` 优先。

## 3. 顶层 variables

`variables` 是一个对象，键名为变量名，值为变量定义：

```json
{
  "variables": {
    "threshold": {
      "when": [
        {
          "condition": "$input.flow == 'flow1'",
          "value": 1000
        }
      ],
      "default": 500
    }
  }
}
```

规则如下：

- 变量名必须是合法标识符：`^[A-Za-z_]\\w*$`
- `default` 必填
- `when` 可为空数组
- `when[].condition` 必须是非空字符串
- `when[].value` 必填
- 顶层变量按声明顺序求值
- 顶层变量条件中可以引用：
  - `$input.<field>`
  - 已经在前面声明过的 `$variables.<name>`
- 顶层变量条件中不能引用 `steps`

## 4. steps

`steps` 是有序数组，当前内置 2 种 step 类型：

- `type: "sql"`
- `type: "variable"`

每个 step 都必须有唯一的 `name`，并且 `name` 不能使用以下保留字：

- `input`
- `context`
- `variables`
- `steps`
- `on_fail`

### 4.1 SQL Step

```json
{
  "name": "exchange_rate",
  "type": "sql",
  "datasource": "data_db",
  "result_mode": "record",
  "sql_template": "SELECT ...",
  "sql_params": {
    "source_object_id": "$input.source_object_id"
  },
  "outputs": ["final_amount"],
  "consumes": [
    {
      "from": "$steps.query_aggregate_amount",
      "alias": "am"
    }
  ],
  "on_fail": {
    "decision": "exists($.final_amount)",
    "mode": "single",
    "message_cn": "失败",
    "message_en": "Failed"
  },
  "on_pass": {
    "decision": "not exists($.final_amount)"
  }
}
```

字段说明：

- `datasource`：必填，运行期从 `DatasourceRegistry` 中按名称取数据源
- `result_mode`：必填，只支持 `record` / `records`
- `sql_template`：必填，只允许单条只读 `SELECT` / `WITH` SQL
- `sql_params`：可选，对象中以 `$` 开头的字符串会按运行时引用解析，其余值按字面量透传
- `outputs`：可选，导出字段白名单
- `consumes`：可选，仅 `sql` step 支持
- `on_fail`：可选，step 失败短路策略
- `on_pass`：可选，step 成功短路策略
- `description`：可选，仅用于说明，不参与执行

运行语义：

- `result_mode = "record"` 时，SQL 必须精确返回 1 行，否则运行时报错
- `result_mode = "records"` 时，返回 0..n 行
- `outputs` 为空时：
  - 若有结果行，运行时会按 SQL 返回列自动导出
  - 若无结果行，导出字段列表为空
- 但只要这个 SQL step 需要被其他表达式引用，或被 `consumes` 引用，仍然应该显式声明 `outputs`

### 4.2 Variable Step

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

规则如下：

- `when/default` 语义与顶层 `variables` 一致
- `default` 必填
- `consumes` 当前不支持出现在 `variable` step 上
- `variable` step 执行结果通过 `$steps.<step_name>` 引用
- `variable` step 没有导出字段概念，不支持 `$steps.<step_name>.<field>`

### 4.3 consumes

`consumes` 会把前序 SQL step 的导出结果转成当前 step 可用的 CTE：

```json
{
  "consumes": [
    {
      "from": "$steps.query_aggregate_amount",
      "alias": "am"
    }
  ]
}
```

约束如下：

- `from` 必须是 `$steps.<step_name>`，不能写成字段级路径
- 只能引用已经执行过的 step
- 被引用 step 必须是 `sql` step
- 被引用 step 必须显式声明 `outputs`
- `alias` 必须是合法 SQL 标识符，且同一 step 内不能重复
- 当来源结果为空时，执行器会生成一个 `WHERE 1=0` 的空 CTE，而不是跳过该别名

## 5. 引用作用域

当前支持的显式引用作用域如下：

- `$input.<field>`
- `$variables.<name>`
- `$steps.<step_name>`
- `$steps.<step_name>.<output>`
- `$.<field>`
- `$.`

约束如下：

- 不支持 `$context`、`$prechecks`
- SQL step 的字段引用必须写成 `$steps.<step_name>.<output>`
- SQL step 若未声明 `outputs`，不能被字段级引用，也不能被 `consumes` 引用
- `variable` step 只能用 `$steps.<step_name>` 引用整个值
- step 级 `on_fail` / `on_pass` 中：
  - 对当前 SQL step 可使用局部作用域 `$.<field>` 或 `$.`
  - 对当前 step 不能通过 `$steps.<current_step>` 回指
- 顶层 `on_fail` 可以引用全部已完成的 steps

`exists(...)` / `not exists(...)` 的补充规则：

- 支持传入 `$steps.<sql_step_name>.<output>`
- 也支持直接传入 `$steps.<sql_step_name>`
- 对 `records` SQL step 来说，`$steps.<step_name>` 按“对象数组”语义参与判断
- 对 step 级 SQL 策略来说，`exists($.)` / `not exists($.)` 允许直接判断当前局部结果

## 6. 表达式语义

当前表达式由 `ExpressionEvaluator` 执行，支持的能力如下：

- 布尔运算：`and`、`or`、`not`
- 比较：`==`、`!=`、`>`、`>=`、`<`、`<=`
- 集合比较：`in`、`not in`
- 常量：字符串、数字、布尔值、`null`
- 容器字面量：tuple、list
- 函数：`exists(...)`

约束如下：

- 不支持任意 Python 函数调用，只允许 `exists(...)`
- `exists` 不能裸写成 `exists`
- `not exists($path)` 在语法上等价于 `not exists($path)`
- `exists(...)` 的判断规则为：
  - `None` => `False`
  - 空集合 / 空列表 / 空字典 => `False`
  - 其他值 => `True`

## 7. 失败与成功策略

### 7.1 `on_fail`

顶层和 step 级 `on_fail` 结构一致：

```json
{
  "decision": "exists($.func)",
  "mode": "sub_repeat",
  "message_cn": "存在异常记录：[记录{func}]",
  "message_en": "Invalid records: [Record {func}]",
  "divider": "，"
}
```

字段说明：

- `decision`：必填，表达式
- `mode`：必填，只支持 `single` / `sub_repeat` / `full_repeat`
- `message_cn` / `message_en`：必填
- `divider` / `divider_cn` / `divider_en`：按模式可选

结构约束：

- `decision` 不能为空
- `exists` / `not exists` 不能裸写，必须带参数
- `divider`、`divider_cn`、`divider_en` 若提供，不能是空字符串 `""`
- `sub_repeat` 模式下：
  - 优先使用 `divider`
  - 若未提供 `divider`，则必须同时提供 `divider_cn` 与 `divider_en`
  - 模板中必须且只能出现一对 `[]`

### 7.2 `on_pass`

`on_pass` 只包含一个字段：

```json
{
  "decision": "not exists($.func)"
}
```

规则如下：

- 只做短路成功判定
- 不渲染消息
- 与 `on_fail` 一样支持 `exists(...)` / `not exists(...)` 表达式

## 8. 消息模板渲染

消息渲染由 `MessageRenderer` 负责，当前支持以下占位符：

- `{field}`：当前结果行字段
- `{$path}`：显式全局路径引用
- `{steps.some_step.some_output}`：模板中的隐式全局路径引用
- `f{...:format_spec}`：带格式化说明符的占位符

示例：

```text
金额{$steps.exchange_rate.final_amount}超过阈值{$variables.threshold}
金额f{$steps.exchange_rate.final_amount:,.0f}超过阈值f{$variables.threshold:,.0f}
存在异常记录：[记录{func}-{txn}]
```

三种渲染模式的语义如下：

- `single`
  - 渲染一次
  - 若传入结果行超过 1 行，运行时报错
  - 顶层 `single` 消息不允许引用 `records` SQL step 的数组输出
- `sub_repeat`
  - 只重复 `[]` 内的片段
  - 最终输出中不会保留 `[]`
  - 可基于 SQL 结果行重复，也可基于等长数组占位符重复
- `full_repeat`
  - 重复整条模板
  - 若同时引用多组数组占位符，它们长度必须一致
  - 若既有结果行又有数组占位符，它们长度也必须一致

分隔符规则：

- `full_repeat`
  - 中文默认 `；`
  - 英文默认空格 `" "`
- `sub_repeat`
  - 优先使用 `divider`
  - 否则分别使用 `divider_cn` / `divider_en`

## 9. 返回结果

执行器最终返回 `ExecutionResult`，对外可通过 `to_dict()` 得到稳定 JSON：

```json
{
  "passed": false,
  "phase": "final",
  "failed_node": "on_fail",
  "message_cn": "金额1300超过阈值900",
  "message_en": "The amount 1300 exceeds the threshold 900.",
  "error_message": null,
  "runtime_exception": false,
  "input": {
    "source_object_id": "DOC_1001"
  },
  "variables": {
    "threshold": 1000
  },
  "steps": {
    "exchange_rate": {
      "final_amount": 1300
    },
    "final_threshold": 900
  },
  "executed_nodes": []
}
```

字段语义：

- `passed`：是否通过
- `phase`：`pass` / `step` / `final` / `runtime`
- `failed_node`：失败节点名，顶层失败固定为 `on_fail`
- `message_cn` / `message_en`：业务失败消息
- `error_message`：运行时异常消息
- `runtime_exception`：是否为运行时异常
- `input` / `variables` / `steps`：执行快照
- `executed_nodes`：已执行节点轨迹

`executed_nodes` 中每项包含：

- `phase`
- `node_name`
- `datasource`
- `result_mode`
- `row_count`
- `executed_sql`

## 10. 事实来源

当文档、样例、实现不一致时，优先级按以下顺序处理：

1. `src/check_engine/` 当前实现
2. `tests/` 中的行为测试
3. `references/example.json`
4. 本文档
