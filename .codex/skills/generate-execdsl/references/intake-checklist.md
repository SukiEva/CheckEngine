# 需求采集清单

按下面顺序补齐信息。缺一项就继续追问，不要直接输出 DSL。

## 1. 输入与上下文

- 输入字段有哪些。
- 每个输入字段的名字、类型、业务含义是什么。
- 哪些输入字段会进入 SQL 参数或表达式判断。

## 2. 顶层变量

- 是否需要顶层 `variables`。
- 每个变量的名字是什么。
- 每个变量的 `when` 条件有哪些。
- 每个变量的 `default` 是什么。

## 3. steps

逐个 step 收集下面信息：

- `name`
- `type`
- `description`
- 若是 `sql` step：
  - `datasource`
  - `result_mode`
  - `sql_template`
  - `sql_params`
  - `outputs`
  - `consumes`
- 若是 `variable` step：
  - `when`
  - `default`
- 是否需要 `on_fail`
- 是否需要 `on_pass`

## 4. 最终失败策略

- 顶层 `on_fail.decision`
- 顶层 `on_fail.mode`
- `message_cn`
- `message_en`
- 若 `mode=sub_repeat`，补 `divider` 或 `divider_cn` / `divider_en`

## 5. 追问模板

缺字段时，优先按组追问，不要一次只问一个碎片问题。可直接复用下面的结构：

1. 输入层：请补充输入字段名、哪些字段参与 SQL 参数、哪些字段参与表达式判断。
2. 变量层：请补充是否需要顶层变量；若需要，请给出变量名、每个条件分支和默认值。
3. Step 层：请按执行顺序给出每个 step 的名称、类型，以及 SQL 或变量计算细节。
4. 策略层：请补充每个 step 是否需要 `on_fail` / `on_pass`，以及顶层 `on_fail` 的中英文消息。

## 6. 输出前复核

输出 JSON 前逐项确认：

- 是否所有引用都指向已定义对象。
- 是否所有会被字段级引用的 SQL step 都声明了 `outputs`。
- 是否所有 `variable` step 都只被整体引用。
- 是否每个 `sub_repeat` 都具备合法模板与分隔符。
- 是否已经运行项目级校验脚本。
