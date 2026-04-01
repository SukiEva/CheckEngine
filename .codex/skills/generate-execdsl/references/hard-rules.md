# ExecDSL 硬约束

本文件只记录当前仓库已经落地、且会真实影响生成正确性的规则。事实来源优先级保持与项目一致：

1. `src/check_engine/`
2. `tests/`
3. `references/example.json`
4. `EXEC_DSL.md`

## 顶层结构

- 顶层只允许 `variables`、`steps`、`on_fail`。
- `steps` 必填，且必须是列表。
- `on_fail` 必填，且必须是对象。
- 不要生成 `context`、`prechecks`、`on_pass` 顶层块。

## variables

- 顶层变量名必须匹配 `^[A-Za-z_]\\w*$`。
- `default` 必填。
- `when` 可为空数组。
- `when[].condition` 必须是非空字符串。
- 顶层变量条件只能引用 `$input.*` 与已定义的 `$variables.*`。
- 顶层变量条件不能引用 `steps`。

## steps

- step 名称必须唯一、合法，且不能使用 `input`、`context`、`variables`、`steps`、`on_fail`。
- 当前只支持 `type: "sql"` 和 `type: "variable"`。
- `variable` step 必须有 `default`，且不支持 `consumes`。
- `sql` step 必须有 `datasource`、`result_mode`、`sql_template`。
- `result_mode` 只支持 `record`、`records`。
- `sql_template` 只允许单条只读 `SELECT` 或 `WITH` SQL。

## outputs 与 consumes

- SQL step 只要会被字段级引用，或会被其他 step 的 `consumes` 引用，就必须显式声明 `outputs`。
- `consumes.from` 只能写成 `$steps.<step_name>`，不能写字段级路径。
- `consumes` 只能引用已经执行过的 SQL step。
- 被 `consumes` 引用的 step 必须显式声明 `outputs`。
- `consumes.alias` 必须是合法 SQL 标识符，且同一 step 内不能重复。

## 引用规则

- 允许的显式作用域只有：
  - `$input.<field>`
  - `$variables.<name>`
  - `$steps.<step_name>`
  - `$steps.<step_name>.<output>`
  - `$.`
  - `$.<field>`
- SQL step 的字段级引用必须写成 `$steps.<step_name>.<output>`。
- `variable` step 只能整体引用成 `$steps.<step_name>`。
- 在 step 级 `on_fail` / `on_pass` 里，可以使用当前 SQL step 的局部作用域 `$.`、`$.<field>`。
- 除 `exists($steps.<sql_step>)` / `not exists($steps.<sql_step>)` 外，不要把 SQL step 根路径直接用于比较表达式。

## 表达式与模板

- 只支持 `and`、`or`、`not`、比较运算、`in`、`not in`、常量、tuple/list、`exists(...)`。
- `exists` 与 `not exists` 不能裸写，必须带参数。
- `sub_repeat` 必须提供 `divider`，或者同时提供 `divider_cn` 和 `divider_en`。
- `sub_repeat` 模板里必须且只能有一对 `[]`。
- `single` 顶层消息不要引用 `records` SQL step 的数组输出。

## 生成时优先自检的高风险错误

- 对未来 step 的引用。
- 把 SQL step 写成 `$steps.some_sql` 后直接比较。
- 把 variable step 写成 `$steps.some_variable.field`。
- 在 `consumes` 中引用无 `outputs` 的 SQL step。
- 在消息模板里混用不合法的局部字段或全局路径。
- 在 `sub_repeat` 中遗漏 `[]`、多写 `[]` 或遗漏分隔符。
