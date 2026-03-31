# ExecDSL Python 执行器架构设计 v0.1（2026-03-31 重构版）

## 1. 当前生效边界

- 顶层仅保留：`variables`、`steps`、`on_fail`
- 移除独立 `context` 阶段
- 移除独立 `prechecks` 阶段
- `steps` 内通过可选 `on_fail` / `on_pass` 承担短路能力

## 2. 执行链路

1. 解析 DSL JSON
2. 结构校验 / 引用校验 / SQL 安全校验
3. 预编译表达式（变量条件、`steps[].on_fail`、`steps[].on_pass`、顶层 `on_fail`）
4. 运行：`$input -> variables -> steps -> top-level on_fail`

## 3. 模块职责

- `parser`：JSON -> `DslDocument`
- `validator`：结构、引用、SQL 安全
- `compiler`：表达式预编译
- `engine`：步骤调度、短路判定、最终判定
- `runtime`：状态与作用域解析
- `renderer`：失败消息渲染
- `sql`：SQL 执行与 consumes CTE 构造

## 4. 作用域约束

- 支持：`$input`、`$variables`、`$steps`、局部 `$.`
- 不支持：`$context`、`$prechecks`

## 5. 结果语义

- `steps[].on_fail` 命中：`phase = "step"`
- `steps[].on_pass` 命中：直接 `pass`
- 顶层 `on_fail` 命中：`phase = "final"`
- 运行时异常：`phase = "runtime"`
