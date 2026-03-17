# ExecDSL Python 执行器架构设计 v0.1

本文档用于指导 `ExecDSL` Python 解析执行器的工程实现，范围聚焦于：

- 解析数据库中的 DSL 文本
- 校验 DSL 结构与引用
- 执行 SQL 节点
- 传递运行时数据
- 生成标准化执行结果

本文档不覆盖版本管理、规则配置表设计、权限平台或规则发布流程。

## 1. 设计目标

当前阶段的目标不是做一个完整工作流平台，而是构建一个稳定、可测试、可扩展的最小执行引擎。

设计目标如下：

1. 输入一段 JSON DSL 文本和运行时入参，得到标准执行结果。
2. 解析、校验、执行分层明确，便于调试和后续扩展。
3. 严格遵守当前 `ExecDSL v0.1` 约束，不擅自扩展 DSL 语义。
4. 保持 `SQL First`，尽量让业务逻辑停留在 DSL 与 SQL 层。
5. 支持多数据源执行与 `consumes -> CTE` 数据传递。

## 2. 非目标

当前版本不做以下能力：

- 规则版本管理
- 规则发布审批
- 写入型 SQL
- 异步调度
- 分布式执行
- 插件系统
- 可视化编排
- 自定义 Python 脚本执行

## 3. 总体分层

推荐采用以下分层：

```text
调用方
  -> Engine 入口层
    -> Parser 解析层
    -> Validator 校验层
    -> Runtime 运行时层
    -> SQL Executor 执行层
    -> Renderer 渲染层
    -> Result Builder 结果封装层
```

职责划分：

- `Engine`：统一入口，编排完整执行流程
- `Parser`：把 DSL 文本解析为内部对象
- `Validator`：做结构校验、引用校验、静态约束校验
- `Runtime`：维护运行期作用域和执行轨迹
- `SQL Executor`：执行 SQL、绑定参数、构建 CTE
- `Renderer`：渲染中英文失败消息
- `Result Builder`：封装标准返回结构

## 4. 推荐目录结构

建议的代码布局如下：

```text
src/check_engine/
  engine.py
  dsl/
    enums.py
    models.py
  parser/
    json_parser.py
  validator/
    structure_validator.py
    reference_validator.py
    sql_validator.py
  expression/
    evaluator.py
  runtime/
    state.py
  sql/
    executor.py
    cte_builder.py
    datasource.py
  renderer/
    message_renderer.py
  result/
    builder.py
tests/
  unit/
  integration/
playground/
references/
```

说明：

- `engine.py`：执行入口
- `dsl/models.py`：内部数据模型
- `parser/json_parser.py`：DSL 文本到模型对象的转换
- `validator/`：静态校验器
- `expression/evaluator.py`：条件表达式求值
- `runtime/state.py`：运行时上下文
- `sql/executor.py`：SQLAlchemy 执行封装
- `sql/cte_builder.py`：`consumes` 转 CTE
- `renderer/message_renderer.py`：失败消息渲染
- `result/builder.py`：统一返回值封装

## 5. 核心对象设计

### 5.1 DSL 模型

建议使用 `dataclasses` 建模，不额外引入依赖。

推荐对象：

- `DslDocument`
- `ContextNode`
- `VariableDefinition`
- `VariableCondition`
- `PrecheckNode`
- `StepNode`
- `ConsumeSpec`
- `FailPolicy`

示意：

```python
@dataclass
class DslDocument:
    context: "ContextNode | None"
    steps: list["StepNode"]
    on_fail: "FailPolicy"
    variables: dict[str, "VariableDefinition"] = field(default_factory=dict)
    prechecks: list["PrecheckNode"] = field(default_factory=list)
```

关键原则：

- 模型对象只表达 DSL 结构，不做执行逻辑
- 原始 DSL 文本和模型对象建议同时保留，便于调试

### 5.2 运行时模型

推荐对象：

- `ExecutionState`
- `NodeExecutionResult`
- `ExecutionResult`

建议字段：

```python
@dataclass
class ExecutionState:
    input_data: dict[str, Any]
    context_data: dict[str, Any]
    variables_data: dict[str, Any]
    step_data: dict[str, dict[str, Any]]
```

建议同时保留两类结果：

- 节点原始结果：用于调试和消息渲染
- 节点导出结果：用于 `$context` / `$steps` 引用

## 6. 对外接口设计

推荐保留一个简洁入口：

```python
class DslEngine:
    def execute(
        self,
        dsl_text: str,
        input_data: dict[str, Any],
        datasource_registry: "DatasourceRegistry",
    ) -> "ExecutionResult":
        ...
```

配套接口：

```python
class DatasourceRegistry(Protocol):
    def get_engine(self, name: str) -> Engine:
        ...
```

设计考虑：

- 执行器只依赖“数据源注册表”，不耦合配置中心
- 调用方负责提供 DSL 文本和运行时输入
- 执行器本身不关心 DSL 从数据库哪张表读出来

## 7. 执行流程设计

推荐完整流程如下：

1. 解析 DSL 文本为 `DslDocument`
2. 执行结构校验
3. 执行引用校验
4. 执行 SQL 安全校验
5. 初始化 `ExecutionState`
6. 若存在 `context`，执行 `context`
7. 若存在 `variables`，计算 `variables`
8. 若存在 `prechecks`，顺序执行 `prechecks`
9. 如有失败，立即渲染消息并返回
10. 顺序执行 `steps`
11. 求值顶层 `on_fail.decision`
12. 命中则渲染失败消息并返回
13. 否则返回 `pass`

伪代码：

```python
def execute(dsl_text, input_data, datasource_registry):
    dsl = parser.parse(dsl_text)
    validator.validate(dsl)

    state = ExecutionState.new(input_data=input_data)

    if dsl.context is not None:
        state.context_data = execute_context(dsl.context, state, datasource_registry)
    state.variables_data = evaluate_variables(dsl.variables, state)

    for precheck in dsl.prechecks:
        result = execute_sql_node(precheck, state, datasource_registry)
        if should_fail_precheck(precheck, result, state):
            return build_precheck_failure(precheck, result, state)

    for step in dsl.steps:
        result = execute_sql_node(step, state, datasource_registry)
        state.step_data[step.name] = project_outputs(step, result)

    if evaluate_expression(dsl.on_fail.decision, state):
        return build_final_failure(dsl.on_fail, state)

    return build_pass_result(state)
```

## 8. 解析器设计

`json_parser.py` 建议只做三件事：

1. `json.loads`
2. 必填顶层块检查（当前仅 `steps`、`on_fail`）
3. 转换为内部 `dataclass` 模型（可选块补默认值）

不建议在解析器里做过多业务校验，避免职责混乱。

解析阶段建议报错的情况：

- JSON 非法
- 必填顶层块缺失
- 顶层块类型错误，例如 `steps` 不是数组
- 明显必填字段缺失，例如 `step.name` 不存在

## 9. 校验器设计

推荐拆成三类校验器。

### 9.1 结构校验

负责检查：

- 必填顶层块齐全（`steps`、`on_fail`）
- 节点字段类型合法
- `name` 唯一
- `result_mode` 合法
- `mode` 合法
- `sub_repeat` 必须有且仅有一段 `[]`

### 9.2 引用校验

负责检查：

- `$input.xxx` 格式是否合法
- `$context.xxx` 是否在 `context.outputs` 中
- `$variables.xxx` 是否已声明
- `$steps.step_name.field` 是否引用了已存在步骤与已导出字段
- `consumes.from` 是否只引用 `context` 或已存在步骤

### 9.3 SQL 安全校验

第一版建议仅允许只读 SQL。

建议检查：

- 禁止 `insert/update/delete/merge`
- 禁止 DDL
- 禁止多语句执行

由于当前不引入额外依赖，建议用“轻量词法扫描 + 保守拒绝”策略，而不是复杂 SQL 解析器。

## 10. 表达式求值设计

`variables.when[].condition` 和顶层 `on_fail.decision` 需要表达式求值。

推荐方案：

1. 先解析出 DSL 路径引用，例如 `$context.flow`
2. 把引用替换成受控的临时变量名
3. 使用 Python 标准库 `ast` 解析表达式
4. 只允许白名单节点
5. 在受控环境中求值

允许的表达式能力：

- 比较：`== != > >= < <=`
- 逻辑：`and or not`
- 集合：`in`
- 常量：字符串、数字、布尔、`null`

不允许：

- 除 `exists(...)` 外的函数调用
- 属性写入
- 下标赋值
- 任意 Python 代码

## 11. SQL 执行器设计

SQL 执行器建议围绕 SQLAlchemy `Engine` 和 `Connection` 实现。

推荐职责：

1. 根据 `datasource` 从 `DatasourceRegistry` 取连接
2. 解析 `sql_params`
3. 解析 `consumes`
4. 生成最终 SQL
5. 执行 SQL
6. 将结果转换为 `dict` 或 `list[dict]`
7. 根据 `result_mode` 做结果形态校验

建议每次 DSL 执行内，为每个 `datasource` 惰性创建并复用一个只读连接。

## 12. consumes -> CTE 设计

### 12.1 基本语义

- `consumes` 表示当前 SQL 节点依赖前序节点结果
- 被依赖结果由执行器读入内存
- 执行器在当前 SQL 前拼接 CTE
- 当前 SQL 通过 `alias` 访问该中间结果

### 12.2 生成策略

推荐先采用 `VALUES` 形式构造 CTE：

```sql
WITH am(func, total_amount) AS (
  VALUES
    (:__cte_am_0_func, :__cte_am_0_total_amount),
    (:__cte_am_1_func, :__cte_am_1_total_amount)
)
SELECT ...
FROM am
```

优点：

- 简单直接
- 易于参数绑定
- 易于跨数据源传递中间结果
- 参数值可保持原始 Python 对象（如 `Decimal`），避免提前字符串化造成精度或展示形式变化

### 12.3 当前风险

当前 DSL 中 `outputs` 只有字段名，没有字段类型。

这会带来一个实现风险：

- 当被 `consumes` 的结果集为空时，空 CTE 的列类型不容易稳定推断

因此建议：

- `v0.1` 先支持非空结果的 `VALUES CTE`
- 对空结果场景做显式处理并保守报错，或由业务 SQL 先规避空结果
- 后续若需要稳定支持空结果，建议为 `outputs` 增加类型元信息

这是当前架构中的一个已知边界，不建议在实现时偷偷绕过。

## 13. outputs 投影设计

节点执行结果建议分成两个层面：

1. 原始结果
2. 导出结果

示例：

- `raw_result`：SQL 返回的全部字段
- `projected_result`：只保留 `outputs` 中声明的字段

运行时引用只面向 `projected_result`，这样有几个好处：

- 行为更可预测
- 不会因为 SQL 临时字段变化影响 DSL 语义
- 有利于 `consumes` 和表达式引用统一

## 14. 消息渲染器设计

消息渲染器负责处理：

- `{field}` 行级占位符
- `{$path}` 全局路径占位符
- `f{$path:format_spec}` 格式化占位符
- `sub_repeat`
- `full_repeat`
- `single`

推荐设计：

- 先解析模板
- 再按模式选择渲染策略
- 最后统一做全局路径替换

模式实现建议：

- `sub_repeat`：识别并提取唯一的 `[]` 片段
  - 分隔符优先使用 `divider`
  - 若未配置 `divider`，则要求同时配置 `divider_cn` 与 `divider_en`
- `full_repeat`：整条模板逐行渲染后拼接
- `single`：只渲染一次

## 15. 标准返回结构设计

推荐统一为：

```json
{
  "passed": true,
  "phase": "pass",
  "failed_node": null,
  "message_cn": null,
  "message_en": null
}
```

失败时：

- `phase = "precheck"` 或 `phase = "final"`
- `failed_node` 填具体节点名或 `on_fail`
- `message_cn` / `message_en` 填渲染后的消息

## 16. 错误模型设计

建议区分三类错误：

### 16.1 DSLParseError

用于 JSON 语法错误和基础结构错误。

### 16.2 DSLValidationError

用于 DSL 静态规则不满足，例如：

- 引用不存在
- `outputs` 缺失
- `mode` 非法
- SQL 不是只读

### 16.3 DSLExecutionError

用于运行时错误，例如：

- 数据源不存在
- SQL 执行失败
- `record` 返回多行
- `single` 渲染时结果多行
- `consumes` 的 CTE 构建失败

这样调用方可以区分：

- DSL 写错了
- DSL 合法但运行失败了
- 规则正常执行但业务上判定失败了

## 17. 测试设计

建议分三层：

### 17.1 单元测试

- JSON 解析
- 引用解析
- 表达式求值
- 消息渲染
- CTE SQL 生成

### 17.2 集成测试

- 基于 PostgreSQL 的 SQL 节点执行
- 多数据源执行
- `consumes` 跨步骤传递
- `prechecks` 短路失败
- 顶层 `on_fail` 判定

### 17.3 样例回归测试

- 以 [`references/example.json`](./references/example.json) 为基础
- 每新增 DSL 能力，都补充样例和期望输出

## 18. 建议实现顺序

推荐按照以下顺序落地：

1. 完成 `dsl/models.py`
2. 完成 `parser/json_parser.py`
3. 完成结构校验与引用校验
4. 完成表达式求值器
5. 完成消息渲染器
6. 完成 SQL 执行器
7. 完成 CTE 构造器
8. 完成 `DslEngine` 主流程
9. 补单元测试和集成测试

这样做的好处是：

- 先把 DSL 边界锁住
- 再补执行细节
- 能尽早开始做小范围验证

## 19. 当前建议先拍板的实现决策

在正式编码前，建议先按以下决策实现：

- 只支持只读 SQL
- 只支持 `record` 和 `records`
- `context` / `steps` 只支持 `type: sql`
- `variables` 只支持赋值语义（`when` 条件匹配；`when` 为空则取 `default`）
- `prechecks.on_fail.decision` 兼容 `exists` 并支持 `exists($path)`
- 顶层 `on_fail.decision` 支持 `exists($path)`（不支持裸 `exists`）
- `consumes` 先用 `VALUES CTE`
- 空结果 CTE 暂视为已知边界，保守处理

## 20. 与现有文档的关系

实现时应优先联合参考以下文件：

- [`AGENTS.md`](./AGENTS.md)
- [`EXEC_DSL_V0.1.md`](./EXEC_DSL_V0.1.md)
- [`references/example.json`](./references/example.json)

若架构设计与 DSL 规范冲突，以 DSL 规范为准；若规范本身不足以支撑实现，应先补文档，再改代码。


## 21. decision 语义统一约束（补充）

- `prechecks[].on_fail.decision`：支持兼容关键字 `exists`。
- 通用表达式：支持 `exists($path)`，用于判断路径值是否非空存在。
- 顶层 `on_fail.decision`：不支持裸 `exists`，必须写成 `exists($path)`。
- 安全约束：除 `exists(...)` 外，不支持其它函数调用。
