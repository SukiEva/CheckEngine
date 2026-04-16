# ExecDSL Python 执行器架构设计

本文档描述当前仓库里已经实现的执行架构，而不是未来规划稿。

## 1. 总体分层

执行器当前分为以下几层：

- `engine.py`
  - 对外统一入口 `DslEngine`
  - 提供 `validate()` 和 `execute()` 两个公开方法
- `parser/`
  - 将 JSON 文本解析为 `DslDocument`
- `validator/`
  - 按顺序执行结构校验、引用校验、SQL 安全校验
- `compiler/`
  - 预编译表达式，构建 `CompiledDsl`
- `execution_pipeline.py`
  - 运行时主调度器
- `runtime/`
  - 维护执行状态、结果对象和引用解析
- `sql/`
  - SQL 执行、参数绑定、`consumes` CTE 构造
- `renderer/`
  - 失败消息模板渲染
- `step_registry.py`
  - step 类型注册与分发

## 2. 真实执行链路

### 2.1 `DslEngine.validate()` 执行顺序

1. 通过 `JsonDslParser` 解析 DSL 文本
2. 通过 `DslValidator` 做静态校验（结构、引用、SQL 安全）
3. 通过 `DslCompiler` 预编译表达式
4. 将编译结果写入编译缓存（预热缓存，供后续 execute 复用）

### 2.2 `DslEngine.execute()` 执行顺序

1. 检查编译缓存；缓存命中则直接跳到第 4 步
2. 通过 `JsonDslParser` 解析 DSL 文本
3. 通过 `DslCompiler` 预编译表达式（**不调用 DslValidator**）
4. 创建 `ExecutionPipeline`
5. 初始化 `ExecutionState`
6. 执行顶层 `variables`
7. 顺序执行 `steps`
8. 判定顶层 `on_fail`
9. 产出 `ExecutionResult`

对应的真实对象链路如下：

```text
validate():
  DSL JSON -> JsonDslParser -> DslDocument -> DslValidator -> DslCompiler -> CompiledDsl -> cache

execute():
  cache hit  -> CompiledDsl
  cache miss -> DSL JSON -> JsonDslParser -> DslCompiler -> CompiledDsl -> cache
  CompiledDsl -> ExecutionPipeline -> ExecutionResult
```

## 3. 关键模块职责

### 3.1 Parser

`parser/json_parser.py` 和 `parser/node_parsers.py` 负责：

- 解析顶层 `variables / steps / on_fail`
- 解析 `sql` / `variable` 两种 step
- 生成不可变风格的 dataclass 模型
- 保留原始 `raw` DSL，供结构校验使用

### 3.2 Validator

`DslValidator` 当前串联 3 个校验器：

- `StructureValidator`
  - 顶层字段白名单
  - step 名称唯一性
  - `record/records`、`single/sub_repeat/full_repeat` 等枚举值
  - `divider`、`[]` 模板片段等结构约束
- `ReferenceValidator`
  - `$input` / `$variables` / `$steps` / `$.` 的作用域合法性
  - 前向引用限制
  - `consumes` 引用限制
  - `single` 模式下的数组输出限制
- `SqlSafetyValidator`
  - 只允许单条只读 `SELECT` / `WITH`
  - 拦截 `INSERT / UPDATE / DELETE / CREATE / DROP ...`

### 3.3 Compiler

`compiler/dsl_compiler.py` 负责：

- 预编译顶层变量条件
- 预编译 step 级 `on_fail` / `on_pass`
- 预编译顶层 `on_fail`
- 预编译 `variable` step 的条件表达式

`sql` step 当前不做额外编译，`compiled_steps[step.name]` 为 `None`。

### 3.4 Execution Pipeline

`execution_pipeline.py` 负责运行态主流程：

- 先执行顶层变量
- 再执行 steps
- 记录已执行节点轨迹
- 在 step 级处理失败短路 / 成功短路
- 在顶层处理最终失败判定
- 将 `DSLExecutionError` 包装成标准 `runtime` 结果

### 3.5 Runtime

`runtime/state.py` 和 `runtime/reference_resolver.py` 负责：

- 维护 `input_data` / `variables_data` / `step_data`
- 冻结对外返回数据，避免调用方误改
- 解析运行时引用
- 管理 `executed_nodes`
- 输出统一的 `ExecutionResult`

运行时当前只支持以下作用域：

- `$input`
- `$variables`
- `$steps`
- 局部 `$.`

### 3.6 SQL

`sql/executor.py` 和 `sql/cte_builder.py` 负责：

- 运行时解析 `sql_params`
- 将 `consumes` 转成 CTE
- 合并到原始 SQL 的 `WITH` 子句
- 执行 SQLAlchemy `Session.execute(...)`
- 基于 `result_mode` 和 `outputs` 投影导出数据
- 记录最终执行 SQL 文本

### 3.7 Renderer

`renderer/` 负责：

- 解析模板占位符
- 渲染 `single` / `sub_repeat` / `full_repeat`
- 处理全局路径、局部路径、行级字段和 `f{...}` 格式化

## 4. Step 扩展机制

当前 step 类型不是写死在解析器或执行器里的，而是通过 `StepTypeRegistry` 注册：

- 内置 `sql`
- 内置 `variable`
- 允许外部注入自定义 step 类型

每个 step 类型定义包含 5 类能力：

- 解析
- 结构校验
- 引用校验
- 编译
- 执行

因此新增 step 类型时，不需要改动 `DslEngine` 主流程，只需要提供新的 `StepTypeDefinition`。

## 5. 编译缓存

`DslEngine` 内置基于 DSL 文本的编译缓存：

- 默认大小：`128`
- 可通过 `compile_cache_size` 调整
- 设为 `0` 时关闭缓存

缓存的对象是 `CompiledDsl`，目的是避免重复解析和重复编译表达式。

## 6. 对外 API

当前公开入口主要有两个：

```python
from check_engine import DslEngine, StaticDatasourceRegistry
```

### 6.1 `DslEngine.validate(dsl_text)`

用于显式校验 DSL，可在”保存规则”或”发布规则”前调用。校验成功后同时完成表达式预编译并写入编译缓存，供后续 `execute()` 调用直接复用，无需重复解析和编译。

### 6.2 `DslEngine.execute(dsl_text, input_data, datasource_registry)`

用于运行 DSL，并返回 `ExecutionResult`。

## 7. 结果语义

当前返回结果的 `phase` 定义如下：

- `pass`
  - 正常通过
  - 或 step 级 `on_pass` 命中后短路通过
- `step`
  - step 级 `on_fail` 命中
- `final`
  - 顶层 `on_fail` 命中
- `runtime`
  - 运行阶段出现异常

同时会返回：

- `failed_node`
- `message_cn` / `message_en`
- `error_message`
- `runtime_exception`
- `input / variables / steps`
- `executed_nodes`

## 8. 当前代码目录

当前实际目录更接近下面这个结构：

```text
src/check_engine/
  compiler/
  dsl/
  expression/
  parser/
  renderer/
  runtime/
  sql/
  validator/
  __init__.py
  engine.py
  execution_pipeline.py
  reference_parser.py
  step_registry.py
tests/
playground/
references/
```

这也是当前维护文档时应当参考的真实工程边界。
