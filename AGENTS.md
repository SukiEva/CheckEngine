# AGENTS.md

## 项目定位

- 本项目用于构建一个 `ExecDSL` 的 Python 解析执行器。
- 当前目标是让执行器能够读取数据库中保存的 DSL 文本，完成解析、校验、执行，并返回标准化执行结果。
- 本项目当前不负责 DSL 的版本管理、名称管理、配置表设计；这些由外部配置表负责。
- 总体链路为：`用户自然语言输入 -> Agent Skill markdown -> 结构化 DSL -> ExecDSL Python 执行引擎`。

## 当前阶段

- 当前已经具备最小可运行闭环：解析 -> 校验 -> 编译 -> 执行 -> 返回结果。
- 第一优先级是继续收敛 DSL 语义、执行顺序、引用规则、错误消息规则。
- 第二优先级是围绕现有实现补强可观测性、测试覆盖和文档清晰度。
- 暂不追求复杂编排、过度抽象、多数据库通用框架或脚本化扩展能力。

## 技术栈

- Python 3.9
- SQLAlchemy 2.x
- PostgreSQL
- `uv` 作为唯一推荐的依赖管理工具

## 依赖与环境约束

- 必须使用 `uv` 管理依赖、虚拟环境和运行命令。
- 如果需要新增任何依赖，必须先征求用户确认，不能直接添加。
- 在未明确要求前，不要自行引入 Celery、Redis、Pydantic、Lark、ANTLR 等额外依赖。
- 优先使用 Python 标准库和现有依赖完成 DSL 解析、表达式求值和执行编排。

## 产品与发布形态

- 本项目应作为一个可发布的 Python 依赖包来设计。
- 同时需要保留一个测试/验证项目，用于验证 DSL 样例和执行效果。
- 设计时优先考虑：
  - 作为库被外部调用时的稳定 API
  - DSL 执行结果的可观测性
  - 后续补充测试样例和回归验证的便利性

## 当前核心目标

围绕 `ExecDSL Python 解析执行器`，优先完成以下能力：

1. DSL 文本解析
2. DSL 结构 / 引用 / SQL 安全校验
3. 表达式预编译与编译缓存
4. 运行时引用解析
5. SQL 参数绑定
6. SQL 与 variable 两类 step 执行
7. 基于 CTE 的 `consumes` 数据传递
8. step 级 `on_fail` / `on_pass` 短路机制
9. 顶层失败判定、消息渲染与标准执行结果输出

## DSL 事实来源

当前 DSL 设计以以下文件为准：

- [EXEC_DSL.md](./EXEC_DSL.md)：当前 DSL 规范
- [example.json](./references/example.json)：当前 DSL 样例

若实现与样例、规范发生冲突，应优先：

1. 先核对 `src/check_engine/` 与 `tests/` 中的真实实现
2. 再核对 `references/example.json`
3. 最后核对 `EXEC_DSL.md`
4. 若仍有歧义，先与用户确认，再修改实现或文档

## ExecDSL 当前边界

当前 `ExecDSL` 顶层固定为：

- `variables`
- `steps`
- `on_fail`

当前执行顺序固定为：

1. 绑定 `$input`
2. 计算 `variables`
3. 顺序执行 `steps`
4. 每个 `step` 执行后先判定 `on_fail`
5. 若未失败，再判定 `on_pass`
6. 所有 `steps` 完成后，求值顶层 `on_fail.decision`
7. 命中则失败，否则返回 `pass`

当前第一版约束：

- 仅支持 JSON DSL 文本输入
- 顶层 `variables` 和 `type: variable` step 都使用 `when/default` 语义
- `steps` 当前支持 `type: sql` 与 `type: variable`
- `sql` step 仅支持单条只读 `SELECT/WITH`
- `consumes` 通过 CTE 实现
- step 级短路通过 `on_fail` / `on_pass` 完成，不再保留独立 `prechecks`
- 成功时直接返回 `pass`

## 关键设计原则

### SQL First

- 优先将业务逻辑表达为 SQL 节点，而不是 Python 硬编码分支。
- 能在 DSL 中表达的执行逻辑，不要提前下沉到 Python 业务逻辑中。
- 引擎负责执行 DSL，不负责偷偷替 DSL 补语义。

### 强约束优先

- 优先构建小而稳的 DSL 能力集合。
- 新能力必须先明确语义、引用范围、错误行为，再进入实现。
- 不要为了“以后可能会用到”提前引入复杂抽象。

### 解析与执行分层

- 解析器负责把 DSL 文本转换为结构化对象，并完成基础结构校验。
- 校验器负责引用检查、字段检查、执行前静态校验。
- 执行器负责节点调度、SQL 执行、结果传递、消息渲染。
- 不要把解析、校验、执行逻辑混在一个超大类中。

### 可观测性优先

- 执行结果应尽量保留可调试信息。
- 至少要能定位：
  - 哪个节点失败
  - 失败阶段是 `step`、`final` 还是 `runtime`
  - 对应中英文消息
  - 哪个 SQL 节点被执行过
  - 实际执行 SQL 与节点执行轨迹

## 引用与数据流原则

- 运行时作用域使用：
  - `$input`
  - `$variables`
  - `$steps`
- step 级 SQL 策略与消息模板允许使用局部作用域 `$.`
- 步骤输出必须使用带命名空间路径引用，例如：
  - `$steps.exchange_rate.final_amount`
- `variable` step 的结果使用 `$steps.<step_name>` 引用，例如：
  - `$steps.final_threshold`
- 不支持扁平引用，例如：
  - `$steps.final_amount`
- `outputs` 是对外暴露字段白名单，不做全局字段提升。
- 被字段级引用或被 `consumes` 引用的 SQL 节点，必须显式声明 `outputs`。

## step 短路规则

- `steps` 按顺序执行。
- 每个 `step` 可选配置 `on_fail` / `on_pass`。
- `on_fail` 命中即短路失败返回，`phase = "step"`。
- `on_pass` 命中即短路成功返回，`phase = "pass"`。
- 同一个 `step` 内，`on_fail` 判定优先于 `on_pass`。
- 若 step 级未短路，才会继续执行后续 steps。
- 顶层 `on_fail` 仅在所有 steps 都完成后才会判定。

## 消息渲染规则

- 支持 `{field}` 形式的行级占位符。
- 支持 `{$path}` 形式的全局路径占位符。
- 支持 `f{...:format_spec}` 形式的格式化占位符。
- `sub_repeat`：
  - `[]` 仅用于标识重复片段
  - 最终输出中不保留 `[]`
  - 使用 `divider` 拼接重复渲染内容
- `full_repeat`：
  - 重复整条模板
  - 中文默认使用 `；` 拼接
  - 英文默认使用空格拼接
- `single`：
  - 只渲染一次

## 工程实现建议

建议优先拆分为以下模块：

- DSL 解析模块
- DSL 校验模块
- DSL 编译模块
- 引用解析模块
- 表达式求值模块
- SQL 执行模块
- CTE 构造模块
- 消息渲染模块
- 标准结果封装模块

建议优先沿着现有目录继续演进，不要脱离当前代码状态另起一套抽象。

## 目录与代码组织

当前实际目录结构更接近：

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

说明：

- `src/check_engine/`：正式发布的库代码
- `compiler/`：表达式预编译与编译缓存
- `dsl/`：DSL 模型与协议定义
- `expression/`：表达式编译与求值
- `parser/`：DSL 解析
- `renderer/`：失败消息渲染
- `runtime/`：运行时状态、引用解析、结果封装
- `sql/`：SQL 执行与 CTE 构造
- `validator/`：DSL 校验
- `tests/`：单元测试与集成测试
- `playground/`：验证项目或样例运行环境
- `references/`：DSL 样例与参考数据

## 测试要求

- 新增 DSL 能力时，优先补 DSL 样例和校验用例。
- 新增执行能力时，至少补：
  - 解析测试
  - 校验测试
  - 执行路径测试
  - 失败消息测试
- 若某项能力依赖数据库行为，优先补可重复执行的集成测试。
- 没有样例或测试支撑时，不要贸然扩展 DSL 语义。

## 协作与输出要求

- 所有问答、解释、文档、注释均使用中文。
- 若需新增依赖，必须先询问用户。
- 修改 DSL 规范前，先确认是否会影响既有样例和执行语义。
- 进行代码改动前，应先阅读已有规范和样例，不要凭空假设 DSL 结构。
- 产出文档时，优先写清边界、约束、失败语义和示例。

## 编码规范补充

- 新增或修改类型注解时，若类型可能为空，统一使用 `Optional[...]`，不要使用 `X | None` 写法。
- 若某个类方法不依赖 `self` 或 `cls`，应显式声明为 `@staticmethod`，不要保留实例方法形式。
- 新增或修改类型注解时，只要能够明确确定具体类型，就不要使用宽泛的 `Any`，应优先写出准确类型。
- 若某个符号已经在包的 `__init__.py` 中显式导出，则优先从包级路径导入，不要继续从更深层的具体文件导入，避免后续重构时批量修改 import。
- Agent 在修改代码时，应主动遵守以上风格；若发现已有代码与该规范冲突，优先在本次改动范围内一并收敛。

## 静态检查与告警治理（强制）

- 任何代码改动后，必须主动消除当前改动范围内的静态检查告警，不得以“后续再处理”遗留。
- 重点避免并整改以下常见告警：
  - `Shadows name 'xxx' from outer scope`（内外层同名遮蔽）
  - `Parameter 'xxx' value is not used`（未使用参数）
  - `Access to a protected member _xxx of a class`（跨类访问受保护成员）
- 设计约束：
  - 避免使用依赖闭包变量重名的写法；必要时改为显式中间变量或 `partial` 绑定。
  - 对协议/接口要求但在实现中暂不使用的参数，统一使用 `_param` 命名以表达“有意未使用”。
  - 跨模块复用能力时，应优先提供公开方法，不得依赖调用方直接访问受保护成员。
- 提交前至少执行一次静态检查命令（如 `uv run pyright`）；若工具未覆盖上述告警，需在本次改动中通过代码结构规避对应风险。

## Git 约定

- 每次实际修改文件后，默认进行一次独立提交。
- 提交时尽量只提交本次修改的文件，不混入用户的其他草稿改动。
- 不要提交 `uv.lock`；若本地执行 `uv` 命令生成了该文件，应将其移出提交范围。
- 提交信息应简洁明确，优先使用英文动词开头的 conventional 风格，例如：
  - `docs: align ExecDSL docs with implementation`
  - `feat: add DSL validator skeleton`
  - `test: add step short-circuit rendering cases`

## 禁止事项

- 未经确认不得新增依赖。
- 不得绕过 DSL 直接把业务规则硬编码进执行器。
- 不得提前抽象成复杂的通用工作流平台。
- 不得在没有样例和测试支撑的情况下扩展 DSL 语义。
- 不得把版本、名称等配置管理职责混入执行器核心。
