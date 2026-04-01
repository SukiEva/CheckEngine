---
name: generate-execdsl
description: 将业务规则转换为符合当前 ExecDSL 实现约束的 JSON DSL，并结合项目内校验脚本完成生成后校验与修复。用于从自然语言、校验规则、SQL 检查需求生成 `variables`、`steps`、`on_fail` 结构，或需要判断候选 DSL 是否满足当前仓库 `check_engine` 解析、引用、消息模板与 SQL 安全规则的场景。
---

# Generate ExecDSL

## 快速流程

1. 先读取 `references/hard-rules.md`，只使用当前实现已经支持的 DSL 能力。
2. 再按 `references/intake-checklist.md` 收集缺失信息，不要跳过必填槽位。
3. 仅在信息足够完整时生成第一版 DSL。
4. 立刻运行 `scripts/validate_dsl.py` 做项目级校验。
5. 根据校验错误修复 DSL，最多循环 3 次。
6. 只有在校验通过后，才输出最终 JSON。

## 采集规则

- 先收集输入字段，再收集变量、步骤、短路策略和最终失败策略。
- 不要猜测缺失的 `datasource`、`sql_template`、`outputs`、`consumes`、中英文消息或引用路径。
- 若用户只给业务背景，先把需求拆成结构化问题，再继续。
- 若用户需求不足以确定 DSL，继续追问，不要先输出半成品 DSL。
- 若用户明确要求使用现有字段或 step 名称，优先复用用户命名；否则使用简洁、稳定、合法的标识符。

## 生成规则

- 只生成当前实现支持的顶层结构：`variables`、`steps`、`on_fail`。
- 只生成当前实现支持的 step 类型：`sql`、`variable`。
- 为每个 step 生成唯一且合法的 `name`，避免使用保留字。
- 为所有会被字段级引用或被 `consumes` 引用的 SQL step 显式声明 `outputs`。
- 在 `sql_params`、表达式、消息模板中，只使用当前实现支持的引用作用域与模板语法。
- 遇到以下模式时主动拦截并改写，不要交给校验器兜底：
  - 裸写 `exists` 或 `not exists`
  - 对 SQL step 使用 `$steps.<name>` 根路径比较
  - 对 `variable` step 使用 `$steps.<name>.<field>`
  - 引用未来 step
  - `consumes` 指向未声明 `outputs` 的 SQL step
  - `sub_repeat` 缺少合法分隔符或缺少且多于一对 `[]`

## 校验与修复

- 在当前仓库内优先运行：

```bash
uv run python .codex/skills/generate-execdsl/scripts/validate_dsl.py
```

- 优先通过标准输入传入候选 DSL JSON。
- 若校验结果 `valid=true`，直接输出最终 JSON。
- 若校验结果 `valid=false`，读取 `stage`、`error_type`、`message`，只修复与报错直接相关的内容，避免额外改写已正确部分。
- 若连续 3 次仍未通过，不要伪造最终 DSL；改为简要说明缺失信息或剩余校验错误，并继续追问。
- 若脚本不可用，再退回 `references/hard-rules.md` 做手工自检，并明确说明未执行项目级校验。
- 未来若项目把同一契约封装成 MCP `validate_dsl`，沿用同一返回结构，不要改变生成流程。

## 最终输出

- 校验通过后，只输出 DSL JSON 本体。
- 不要附加解释、标题、代码块围栏或额外注释。
- 若当前回合的目标是补齐信息而不是输出成品 DSL，只输出问题列表或错误摘要，不要混入半成品 JSON。

## 资源

- `references/hard-rules.md`
  - 汇总当前仓库实现已经落地的硬约束，生成前优先读取。
- `references/intake-checklist.md`
  - 提供结构化采集顺序和提问模板，在信息不足时按此补问。
- `references/regression-prompts.md`
  - 提供用于回归和前向验证的典型提示词样例，仅在验证 skill 行为时读取。
- `scripts/validate_dsl.py`
  - 使用项目内 `check_engine.DslEngine` 返回稳定 JSON 校验结果，供生成后自动修复使用。
