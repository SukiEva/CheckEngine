# 回归提示词样例

仅在验证 skill 本身时读取本文件，不要在普通生成流程中默认加载。

## 样例 1：纯 SQL 校验

用户请求：

生成一个 DSL，校验某单据下是否存在汇率为空的分录。输入只有 `source_object_id`。需要一个 `sql` step，从 `saas_db` 查出 `func`、`txn`、`rate_date`，如果存在结果就 step 级失败，中文消息是“存在汇率为空的记录: [记录{func}-{txn}-{rate_date}]”，英文消息同步生成，模式用 `sub_repeat`。

期望：

- 只生成一个 `sql` step。
- `on_fail.decision` 使用局部作用域。
- 最终顶层 `on_fail` 保底返回不失败。

## 样例 2：包含 variable step

用户请求：

生成一个 DSL。先按输入 `flow` 和 `scenario` 算出顶层变量 `threshold`。再用 SQL 汇总金额，最后用一个 `variable` step 计算 `final_threshold`。若汇总金额超过最终阈值则顶层失败。

期望：

- 顶层变量和 `variable` step 都使用 `when/default`。
- 对 `variable` step 的引用只能是 `$steps.final_threshold`。
- 顶层消息需要同时引用金额与阈值。

## 样例 3：包含 consumes 链路

用户请求：

生成一个 DSL。先按币种汇总金额，再把前一个 SQL step 的结果作为 CTE `am` 提供给第二个 SQL step 做汇率换算。第二个 step 输出 `final_amount`，最后再结合阈值判断是否失败。

期望：

- 第一段 SQL step 显式声明 `outputs`。
- 第二段 SQL step 的 `consumes.from` 写成 `$steps.query_aggregate_amount`。
- 第二段 SQL step 的 `consumes.alias` 合法且唯一。

## 样例 4：信息不足

用户请求：

帮我写一个校验付款的 DSL。

期望：

- 不直接输出 JSON。
- 先追问输入字段、数据源、需要几个步骤、失败条件、中英文消息。
- 追问时按输入、step、策略分组，而不是零散提问。
