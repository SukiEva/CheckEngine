"""消息渲染器测试。"""

from __future__ import annotations

import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine.dsl.models import FailPolicy
from check_engine.exceptions import DSLExecutionError
from check_engine.renderer import MessageRenderer
from check_engine.runtime.state import ExecutionState


class MessageRendererTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.renderer = MessageRenderer()
        self.state = ExecutionState.new({"source_object_id": "HDR001"})
        self.state.variables_data = {"threshold": 1000}
        self.state.step_data = {"exchange_rate": {"final_amount": 1200}}

    def test_render_sub_repeat_without_brackets(self) -> None:
        policy = FailPolicy(
            decision="exists",
            mode="sub_repeat",
            divider=",",
            message_cn="存在异常记录: [记录{func}-{txn}]",
            message_en="Invalid records: [Record{func}-{txn}]",
        )
        rows = [{"func": "A", "txn": "1"}, {"func": "B", "txn": "2"}]

        message_cn, message_en = self.renderer.render(policy, self.state, rows)

        self.assertEqual(message_cn, "存在异常记录: 记录A-1,记录B-2")
        self.assertEqual(message_en, "Invalid records: RecordA-1,RecordB-2")

    def test_render_single_with_global_paths(self) -> None:
        policy = FailPolicy(
            decision="$steps.exchange_rate.final_amount > $variables.threshold",
            mode="single",
            message_cn="金额{$steps.exchange_rate.final_amount}超过阈值{$variables.threshold}",
            message_en="The amount {steps.exchange_rate.final_amount} exceeds {$variables.threshold}.",
        )

        message_cn, message_en = self.renderer.render(policy, self.state)

        self.assertEqual(message_cn, "金额1200超过阈值1000")
        self.assertEqual(message_en, "The amount 1200 exceeds 1000.")

    def test_render_sub_repeat_with_step_array_paths(self) -> None:
        self.state.step_data = {
            "a": {
                "out1": ["100", "200"],
                "out2": ["USD", "CNY"],
            }
        }
        policy = FailPolicy(
            decision="exists($steps.a.out1)",
            mode="sub_repeat",
            divider=",",
            message_cn="结果是：[{$steps.a.out1}-{$steps.a.out2}]",
            message_en="result: [{$steps.a.out1}-{$steps.a.out2}]",
        )

        message_cn, message_en = self.renderer.render(policy, self.state)

        self.assertEqual(message_cn, "结果是：100-USD,200-CNY")
        self.assertEqual(message_en, "result: 100-USD,200-CNY")

    def test_render_sub_repeat_with_tuple_array_paths(self) -> None:
        self.state.step_data = {
            "a": {
                "out1": ("100", "200"),
                "out2": ("USD", "CNY"),
            }
        }
        policy = FailPolicy(
            decision="exists($steps.a.out1)",
            mode="sub_repeat",
            divider=",",
            message_cn="结果是：[{$steps.a.out1}-{$steps.a.out2}]",
            message_en="result: [{$steps.a.out1}-{$steps.a.out2}]",
        )

        message_cn, message_en = self.renderer.render(policy, self.state)

        self.assertEqual(message_cn, "结果是：100-USD,200-CNY")
        self.assertEqual(message_en, "result: 100-USD,200-CNY")

    def test_render_sub_repeat_with_mismatched_step_array_lengths(self) -> None:
        self.state.step_data = {
            "a": {
                "out1": ["100", "200"],
                "out2": ["USD"],
            }
        }
        policy = FailPolicy(
            decision="exists($steps.a.out1)",
            mode="sub_repeat",
            divider=",",
            message_cn="结果是：[{$steps.a.out1}-{$steps.a.out2}]",
            message_en="result: [{$steps.a.out1}-{$steps.a.out2}]",
        )

        with self.assertRaisesRegex(DSLExecutionError, "same length") as ctx:
            self.renderer.render(policy, self.state)
        self.assertIn("same length", str(ctx.exception))

    def test_render_single_with_multiple_rows_returns_error(self) -> None:
        policy = FailPolicy(
            decision="exists",
            mode="single",
            message_cn="记录{func}",
            message_en="Record {func}",
        )
        rows = [{"func": "A"}, {"func": "B"}]

        with self.assertRaises(DSLExecutionError) as ctx:
            self.renderer.render(policy, self.state, rows)
        self.assertIn("single mode", str(ctx.exception))

    def test_render_sub_repeat_with_locale_specific_divider(self) -> None:
        policy = FailPolicy(
            decision="exists",
            mode="sub_repeat",
            divider=None,
            divider_cn="；",
            divider_en=" | ",
            message_cn="存在异常记录: [记录{func}-{txn}]",
            message_en="Invalid records: [Record{func}-{txn}]",
        )
        rows = [{"func": "A", "txn": "1"}, {"func": "B", "txn": "2"}]

        message_cn, message_en = self.renderer.render(policy, self.state, rows)

        self.assertEqual(message_cn, "存在异常记录: 记录A-1；记录B-2")
        self.assertEqual(message_en, "Invalid records: RecordA-1 | RecordB-2")

    def test_render_single_with_formatted_global_paths(self) -> None:
        self.state.step_data = {"exchange_rate": {"final_amount": 12345.67}}
        self.state.variables_data = {"threshold": 1000}
        policy = FailPolicy(
            decision="$steps.exchange_rate.final_amount > $variables.threshold",
            mode="single",
            message_cn="金额f{$steps.exchange_rate.final_amount:,.0f}超过阈值f{$variables.threshold:,.0f}",
            message_en="Amount f{$steps.exchange_rate.final_amount:,.2f} exceeds f{$variables.threshold:,.0f}.",
        )

        message_cn, message_en = self.renderer.render(policy, self.state)

        self.assertEqual(message_cn, "金额12,346超过阈值1,000")
        self.assertEqual(message_en, "Amount 12,345.67 exceeds 1,000.")

    def test_render_sub_repeat_with_formatted_array_paths(self) -> None:
        self.state.step_data = {
            "a": {
                "out1": [1200.2, 3000],
                "out2": [5.6, 7.0],
            }
        }
        policy = FailPolicy(
            decision="exists($steps.a.out1)",
            mode="sub_repeat",
            divider=" | ",
            message_cn="结果：[f{$steps.a.out1:,.0f}-f{$steps.a.out2:,.1f}]",
            message_en="result: [f{$steps.a.out1:,.0f}-f{$steps.a.out2:,.1f}]",
        )

        message_cn, message_en = self.renderer.render(policy, self.state)

        self.assertEqual(message_cn, "结果：1,200-5.6 | 3,000-7.0")
        self.assertEqual(message_en, "result: 1,200-5.6 | 3,000-7.0")



if __name__ == "__main__":
    unittest.main()
