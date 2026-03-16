"""ExecDSL JSON 解析器。"""

from __future__ import annotations

import json
from typing import Any, Optional

from check_engine.dsl.models import (
    ConsumeSpec,
    ContextNode,
    DslDocument,
    FailPolicy,
    PrecheckNode,
    StepNode,
    VariableCondition,
    VariableDefinition,
)
from check_engine.exceptions import DSLParseError


class JsonDslParser:
    """将 JSON DSL 文本解析为内部数据模型。"""

    REQUIRED_TOP_LEVEL_KEYS = ("context", "variables", "prechecks", "steps", "on_fail")

    def parse(self, dsl_text: str) -> DslDocument:
        if not isinstance(dsl_text, str):
            raise DSLParseError("DSL 文本必须是字符串。")

        try:
            data = json.loads(dsl_text)
        except json.JSONDecodeError as exc:
            raise DSLParseError(f"DSL JSON 解析失败: {exc.msg}") from exc

        if not isinstance(data, dict):
            raise DSLParseError("DSL 顶层必须是 JSON 对象。")

        missing = [key for key in self.REQUIRED_TOP_LEVEL_KEYS if key not in data]
        if missing:
            raise DSLParseError(f"DSL 缺少顶层块: {', '.join(missing)}")

        return DslDocument(
            context=self._parse_context(data["context"]),
            variables=self._parse_variables(data["variables"]),
            prechecks=self._parse_prechecks(data["prechecks"]),
            steps=self._parse_steps(data["steps"]),
            on_fail=self._parse_fail_policy(data["on_fail"], "on_fail"),
            raw=data,
        )

    def _parse_context(self, value: Any) -> ContextNode:
        mapping = self._expect_dict(value, "context")
        return ContextNode(
            type=self._expect_string(mapping.get("type"), "context.type"),
            datasource=self._expect_string(mapping.get("datasource"), "context.datasource"),
            result_mode=self._expect_string(mapping.get("result_mode"), "context.result_mode"),
            sql_template=self._expect_string(mapping.get("sql_template"), "context.sql_template"),
            sql_params=self._expect_dict(mapping.get("sql_params", {}), "context.sql_params"),
            outputs=self._parse_string_list(mapping.get("outputs", []), "context.outputs"),
            description=self._optional_string(mapping.get("description"), "context.description"),
        )

    def _parse_variables(self, value: Any) -> dict[str, VariableDefinition]:
        mapping = self._expect_dict(value, "variables")
        variables: dict[str, VariableDefinition] = {}

        for name, raw_definition in mapping.items():
            definition = self._expect_dict(raw_definition, f"variables.{name}")
            when_items = self._expect_list(definition.get("when", []), f"variables.{name}.when")
            variables[name] = VariableDefinition(
                type=self._expect_string(definition.get("type"), f"variables.{name}.type"),
                when=[
                    VariableCondition(
                        condition=self._expect_string(
                            self._expect_dict(item, f"variables.{name}.when[{index}]").get("condition"),
                            f"variables.{name}.when[{index}].condition",
                        ),
                        value=self._expect_dict(item, f"variables.{name}.when[{index}]").get("value"),
                    )
                    for index, item in enumerate(when_items)
                ],
                default=definition.get("default"),
            )

        return variables

    def _parse_prechecks(self, value: Any) -> list[PrecheckNode]:
        items = self._expect_list(value, "prechecks")
        nodes: list[PrecheckNode] = []
        for index, item in enumerate(items):
            mapping = self._expect_dict(item, f"prechecks[{index}]")
            nodes.append(
                PrecheckNode(
                    name=self._expect_string(mapping.get("name"), f"prechecks[{index}].name"),
                    description=self._optional_string(mapping.get("description"), f"prechecks[{index}].description"),
                    type=self._expect_string(mapping.get("type"), f"prechecks[{index}].type"),
                    datasource=self._expect_string(mapping.get("datasource"), f"prechecks[{index}].datasource"),
                    result_mode=self._expect_string(mapping.get("result_mode"), f"prechecks[{index}].result_mode"),
                    sql_template=self._expect_string(mapping.get("sql_template"), f"prechecks[{index}].sql_template"),
                    sql_params=self._expect_dict(mapping.get("sql_params", {}), f"prechecks[{index}].sql_params"),
                    outputs=self._parse_string_list(mapping.get("outputs", []), f"prechecks[{index}].outputs"),
                    on_fail=self._parse_fail_policy(mapping.get("on_fail"), f"prechecks[{index}].on_fail"),
                )
            )
        return nodes

    def _parse_steps(self, value: Any) -> list[StepNode]:
        items = self._expect_list(value, "steps")
        nodes: list[StepNode] = []
        for index, item in enumerate(items):
            mapping = self._expect_dict(item, f"steps[{index}]")
            raw_consumes = self._expect_list(mapping.get("consumes", []), f"steps[{index}].consumes")
            consumes = [
                ConsumeSpec(
                    from_path=self._expect_string(
                        self._expect_dict(raw_consume, f"steps[{index}].consumes[{consume_index}]").get("from"),
                        f"steps[{index}].consumes[{consume_index}].from",
                    ),
                    alias=self._expect_string(
                        self._expect_dict(raw_consume, f"steps[{index}].consumes[{consume_index}]").get("alias"),
                        f"steps[{index}].consumes[{consume_index}].alias",
                    ),
                )
                for consume_index, raw_consume in enumerate(raw_consumes)
            ]
            nodes.append(
                StepNode(
                    name=self._expect_string(mapping.get("name"), f"steps[{index}].name"),
                    description=self._optional_string(mapping.get("description"), f"steps[{index}].description"),
                    type=self._expect_string(mapping.get("type"), f"steps[{index}].type"),
                    datasource=self._expect_string(mapping.get("datasource"), f"steps[{index}].datasource"),
                    result_mode=self._expect_string(mapping.get("result_mode"), f"steps[{index}].result_mode"),
                    sql_template=self._expect_string(mapping.get("sql_template"), f"steps[{index}].sql_template"),
                    sql_params=self._expect_dict(mapping.get("sql_params", {}), f"steps[{index}].sql_params"),
                    outputs=self._parse_string_list(mapping.get("outputs", []), f"steps[{index}].outputs"),
                    consumes=consumes,
                )
            )
        return nodes

    def _parse_fail_policy(self, value: Any, path: str) -> FailPolicy:
        mapping = self._expect_dict(value, path)
        return FailPolicy(
            decision=self._expect_string(mapping.get("decision"), f"{path}.decision"),
            mode=self._expect_string(mapping.get("mode"), f"{path}.mode"),
            message_cn=self._expect_string(mapping.get("message_cn"), f"{path}.message_cn"),
            message_en=self._expect_string(mapping.get("message_en"), f"{path}.message_en"),
            divider=self._optional_string(mapping.get("divider"), f"{path}.divider"),
            divider_cn=self._optional_string(mapping.get("divider_cn"), f"{path}.divider_cn"),
            divider_en=self._optional_string(mapping.get("divider_en"), f"{path}.divider_en"),
        )

    def _parse_string_list(self, value: Any, path: str) -> list[str]:
        items = self._expect_list(value, path)
        return [self._expect_string(item, f"{path}[{index}]") for index, item in enumerate(items)]

    def _expect_dict(self, value: Any, path: str) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise DSLParseError(f"{path} 必须是对象。")
        return value

    def _expect_list(self, value: Any, path: str) -> list[Any]:
        if not isinstance(value, list):
            raise DSLParseError(f"{path} 必须是数组。")
        return value

    def _expect_string(self, value: Any, path: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise DSLParseError(f"{path} 必须是非空字符串。")
        return value

    def _optional_string(self, value: Any, path: str) -> Optional[str]:
        if value is None:
            return None
        return self._expect_string(value, path)
