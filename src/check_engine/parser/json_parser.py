"""ExecDSL JSON 解析器。"""

from __future__ import annotations

import json
from enum import Enum
from typing import Any, Mapping, Optional, Sequence

from ..dsl import ContextNode, DslDocument, FailPolicy, PrecheckNode, StepNode, VariableDefinition
from ..exceptions import DSLParseError
from .node_parsers import JsonNodeParser, ParserHelpers


class DslField(str, Enum):
    """DSL 字段常量。"""

    CONTEXT = "context"
    VARIABLES = "variables"
    PRECHECKS = "prechecks"
    STEPS = "steps"
    ON_FAIL = "on_fail"

    TYPE = "type"
    DATASOURCE = "datasource"
    RESULT_MODE = "result_mode"
    SQL_TEMPLATE = "sql_template"
    SQL_PARAMS = "sql_params"
    OUTPUTS = "outputs"
    DESCRIPTION = "description"

    WHEN = "when"
    CONDITION = "condition"
    VALUE = "value"
    DEFAULT = "default"
    NAME = "name"

    CONSUMES = "consumes"
    FROM = "from"
    ALIAS = "alias"

    DECISION = "decision"
    MODE = "mode"
    MESSAGE_CN = "message_cn"
    MESSAGE_EN = "message_en"
    DIVIDER = "divider"
    DIVIDER_CN = "divider_cn"
    DIVIDER_EN = "divider_en"


class JsonDslParser:
    """将 JSON DSL 文本解析为内部数据模型。"""

    REQUIRED_TOP_LEVEL_KEYS = (
        DslField.STEPS.value,
        DslField.ON_FAIL.value,
    )

    def __init__(self) -> None:
        self.node_parser = JsonNodeParser(
            helpers=ParserHelpers(
                expect_dict=self._expect_dict,
                expect_list=self._expect_list,
                expect_string=self._expect_string,
                optional_string=self._optional_string,
                parse_string_list=self._parse_string_list,
            )
        )

    def parse(self, dsl_text: str) -> DslDocument:
        if not isinstance(dsl_text, str):
            raise DSLParseError("DSL text must be a string.")

        try:
            data = json.loads(dsl_text)
        except json.JSONDecodeError as exc:
            raise DSLParseError(f"Failed to parse DSL JSON: {exc.msg}") from exc

        if not isinstance(data, dict):
            raise DSLParseError("Top-level DSL must be a JSON object.")

        missing = [key for key in self.REQUIRED_TOP_LEVEL_KEYS if key not in data]
        if missing:
            raise DSLParseError(f"DSL is missing top-level blocks: {', '.join(missing)}")

        return DslDocument(
            context=self._parse_context(data[DslField.CONTEXT.value]) if DslField.CONTEXT.value in data else None,
            variables=self._parse_variables(data.get(DslField.VARIABLES.value, {})),
            prechecks=self._parse_prechecks(data.get(DslField.PRECHECKS.value, [])),
            steps=self._parse_steps(data[DslField.STEPS.value]),
            on_fail=self._parse_fail_policy(data[DslField.ON_FAIL.value], DslField.ON_FAIL.value),
            raw=data,
        )

    def _parse_context(self, value: Any) -> ContextNode:
        path = DslField.CONTEXT.value
        mapping = self._expect_dict(value, path)
        return ContextNode(**self._parse_sql_node_fields(mapping, path))

    def _parse_variables(self, value: Any) -> Mapping[str, VariableDefinition]:
        path = DslField.VARIABLES.value
        return self.node_parser.parse_variables(value, path, DslField)

    def _parse_prechecks(self, value: Any) -> Sequence[PrecheckNode]:
        path = DslField.PRECHECKS.value
        return self.node_parser.parse_prechecks(value, path, DslField)

    def _parse_steps(self, value: Any) -> Sequence[StepNode]:
        path = DslField.STEPS.value
        return self.node_parser.parse_steps(value, path, DslField)

    def _parse_fail_policy(self, value: Any, path: str) -> FailPolicy:
        return self.node_parser.parse_fail_policy(value, path, DslField)

    def _parse_string_list(self, value: Any, path: str) -> Sequence[str]:
        items = self._expect_list(value, path)
        return [self._expect_string(item, f"{path}[{index}]") for index, item in enumerate(items)]

    def _parse_sql_node_fields(self, mapping: Mapping[str, Any], path: str) -> dict[str, Any]:
        return self.node_parser.parse_sql_node_fields(mapping, path, DslField)

    @staticmethod
    def _expect_dict(value: Any, path: str) -> Mapping[str, Any]:
        if not isinstance(value, dict):
            raise DSLParseError(f"{path} must be an object.")
        return value

    @staticmethod
    def _expect_list(value: Any, path: str) -> Sequence[Any]:
        if not isinstance(value, list):
            raise DSLParseError(f"{path} must be a list.")
        return value

    @staticmethod
    def _expect_string(value: Any, path: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise DSLParseError(f"{path} must be a non-empty string.")
        return value

    def _optional_string(self, value: Any, path: str) -> Optional[str]:
        if value is None:
            return None
        return self._expect_string(value, path)
