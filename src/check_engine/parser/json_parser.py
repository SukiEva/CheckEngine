"""ExecDSL JSON 解析器。"""

from __future__ import annotations

import json
from typing import Any, Mapping, Optional, Sequence

from ..dsl import ContextNode, DslDocument, FailPolicy, PrecheckNode, StepNode, TopLevelField, VariableDefinition
from ..exceptions import DSLParseError
from .node_parsers import JsonNodeParser, ParserHelpers


class JsonDslParser:
    """将 JSON DSL 文本解析为内部数据模型。"""

    REQUIRED_TOP_LEVEL_KEYS = (
        TopLevelField.STEPS,
        TopLevelField.ON_FAIL,
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
            context=self._parse_context(data[TopLevelField.CONTEXT]) if TopLevelField.CONTEXT in data else None,
            variables=self._parse_variables(data.get(TopLevelField.VARIABLES, {})),
            prechecks=self._parse_prechecks(data.get(TopLevelField.PRECHECKS, [])),
            steps=self._parse_steps(data[TopLevelField.STEPS]),
            on_fail=self._parse_fail_policy(data[TopLevelField.ON_FAIL], TopLevelField.ON_FAIL),
            raw=data,
        )

    def _parse_context(self, value: Any) -> ContextNode:
        path = TopLevelField.CONTEXT
        mapping = self._expect_dict(value, path)
        return ContextNode(**self._parse_sql_node_fields(mapping, path))

    def _parse_variables(self, value: Any) -> Mapping[str, VariableDefinition]:
        path = TopLevelField.VARIABLES
        return self.node_parser.parse_variables(value, path)

    def _parse_prechecks(self, value: Any) -> Sequence[PrecheckNode]:
        path = TopLevelField.PRECHECKS
        return self.node_parser.parse_prechecks(value, path)

    def _parse_steps(self, value: Any) -> Sequence[StepNode]:
        path = TopLevelField.STEPS
        return self.node_parser.parse_steps(value, path)

    def _parse_fail_policy(self, value: Any, path: str) -> FailPolicy:
        return self.node_parser.parse_fail_policy(value, path)

    def _parse_string_list(self, value: Any, path: str) -> Sequence[str]:
        items = self._expect_list(value, path)
        return [self._expect_string(item, f"{path}[{index}]") for index, item in enumerate(items)]

    def _parse_sql_node_fields(self, mapping: Mapping[str, Any], path: str) -> dict[str, Any]:
        return self.node_parser.parse_sql_node_fields(mapping, path)

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
