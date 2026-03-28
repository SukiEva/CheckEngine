"""ExecDSL JSON 解析器。"""

from __future__ import annotations

import json
import logging
from typing import Any, Mapping, Optional, Sequence

from ..dsl import ContextNode, DslDocument, TopLevelField
from ..exceptions import DSLParseError
from .node_parsers import JsonNodeParser, ParserHelpers


class JsonDslParser:
    """将 JSON DSL 文本解析为内部数据模型。"""

    REQUIRED_TOP_LEVEL_KEYS = (
        TopLevelField.STEPS,
        TopLevelField.ON_FAIL,
    )

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self.logger = logger or logging.getLogger(__name__)
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
            self.logger.exception("Failed to decode DSL JSON text.")
            raise DSLParseError(f"Failed to parse DSL JSON: {exc.msg}") from exc

        if not isinstance(data, dict):
            raise DSLParseError("Top-level DSL must be a JSON object.")

        missing = [key for key in self.REQUIRED_TOP_LEVEL_KEYS if key not in data]
        if missing:
            raise DSLParseError(f"DSL is missing top-level blocks: {', '.join(missing)}")

        context: Optional[ContextNode] = None
        if TopLevelField.CONTEXT in data:
            context_path = TopLevelField.CONTEXT
            context_mapping = self._expect_dict(data[TopLevelField.CONTEXT], context_path)
            context = ContextNode(**self.node_parser.parse_sql_node_fields(context_mapping, context_path))

        variables = self.node_parser.parse_variables(data.get(TopLevelField.VARIABLES, {}), TopLevelField.VARIABLES)
        prechecks = self.node_parser.parse_prechecks(data.get(TopLevelField.PRECHECKS, []), TopLevelField.PRECHECKS)
        steps = self.node_parser.parse_steps(data[TopLevelField.STEPS], TopLevelField.STEPS)
        on_fail = self.node_parser.parse_fail_policy(data[TopLevelField.ON_FAIL], TopLevelField.ON_FAIL)

        return DslDocument(context=context, variables=variables, prechecks=prechecks, steps=steps, on_fail=on_fail, raw=data)

    def _parse_string_list(self, value: Any, path: str) -> Sequence[str]:
        items = self._expect_list(value, path)
        return [self._expect_string(item, f"{path}[{index}]") for index, item in enumerate(items)]

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
