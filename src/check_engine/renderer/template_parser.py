"""消息模板占位符解析。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from check_engine.reference_parser import ReferenceParser, ReferenceSpec


@dataclass(frozen=True)
class TemplateToken:
    """模板中的单个占位符。"""

    raw: str
    start: int
    end: int
    content: str
    token: str
    format_spec: Optional[str]
    formatted: bool
    reference: Optional[ReferenceSpec]


class TemplateParser:
    """统一负责提取模板占位符及其中的引用。"""

    TOKEN_PATTERN = re.compile(r"(f)?\{([^{}]+)\}")

    def __init__(self, reference_parser: Optional[ReferenceParser] = None) -> None:
        self.reference_parser = reference_parser or ReferenceParser()

    def extract_tokens(self, template: str) -> tuple[TemplateToken, ...]:
        return tuple(self._build_token(match) for match in self.TOKEN_PATTERN.finditer(template))

    def _build_token(self, match: re.Match[str]) -> TemplateToken:
        formatted = match.group(1) == "f"
        content = match.group(2).strip()
        token_name, format_spec = self.split_format_token(content)
        reference = self.reference_parser.parse_template_token(token_name)
        return TemplateToken(
            raw=match.group(0),
            start=match.start(),
            end=match.end(),
            content=content,
            token=token_name,
            format_spec=format_spec,
            formatted=formatted,
            reference=reference,
        )

    @staticmethod
    def split_format_token(token: str) -> tuple[str, Optional[str]]:
        if ":" not in token:
            return token.strip(), None
        name, format_spec = token.split(":", 1)
        return name.strip(), format_spec
