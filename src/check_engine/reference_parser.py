"""统一的 DSL 引用语法解析。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Optional

from .exceptions import DSLExecutionError


@dataclass(frozen=True)
class ReferenceSpec:
    """标准化后的引用描述。"""

    raw: str
    scope: str
    path: tuple[str, ...]
    implicit: bool = False

    @property
    def is_local(self) -> bool:
        return self.scope == "local"

    @property
    def explicit(self) -> str:
        if self.is_local:
            if not self.path:
                return "$."
            return "$." + ".".join(self.path)
        if not self.path:
            return "$" + self.scope
        return "$" + self.scope + "." + ".".join(self.path)


class ReferenceParser:
    """统一负责 DSL 引用的提取、解析与基础语法判断。"""

    IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_]\w*$")
    EXPLICIT_REFERENCE_PATTERN = re.compile(
        r"\$(?:\.(?:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)?|[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)"
    )
    FULL_EXPLICIT_REFERENCE_PATTERN = re.compile(
        r"^\$(?:\.(?:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)?|[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)$"
    )
    IMPLICIT_GLOBAL_REFERENCE_PATTERN = re.compile(
        r"^(input|variables|steps|context)\.[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*$"
    )
    EXISTS_CALL_PATTERN = re.compile(
        r"(?:not\s+)?exists\(\s*(\$(?:\.(?:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)?|[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*))\s*\)"
    )
    FULL_EXISTS_CALL_PATTERN = re.compile(
        r"^exists\(\s*(\$(?:\.(?:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)?|[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*))\s*\)$"
    )
    FULL_NOT_EXISTS_CALL_PATTERN = re.compile(
        r"^not\s+exists\(\s*(\$(?:\.(?:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)?|[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*))\s*\)$"
    )

    def parse(self, reference: str, *, allow_implicit_global: bool = False) -> ReferenceSpec:
        stripped = reference.strip()
        if stripped.startswith("$"):
            return self._parse_explicit_reference(stripped)
        if allow_implicit_global:
            return self._parse_implicit_global_reference(stripped)
        raise DSLExecutionError(f"Invalid reference path: {reference}")

    def parse_template_token(self, token: str) -> Optional[ReferenceSpec]:
        stripped = token.strip()
        if stripped.startswith("$"):
            return self.parse(stripped)
        if self._looks_like_implicit_reference_candidate(stripped):
            return self.parse(stripped, allow_implicit_global=True)
        return None

    def extract_references(self, text: str) -> tuple[ReferenceSpec, ...]:
        return tuple(
            self.parse(match.group(0))
            for match in self.EXPLICIT_REFERENCE_PATTERN.finditer(text)
        )

    def replace_references(
        self,
        text: str,
        replacer: Callable[[ReferenceSpec], str],
    ) -> str:
        def replace_match(match: re.Match[str]) -> str:
            return replacer(self.parse(match.group(0)))

        return self.EXPLICIT_REFERENCE_PATTERN.sub(replace_match, text)

    def extract_exists_argument_references(self, text: str) -> tuple[ReferenceSpec, ...]:
        return tuple(
            self.parse(match.group(1))
            for match in self.EXISTS_CALL_PATTERN.finditer(text)
        )

    def is_exists_call(self, expression: str) -> bool:
        return self.FULL_EXISTS_CALL_PATTERN.fullmatch(expression.strip()) is not None

    def is_not_exists_call(self, expression: str) -> bool:
        return self.FULL_NOT_EXISTS_CALL_PATTERN.fullmatch(expression.strip()) is not None

    def _parse_explicit_reference(self, reference: str) -> ReferenceSpec:
        if self.FULL_EXPLICIT_REFERENCE_PATTERN.fullmatch(reference) is None:
            raise DSLExecutionError(f"Invalid reference path: {reference}")

        if reference.startswith("$."):
            suffix = reference[2:]
            if not suffix:
                return ReferenceSpec(raw=reference, scope="local", path=())
            parts = tuple(suffix.split("."))
            self._validate_identifier_parts(parts, reference)
            return ReferenceSpec(raw=reference, scope="local", path=parts)

        parts = reference[1:].split(".")
        self._validate_identifier_parts(tuple(parts), reference)
        scope = parts[0]
        return ReferenceSpec(raw=reference, scope=scope, path=tuple(parts[1:]))

    def _parse_implicit_global_reference(self, reference: str) -> ReferenceSpec:
        if self.IMPLICIT_GLOBAL_REFERENCE_PATTERN.fullmatch(reference) is None:
            raise DSLExecutionError(f"Invalid reference path: {reference}")
        parts = reference.split(".")
        self._validate_identifier_parts(tuple(parts), reference)
        scope = parts[0]
        return ReferenceSpec(raw=reference, scope=scope, path=tuple(parts[1:]), implicit=True)

    def _validate_identifier_parts(self, parts: tuple[str, ...], reference: str) -> None:
        if not parts:
            raise DSLExecutionError(f"Invalid reference path: {reference}")
        for part in parts:
            if self.IDENTIFIER_PATTERN.fullmatch(part) is None:
                raise DSLExecutionError(f"Invalid reference path: {reference}")

    @staticmethod
    def _looks_like_implicit_reference_candidate(token: str) -> bool:
        root, dot, _suffix = token.partition(".")
        return dot == "." and root in {"input", "variables", "steps", "context"}
