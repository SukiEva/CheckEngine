"""ExecDSL 失败消息渲染器。"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any, Mapping, Optional

from ..dsl import FAIL_MODE_FULL_REPEAT, FAIL_MODE_SINGLE, FAIL_MODE_SUB_REPEAT, FailPolicy
from ..exceptions import DSLExecutionError, ExecutionErrorCode
from ..runtime import ExecutionState


class MessageRenderer:
    """按 DSL mode 渲染失败消息。"""

    PLACEHOLDER_PATTERN = re.compile(r"\{([^{}]+)\}")
    FORMAT_PLACEHOLDER_PATTERN = re.compile(r"f\{([^{}]+)\}")
    IMPLICIT_PATH_PATTERN = re.compile(r"^(input|context|variables|steps)\.[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*$")

    def render(
        self,
        policy: FailPolicy,
        state: ExecutionState,
        rows: Optional[Sequence[Mapping[str, Any]]] = None,
    ) -> tuple[str, str]:
        render_rows = rows or []
        return (
            self._render_template(policy.message_cn, policy, "cn", state, render_rows),
            self._render_template(policy.message_en, policy, "en", state, render_rows),
        )

    def _render_template(
        self,
        template: str,
        policy: FailPolicy,
        locale: str,
        state: ExecutionState,
        rows: Sequence[Mapping[str, Any]],
    ) -> str:
        if policy.mode == FAIL_MODE_SINGLE:
            if len(rows) > 1:
                raise DSLExecutionError(
                    "single mode requires at most one result row.",
                    code=ExecutionErrorCode.SINGLE_MODE_MULTI_ROWS,
                )
            row = rows[0] if rows else None
            return self._render_once(template, state, row)

        if policy.mode == FAIL_MODE_FULL_REPEAT:
            if not rows:
                return self._render_once(template, state, None)
            divider = self._resolve_full_repeat_divider(policy, locale)
            return divider.join(self._render_once(template, state, row) for row in rows)

        if policy.mode == FAIL_MODE_SUB_REPEAT:
            return self._render_sub_repeat(template, policy, locale, state, rows)

        raise DSLExecutionError(
            f"Unknown message rendering mode: {policy.mode}",
            code=ExecutionErrorCode.TEMPLATE_RENDER_FAILED,
        )

    def _render_sub_repeat(
        self,
        template: str,
        policy: FailPolicy,
        locale: str,
        state: ExecutionState,
        rows: Sequence[Mapping[str, Any]],
    ) -> str:
        left = template.index("[")
        right = template.index("]")
        prefix = template[:left]
        segment = template[left + 1 : right]
        suffix = template[right + 1 :]
        divider = self._resolve_sub_repeat_divider(policy, locale)
        repeated = divider.join(self._render_sub_repeat_segments(segment, state, rows))
        return "{0}{1}{2}".format(
            self._render_once(prefix, state, None),
            repeated,
            self._render_once(suffix, state, None),
        )

    def _render_sub_repeat_segments(
        self,
        segment: str,
        state: ExecutionState,
        rows: Sequence[Mapping[str, Any]],
    ) -> list[str]:
        if rows:
            return [self._render_once(segment, state, row) for row in rows]

        array_tokens = self._collect_array_tokens(segment, state)
        if not array_tokens:
            return []

        token_lengths = {len(values) for values in array_tokens.values()}
        if len(token_lengths) != 1:
            raise DSLExecutionError(
                "sub_repeat list placeholders must have the same length.",
                code=ExecutionErrorCode.ARRAY_LENGTH_MISMATCH,
            )

        token_size = token_lengths.pop()
        rendered: list[str] = []
        for index in range(token_size):
            overrides = {token: values[index] for token, values in array_tokens.items()}
            rendered.append(self._render_once(segment, state, None, overrides=overrides))
        return rendered

    def _collect_array_tokens(self, template: str, state: ExecutionState) -> dict[str, list[Any]]:
        token_map: dict[str, list[Any]] = {}
        for token in self._extract_template_tokens(template):
            reference_token, _ = self._split_format_token(token)
            if not self._is_global_reference_token(reference_token):
                continue
            value = self._resolve_global_token(reference_token, state)
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                token_map[reference_token] = list(value)
        return token_map

    def _render_once(
        self,
        template: str,
        state: ExecutionState,
        row: Optional[Mapping[str, Any]],
        overrides: Optional[dict[str, Any]] = None,
    ) -> str:
        formatted_template = self.FORMAT_PLACEHOLDER_PATTERN.sub(
            lambda match: self._render_formatted_placeholder(match, state, row, overrides),
            template,
        )
        return self.PLACEHOLDER_PATTERN.sub(
            lambda match: self._stringify(
                self._resolve_token_value(match.group(1).strip(), state, row, overrides)
            ),
            formatted_template,
        )

    def _render_formatted_placeholder(
        self,
        match: re.Match[str],
        state: ExecutionState,
        row: Optional[Mapping[str, Any]],
        overrides: Optional[dict[str, Any]],
    ) -> str:
        token, format_spec = self._split_format_token(match.group(1).strip())
        if format_spec is None:
            raise DSLExecutionError(
                f"Formatted placeholder must include format spec: {match.group(0)}",
                code=ExecutionErrorCode.TEMPLATE_RENDER_FAILED,
            )
        try:
            value = self._resolve_token_value(token, state, row, overrides)
            return format(value, format_spec)
        except DSLExecutionError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise DSLExecutionError(
                f"Failed to format placeholder: {match.group(0)}",
                code=ExecutionErrorCode.TEMPLATE_RENDER_FAILED,
            ) from exc

    def _resolve_token_value(
        self,
        token: str,
        state: ExecutionState,
        row: Optional[Mapping[str, Any]],
        overrides: Optional[dict[str, Any]],
    ) -> Any:
        if overrides is not None and token in overrides:
            return overrides[token]
        if self._is_global_reference_token(token):
            return self._resolve_global_token(token, state)
        if row is None:
            raise DSLExecutionError(
                f"Cannot resolve row-level placeholder in template: {token}",
                code=ExecutionErrorCode.TEMPLATE_RENDER_FAILED,
            )
        if token not in row:
            raise DSLExecutionError(
                f"Template placeholder field does not exist: {token}",
                code=ExecutionErrorCode.TEMPLATE_RENDER_FAILED,
            )
        return row[token]

    def _extract_template_tokens(self, template: str) -> list[str]:
        return [match.group(1).strip() for match in self.PLACEHOLDER_PATTERN.finditer(template)]

    def _is_global_reference_token(self, token: str) -> bool:
        return token.startswith("$") or self.IMPLICIT_PATH_PATTERN.match(token) is not None

    @staticmethod
    def _resolve_global_token(token: str, state: ExecutionState) -> Any:
        if token.startswith("$"):
            return state.resolve_reference(token)
        return state.resolve_path(token)

    @staticmethod
    def _split_format_token(token: str) -> tuple[str, Optional[str]]:
        if ":" not in token:
            return token, None
        name, format_spec = token.split(":", 1)
        return name.strip(), format_spec

    @staticmethod
    def _resolve_full_repeat_divider(policy: FailPolicy, locale: str) -> str:
        if locale == "cn":
            if policy.divider_cn is not None:
                return policy.divider_cn
            if policy.divider is not None:
                return policy.divider
            return "；"
        if policy.divider_en is not None:
            return policy.divider_en
        if policy.divider is not None:
            return policy.divider
        return " "

    @staticmethod
    def _resolve_sub_repeat_divider(policy: FailPolicy, locale: str) -> str:
        if policy.divider is not None:
            return policy.divider
        if locale == "cn":
            if policy.divider_cn is None:
                raise DSLExecutionError(
                    "sub_repeat divider_cn is required when divider is not set.",
                    code=ExecutionErrorCode.TEMPLATE_RENDER_FAILED,
                )
            return policy.divider_cn
        if policy.divider_en is None:
            raise DSLExecutionError(
                "sub_repeat divider_en is required when divider is not set.",
                code=ExecutionErrorCode.TEMPLATE_RENDER_FAILED,
            )
        return policy.divider_en

    @staticmethod
    def _stringify(value: Any) -> str:
        if value is None:
            return ""
        return str(value)
