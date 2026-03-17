"""ExecDSL 失败消息渲染器。"""

from __future__ import annotations

import re
from typing import Any, Optional

from ..dsl.models import FailPolicy
from ..exceptions import DSLExecutionError
from ..runtime.state import ExecutionState


class MessageRenderer:
    """按 DSL mode 渲染失败消息。"""

    PLACEHOLDER_PATTERN = re.compile(r"\{([^{}]+)\}")
    FORMAT_PLACEHOLDER_PATTERN = re.compile(r"f\{([^{}]+)\}")
    IMPLICIT_PATH_PATTERN = re.compile(r"^(input|context|variables|steps)\.[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*$")

    def render(
        self,
        policy: FailPolicy,
        state: ExecutionState,
        rows: Optional[list[dict[str, Any]]] = None,
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
        rows: list[dict[str, Any]],
    ) -> str:
        if policy.mode == "single":
            if len(rows) > 1:
                raise DSLExecutionError("single mode requires at most one result row.")
            row = rows[0] if rows else None
            return self._render_once(template, state, row)

        if policy.mode == "full_repeat":
            if not rows:
                return self._render_once(template, state, None)
            divider = self._resolve_full_repeat_divider(policy, locale)
            return divider.join([self._render_once(template, state, row) for row in rows])

        if policy.mode == "sub_repeat":
            return self._render_sub_repeat(template, policy, locale, state, rows)

        raise DSLExecutionError(f"Unknown message rendering mode: {policy.mode}")

    def _render_sub_repeat(
        self,
        template: str,
        policy: FailPolicy,
        locale: str,
        state: ExecutionState,
        rows: list[dict[str, Any]],
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
        rows: list[dict[str, Any]],
    ) -> list[str]:
        if rows:
            return [self._render_once(segment, state, row) for row in rows]

        array_tokens = self._collect_array_tokens(segment, state)
        if not array_tokens:
            return []

        token_lengths = {len(values) for values in array_tokens.values()}
        if len(token_lengths) != 1:
            raise DSLExecutionError("sub_repeat list placeholders must have the same length.")

        token_size = token_lengths.pop()
        rendered: list[str] = []
        for index in range(token_size):
            overrides = {token: values[index] for token, values in array_tokens.items()}
            rendered.append(self._render_once(segment, state, None, overrides=overrides))
        return rendered

    def _collect_array_tokens(self, template: str, state: ExecutionState) -> dict[str, list[Any]]:
        token_map: dict[str, list[Any]] = {}
        for match in self.PLACEHOLDER_PATTERN.finditer(template):
            token = match.group(1).strip()
<<<<<<< codex/fix-on_fail-message_cn-for-array-outputs-ht74ms
            reference_token, _ = self._split_format_token(token)
            if reference_token.startswith("$"):
                value = state.resolve_reference(reference_token)
            elif self.IMPLICIT_PATH_PATTERN.match(reference_token):
                value = state.resolve_path(reference_token)
=======
            if token.startswith("$"):
                value = state.resolve_reference(token)
            elif self.IMPLICIT_PATH_PATTERN.match(token):
                value = state.resolve_path(token)
>>>>>>> main
            else:
                continue

            if isinstance(value, list):
<<<<<<< codex/fix-on_fail-message_cn-for-array-outputs-ht74ms
                token_map[reference_token] = value
=======
                token_map[token] = value
>>>>>>> main
        return token_map

    def _render_once(
        self,
        template: str,
        state: ExecutionState,
        row: Optional[dict[str, Any]],
        overrides: Optional[dict[str, Any]] = None,
    ) -> str:
<<<<<<< codex/fix-on_fail-message_cn-for-array-outputs-ht74ms
        def resolve_token(token: str) -> Any:
            if overrides is not None and token in overrides:
                return overrides[token]
=======
        def replace(match: re.Match[str]) -> str:
            token = match.group(1).strip()
            if overrides is not None and token in overrides:
                return self._stringify(overrides[token])
>>>>>>> main
            if token.startswith("$"):
                return state.resolve_reference(token)
            if self.IMPLICIT_PATH_PATTERN.match(token):
                return state.resolve_path(token)
            if row is None:
                raise DSLExecutionError(f"Cannot resolve row-level placeholder in template: {token}")
            if token not in row:
                raise DSLExecutionError(f"Template placeholder field does not exist: {token}")
            return row[token]

        def replace_format(match: re.Match[str]) -> str:
            token, format_spec = self._split_format_token(match.group(1).strip())
            if format_spec is None:
                raise DSLExecutionError(f"Formatted placeholder must include format spec: {match.group(0)}")
            try:
                return format(resolve_token(token), format_spec)
            except Exception as exc:  # noqa: BLE001
                raise DSLExecutionError(f"Failed to format placeholder: {match.group(0)}") from exc

        def replace(match: re.Match[str]) -> str:
            token = match.group(1).strip()
            return self._stringify(resolve_token(token))

        formatted_template = self.FORMAT_PLACEHOLDER_PATTERN.sub(replace_format, template)
        return self.PLACEHOLDER_PATTERN.sub(replace, formatted_template)

    def _split_format_token(self, token: str) -> tuple[str, Optional[str]]:
        if ":" not in token:
            return token, None
        name, format_spec = token.split(":", 1)
        return name.strip(), format_spec

    def _resolve_full_repeat_divider(self, policy: FailPolicy, locale: str) -> str:
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

    def _resolve_sub_repeat_divider(self, policy: FailPolicy, locale: str) -> str:
        if policy.divider is not None:
            return policy.divider
        if locale == "cn":
            if policy.divider_cn is None:
                raise DSLExecutionError("sub_repeat divider_cn is required when divider is not set.")
            return policy.divider_cn
        if policy.divider_en is None:
            raise DSLExecutionError("sub_repeat divider_en is required when divider is not set.")
        return policy.divider_en

    def _stringify(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value)
