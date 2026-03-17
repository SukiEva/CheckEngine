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
            return self._render_sub_repeat(template, policy, state, rows)

        raise DSLExecutionError(f"Unknown message rendering mode: {policy.mode}")

    def _render_sub_repeat(
        self,
        template: str,
        policy: FailPolicy,
        state: ExecutionState,
        rows: list[dict[str, Any]],
    ) -> str:
        left = template.index("[")
        right = template.index("]")
        prefix = template[:left]
        segment = template[left + 1 : right]
        suffix = template[right + 1 :]
        repeated = policy.divider.join([self._render_once(segment, state, row) for row in rows])
        return "{0}{1}{2}".format(
            self._render_once(prefix, state, None),
            repeated,
            self._render_once(suffix, state, None),
        )

    def _render_once(
        self,
        template: str,
        state: ExecutionState,
        row: Optional[dict[str, Any]],
    ) -> str:
        def replace(match: re.Match[str]) -> str:
            token = match.group(1).strip()
            if token.startswith("$"):
                return self._stringify(state.resolve_reference(token))
            if self.IMPLICIT_PATH_PATTERN.match(token):
                return self._stringify(state.resolve_path(token))
            if row is None:
                raise DSLExecutionError(f"Cannot resolve row-level placeholder in template: {token}")
            if token not in row:
                raise DSLExecutionError(f"Template placeholder field does not exist: {token}")
            return self._stringify(row[token])

        return self.PLACEHOLDER_PATTERN.sub(replace, template)

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

    def _stringify(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value)
