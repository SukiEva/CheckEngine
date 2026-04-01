"""ExecDSL 失败消息渲染器。"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any, Mapping, Optional

from ..dsl import FAIL_MODE_FULL_REPEAT, FAIL_MODE_SINGLE, FAIL_MODE_SUB_REPEAT, FailPolicy
from ..exceptions import DSLExecutionError
from ..runtime import ExecutionState
from .render_context import RenderContext
from .template_parser import TemplateParser, TemplateToken
from .mode_renderers import FullRepeatModeRenderer, MessageRenderHelpers, ModeRenderer, SingleModeRenderer, SubRepeatModeRenderer


class MessageRenderer(MessageRenderHelpers):
    """按 DSL mode 渲染失败消息。"""

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        template_parser: Optional[TemplateParser] = None,
    ) -> None:
        self.logger = logger or logging.getLogger(__name__)
        self.template_parser = template_parser or TemplateParser()
        self.mode_renderers: dict[str, ModeRenderer] = {
            FAIL_MODE_SINGLE: SingleModeRenderer(),
            FAIL_MODE_FULL_REPEAT: FullRepeatModeRenderer(),
            FAIL_MODE_SUB_REPEAT: SubRepeatModeRenderer(),
        }

    def render(
        self,
        policy: FailPolicy,
        state: ExecutionState,
        rows: Optional[Sequence[Mapping[str, Any]]] = None,
        local_data: Optional[Any] = None,
    ) -> tuple[str, str]:
        render_rows = rows or []
        return (
            self._render_template(policy.message_cn, policy, "cn", state, render_rows, local_data),
            self._render_template(policy.message_en, policy, "en", state, render_rows, local_data),
        )

    def _render_template(
        self,
        template: str,
        policy: FailPolicy,
        locale: str,
        state: ExecutionState,
        rows: Sequence[Mapping[str, Any]],
        local_data: Optional[Any],
    ) -> str:
        mode_renderer = self.mode_renderers.get(policy.mode)
        if mode_renderer is None:
            raise DSLExecutionError(
                f"Unknown message rendering mode: {policy.mode}",
            )
        return mode_renderer.render(
            template=template,
            policy=policy,
            locale=locale,
            state=state,
            rows=rows,
            local_data=local_data,
            helpers=self,
        )

    def _render_sub_repeat_segments(
        self,
        segment: str,
        state: ExecutionState,
        rows: Sequence[Mapping[str, Any]],
        local_data: Optional[Any] = None,
    ) -> list[str]:
        if rows:
            return [self._render_once(segment, state, row) for row in rows]

        array_tokens = self._collect_array_tokens(segment, state, local_data)
        if not array_tokens:
            return []

        token_size = self._validate_and_get_array_token_size(array_tokens)
        rendered: list[str] = []
        for index in range(token_size):
            overrides = {token: values[index] for token, values in array_tokens.items()}
            rendered.append(self._render_once(segment, state, None, overrides=overrides, local_data=local_data))
        return rendered

    def _collect_array_tokens(
        self,
        template: str,
        state: ExecutionState,
        local_data: Optional[Any],
    ) -> dict[str, list[Any]]:
        token_map: dict[str, list[Any]] = {}
        render_context = RenderContext(state=state, local_data=local_data)
        for template_token in self.template_parser.extract_tokens(template):
            if template_token.reference is None:
                continue
            value = render_context.resolve_token(template_token)
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                token_map[template_token.token] = list(value)
        return token_map

    @staticmethod
    def _validate_and_get_array_token_size(array_tokens: Mapping[str, list[Any]]) -> int:
        token_lengths = {len(values) for values in array_tokens.values()}
        if len(token_lengths) != 1:
            raise DSLExecutionError(
                "sub_repeat/full_repeat list placeholders must have the same length.",
            )
        return token_lengths.pop()


    def render_once(
        self,
        template: str,
        state: ExecutionState,
        row: Optional[Mapping[str, Any]],
        local_data: Optional[Any] = None,
    ) -> str:
        return self._render_once(template, state, row, local_data=local_data)

    def render_sub_repeat_segments(
        self,
        segment: str,
        state: ExecutionState,
        rows: Sequence[Mapping[str, Any]],
        local_data: Optional[Any] = None,
    ) -> list[str]:
        return self._render_sub_repeat_segments(segment, state, rows, local_data=local_data)

    def render_full_repeat_messages(
        self,
        template: str,
        state: ExecutionState,
        rows: Sequence[Mapping[str, Any]],
        local_data: Optional[Any] = None,
    ) -> list[str]:
        array_tokens = self._collect_array_tokens(template, state, local_data)
        if not array_tokens:
            if not rows:
                return []
            return [self._render_once(template, state, row, local_data=local_data) for row in rows]

        token_size = self._validate_and_get_array_token_size(array_tokens)
        if rows and len(rows) != token_size:
            raise DSLExecutionError(
                "full_repeat list placeholders must have the same length as result rows.",
            )
        row_sequence: list[Optional[Mapping[str, Any]]] = list(rows) if rows else [None] * token_size
        rendered: list[str] = []
        for index, row in enumerate(row_sequence):
            overrides = {token: values[index] for token, values in array_tokens.items()}
            rendered.append(self._render_once(template, state, row, overrides=overrides, local_data=local_data))
        return rendered

    def resolve_full_repeat_divider(self, policy: FailPolicy, locale: str) -> str:
        return self._resolve_full_repeat_divider(policy, locale)

    def resolve_sub_repeat_divider(self, policy: FailPolicy, locale: str) -> str:
        return self._resolve_sub_repeat_divider(policy, locale)

    def _render_once(
        self,
        template: str,
        state: ExecutionState,
        row: Optional[Mapping[str, Any]],
        overrides: Optional[dict[str, Any]] = None,
        local_data: Optional[Any] = None,
    ) -> str:
        render_context = RenderContext(
            state=state,
            row=row,
            overrides=overrides,
            local_data=local_data,
        )
        rendered_parts: list[str] = []
        cursor = 0
        for template_token in self.template_parser.extract_tokens(template):
            rendered_parts.append(template[cursor:template_token.start])
            rendered_parts.append(self._render_template_token(template_token, render_context))
            cursor = template_token.end
        rendered_parts.append(template[cursor:])
        return "".join(rendered_parts)

    @staticmethod
    def _render_template_token(template_token: TemplateToken, render_context: RenderContext) -> str:
        try:
            value = render_context.resolve_token(template_token)
            if template_token.formatted:
                if template_token.format_spec is None:
                    raise DSLExecutionError(
                        f"Formatted placeholder must include format spec: {template_token.raw}",
                    )
                return format(value, template_token.format_spec)
            return MessageRenderer._stringify(value)
        except DSLExecutionError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise DSLExecutionError(
                f"Failed to format placeholder: {template_token.raw}",
                original_exception=exc,
            ) from exc

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
                )
            return policy.divider_cn
        if policy.divider_en is None:
            raise DSLExecutionError(
                "sub_repeat divider_en is required when divider is not set.",
            )
        return policy.divider_en

    @staticmethod
    def _stringify(value: Any) -> str:
        if value is None:
            return ""
        return str(value)
