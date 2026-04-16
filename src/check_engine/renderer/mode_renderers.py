"""失败消息渲染模式策略。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Optional

from check_engine.dsl import FailPolicy
from check_engine.exceptions import DSLExecutionError
from check_engine.runtime import ExecutionState


class MessageRenderHelpers(ABC):
    """消息渲染辅助能力接口。"""

    @abstractmethod
    def render_once(
        self,
        template: str,
        state: ExecutionState,
        row: Optional[Mapping[str, Any]],
        local_data: Optional[Any] = None,
    ) -> str:
        """渲染单条模板。"""

    @abstractmethod
    def render_sub_repeat_segments(
        self,
        segment: str,
        state: ExecutionState,
        rows: Sequence[Mapping[str, Any]],
        local_data: Optional[Any] = None,
    ) -> list[str]:
        """渲染 sub_repeat 重复片段。"""

    @abstractmethod
    def resolve_full_repeat_divider(self, policy: FailPolicy, locale: str) -> str:
        """解析 full_repeat 分隔符。"""

    @abstractmethod
    def resolve_sub_repeat_divider(self, policy: FailPolicy, locale: str) -> str:
        """解析 sub_repeat 分隔符。"""

    @abstractmethod
    def render_full_repeat_messages(
        self,
        template: str,
        state: ExecutionState,
        rows: Sequence[Mapping[str, Any]],
        local_data: Optional[Any] = None,
    ) -> list[str]:
        """渲染 full_repeat 每个重复片段。"""


class ModeRenderer(ABC):
    """渲染模式策略接口。"""

    @abstractmethod
    def render(
        self,
        *,
        template: str,
        policy: FailPolicy,
        locale: str,
        state: ExecutionState,
        rows: Sequence[Mapping[str, Any]],
        local_data: Optional[Any],
        helpers: MessageRenderHelpers,
    ) -> str:
        """按模式渲染消息。"""


@dataclass(frozen=True)
class SingleModeRenderer(ModeRenderer):
    """single 模式渲染策略。"""

    def render(
        self,
        *,
        template: str,
        policy: FailPolicy,
        locale: str,
        state: ExecutionState,
        rows: Sequence[Mapping[str, Any]],
        local_data: Optional[Any],
        helpers: MessageRenderHelpers,
    ) -> str:
        del policy, locale
        if len(rows) > 1:
            raise DSLExecutionError(
                "single mode requires at most one result row.",
            )
        row = rows[0] if rows else None
        return helpers.render_once(template, state, row, local_data=local_data)


@dataclass(frozen=True)
class FullRepeatModeRenderer(ModeRenderer):
    """full_repeat 模式渲染策略。"""

    def render(
        self,
        *,
        template: str,
        policy: FailPolicy,
        locale: str,
        state: ExecutionState,
        rows: Sequence[Mapping[str, Any]],
        local_data: Optional[Any],
        helpers: MessageRenderHelpers,
    ) -> str:
        messages = helpers.render_full_repeat_messages(template, state, rows, local_data=local_data)
        if not messages:
            return helpers.render_once(template, state, None, local_data=local_data)

        divider = helpers.resolve_full_repeat_divider(policy, locale)
        return divider.join(messages)


@dataclass(frozen=True)
class SubRepeatModeRenderer(ModeRenderer):
    """sub_repeat 模式渲染策略。"""

    def render(
        self,
        *,
        template: str,
        policy: FailPolicy,
        locale: str,
        state: ExecutionState,
        rows: Sequence[Mapping[str, Any]],
        local_data: Optional[Any],
        helpers: MessageRenderHelpers,
    ) -> str:
        try:
            left = template.index("[")
            right = template.index("]")
        except ValueError:
            raise DSLExecutionError(
                f"sub_repeat template must contain exactly one [] segment: {template!r}",
            )
        prefix = template[:left]
        segment = template[left + 1 : right]
        suffix = template[right + 1 :]
        divider = helpers.resolve_sub_repeat_divider(policy, locale)
        repeated = divider.join(helpers.render_sub_repeat_segments(segment, state, rows, local_data=local_data))

        return "{0}{1}{2}".format(
            helpers.render_once(prefix, state, None, local_data=local_data),
            repeated,
            helpers.render_once(suffix, state, None, local_data=local_data),
        )
