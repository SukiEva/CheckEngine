"""消息模板渲染上下文。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Optional

from check_engine.exceptions import DSLExecutionError
from check_engine.runtime import ExecutionState
from check_engine.renderer.template_parser import TemplateToken


@dataclass(frozen=True)
class RenderContext:
    """单次模板渲染所需的上下文。"""

    state: ExecutionState
    row: Optional[Mapping[str, Any]] = None
    overrides: Optional[Mapping[str, Any]] = None
    local_data: Optional[Any] = None

    def resolve_token(self, template_token: TemplateToken) -> Any:
        token = template_token.token
        if self.overrides is not None and token in self.overrides:
            return self.overrides[token]
        if template_token.reference is not None:
            return self.state.resolve_reference(template_token.reference.explicit, local_data=self.local_data)
        if self.row is None:
            raise DSLExecutionError(
                f"Cannot resolve row-level placeholder in template: {token}",
            )
        if token not in self.row:
            raise DSLExecutionError(
                f"Template placeholder field does not exist: {token}",
            )
        return self.row[token]
