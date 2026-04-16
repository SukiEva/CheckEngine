"""ExecDSL 相关异常定义。"""

from __future__ import annotations

import logging
import traceback
from typing import Optional


class _DslBaseError(Exception):
    """DSL 异常基类，保留原始异常与堆栈。"""

    def __init__(
        self,
        message: str,
        *,
        original_exception: Optional[BaseException] = None,
        original_traceback: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.original_exception = original_exception
        self.original_traceback = original_traceback or self._format_traceback(original_exception)

    @staticmethod
    def _format_traceback(original_exception: Optional[BaseException]) -> Optional[str]:
        if original_exception is None:
            return None
        return "".join(
            traceback.format_exception(
                type(original_exception),
                original_exception,
                original_exception.__traceback__,
            )
        )

    @staticmethod
    def _inherit_traceback_from_exception(
        original_exception: Optional[BaseException],
    ) -> Optional[str]:
        if original_exception is None:
            return None
        inherited_traceback = getattr(original_exception, "original_traceback", None)
        if isinstance(inherited_traceback, str):
            return inherited_traceback
        return _DslBaseError._format_traceback(original_exception)


class DSLParseError(_DslBaseError, ValueError):
    """DSL 文本解析失败。"""

    def __init__(
        self,
        message: str,
        *,
        original_exception: Optional[BaseException] = None,
    ) -> None:
        super().__init__(
            message,
            original_exception=original_exception,
            original_traceback=self._inherit_traceback_from_exception(original_exception),
        )


class DSLValidationError(_DslBaseError, ValueError):
    """DSL 静态校验失败。"""

    def __init__(
        self,
        message: str,
        *,
        original_exception: Optional[BaseException] = None,
    ) -> None:
        super().__init__(
            message,
            original_exception=original_exception,
            original_traceback=self._inherit_traceback_from_exception(original_exception),
        )


class DSLExecutionError(_DslBaseError, RuntimeError):
    """DSL 运行时执行失败。"""

    def __init__(
        self,
        message: str,
        *,
        original_exception: Optional[BaseException] = None,
    ) -> None:
        super().__init__(
            message,
            original_exception=original_exception,
            original_traceback=self._inherit_traceback_from_exception(original_exception),
        )


def log_dsl_error(logger: logging.Logger, phase: str, exc: Exception) -> None:
    """记录 DSL 错误日志，优先使用异常中保留的原始堆栈。"""
    error_traceback = getattr(exc, "original_traceback", None)
    if not isinstance(error_traceback, str) or not error_traceback:
        error_traceback = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    logger.error("DslEngine %s failed: %s\n%s", phase, exc, error_traceback)
