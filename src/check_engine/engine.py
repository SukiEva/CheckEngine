"""ExecDSL 主执行引擎。"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Optional

from .compiler import CompiledDsl, CompileCacheLike, DslCompiler, HashedLruCompileCache, NoopCompileCache
from .exceptions import DSLExecutionError, log_dsl_error
from .execution_pipeline import ExecutionPipeline
from .expression import ExpressionEvaluator
from .parser import JsonDslParser
from .reference_parser import ReferenceParser
from .renderer import MessageRenderer
from .renderer.template_parser import TemplateParser
from .runtime import ExecutionResult
from .sql import DatasourceRegistry, SqlExecutor
from .step_registry import StepTypeRegistry, build_default_step_registry
from .validator import DslValidator


class DslEngine:
    """统一入口：编译、校验并执行 DSL。"""

    def __init__(
        self,
        compile_cache_size: int = 128,
        logger: Optional[logging.Logger] = None,
        step_registry: Optional[StepTypeRegistry] = None,
        reference_parser: Optional[ReferenceParser] = None,
    ) -> None:
        if compile_cache_size < 0:
            raise ValueError("compile_cache_size must be greater than or equal to 0.")

        self.logger = logger or logging.getLogger(__name__)
        self.reference_parser = reference_parser or ReferenceParser()
        self.step_registry = step_registry or build_default_step_registry()
        self.parser: JsonDslParser = JsonDslParser(step_registry=self.step_registry)
        self.validator: DslValidator = DslValidator(
            reference_parser=self.reference_parser,
            step_registry=self.step_registry,
        )
        self.expression_evaluator: ExpressionEvaluator = ExpressionEvaluator(
            reference_parser=self.reference_parser,
        )
        self.compiler: DslCompiler = DslCompiler(
            expression_evaluator=self.expression_evaluator,
            step_registry=self.step_registry,
        )
        self.sql_executor: SqlExecutor = SqlExecutor()
        self.message_renderer: MessageRenderer = MessageRenderer(
            template_parser=TemplateParser(self.reference_parser),
        )
        self.compile_cache_size = compile_cache_size
        self._compile_cache_backend: CompileCacheLike[CompiledDsl] = (
            HashedLruCompileCache(compile_cache_size) if compile_cache_size > 0 else NoopCompileCache()
        )

    def validate(self, dsl_text: str) -> None:
        """显式校验 DSL（适合保存/更新规则时调用）。"""
        if not isinstance(dsl_text, str):
            raise TypeError("dsl_text must be a string.")
        try:
            document = self.parser.parse(dsl_text)
            self.validator.validate(document)
            self.compiler.compile(document)
        except Exception as exc:  # noqa: BLE001
            handled_error = self._ensure_dsl_error(exc)
            self._log_dsl_error("validate", handled_error)
            raise handled_error from exc

    def execute(
        self,
        dsl_text: str,
        input_data: Mapping[str, Any],
        datasource_registry: DatasourceRegistry,
    ) -> ExecutionResult:
        if not isinstance(dsl_text, str):
            raise TypeError("dsl_text must be a string.")
        try:
            compiled_dsl = self._compile(dsl_text)
        except Exception as exc:  # noqa: BLE001
            handled_error = self._ensure_dsl_error(exc)
            self._log_dsl_error("compile", handled_error)
            raise handled_error from exc
        pipeline = ExecutionPipeline(
            expression_evaluator=self.expression_evaluator,
            sql_executor=self.sql_executor,
            message_renderer=self.message_renderer,
            step_registry=self.step_registry,
            logger=self.logger,
        )
        return pipeline.execute(compiled_dsl, input_data=input_data, datasource_registry=datasource_registry)

    def _compile(self, dsl_text: str) -> CompiledDsl:
        """编译 DSL 并返回可由缓存共享复用的只读结果。"""
        cached = self._compile_cache_backend.get(dsl_text)
        if cached is not None:
            return cached

        document = self.parser.parse(dsl_text)
        compiled = self.compiler.compile(document=document)
        self._compile_cache_backend.put(dsl_text, compiled)
        return compiled

    @staticmethod
    def _ensure_dsl_error(exc: Exception) -> Exception:
        from .exceptions import DSLParseError, DSLValidationError

        if isinstance(exc, (DSLParseError, DSLValidationError, DSLExecutionError)):
            return exc
        return DSLExecutionError("Unexpected runtime error in DSL engine.", original_exception=exc)

    def _log_dsl_error(self, phase: str, exc: Exception) -> None:
        log_dsl_error(self.logger, phase, exc)
