"""组合 DSL 校验入口。"""

from typing import Optional

from ..dsl import DslDocument
from ..reference_parser import ReferenceParser
from ..step_registry import StepTypeRegistry
from .reference_validator import ReferenceValidator
from .sql_validator import SqlSafetyValidator
from .structure_validator import StructureValidator


class DslValidator:
    """按固定顺序执行 DSL 静态校验。"""

    def __init__(
        self,
        structure_validator: Optional[StructureValidator] = None,
        reference_validator: Optional[ReferenceValidator] = None,
        sql_validator: Optional[SqlSafetyValidator] = None,
        reference_parser: Optional[ReferenceParser] = None,
        step_registry: Optional[StepTypeRegistry] = None,
    ) -> None:
        self.structure_validator = structure_validator or StructureValidator(
            reference_parser=reference_parser,
            step_registry=step_registry,
        )
        self.reference_validator = reference_validator or ReferenceValidator(
            reference_parser=reference_parser,
            step_registry=step_registry,
        )
        self.sql_validator = sql_validator or SqlSafetyValidator()

    def validate(self, document: DslDocument) -> None:
        self.structure_validator.validate(document)
        self.reference_validator.validate(document)
        self.sql_validator.validate(document)
