"""组合 DSL 校验入口。"""

from typing import Optional

from check_engine.dsl.models import DslDocument
from check_engine.validator.reference_validator import ReferenceValidator
from check_engine.validator.structure_validator import StructureValidator


class DslValidator:
    """按固定顺序执行 DSL 静态校验。"""

    def __init__(
        self,
        structure_validator: Optional[StructureValidator] = None,
        reference_validator: Optional[ReferenceValidator] = None,
    ) -> None:
        self.structure_validator = structure_validator or StructureValidator()
        self.reference_validator = reference_validator or ReferenceValidator()

    def validate(self, document: DslDocument) -> None:
        self.structure_validator.validate(document)
        self.reference_validator.validate(document)
