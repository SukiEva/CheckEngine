"""步骤类型注册表测试。"""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from check_engine.engine import DslEngine
from check_engine.dsl import VariableStepNode
from check_engine.runtime import NodeExecutionResult
from check_engine.sql import DatasourceRegistry
from check_engine.step_registry import StepTypeDefinition, StepTypeRegistry, build_default_step_registry


class _UnusedRegistry(DatasourceRegistry):
    def get(self, name: str) -> Any:
        raise AssertionError(f"unexpected datasource lookup: {name}")


class StepRegistryExtensionTestCase(unittest.TestCase):
    def test_custom_step_type_only_needs_registry_definition(self) -> None:
        base_registry = build_default_step_registry()
        constant_definition = StepTypeDefinition(
            type_name="constant",
            parse=self._parse_constant_step,
            validate_structure=lambda validator, node, _raw_step, path: validator.validate_variable_step_structure(node, path),
            validate_references=lambda validator, step, document, available_steps, available_variables, step_map, path_prefix: None,
            compile=lambda compiler, step: None,
            execute=self._execute_constant_step,
        )
        registry = StepTypeRegistry((*base_registry.definitions(), constant_definition))
        engine = DslEngine(step_registry=registry)
        dsl_text = json.dumps(
            {
                "steps": [
                    {
                        "name": "const_step",
                        "type": "constant",
                        "value": 123,
                    }
                ],
                "on_fail": {
                    "decision": "1 == 2",
                    "mode": "single",
                    "message_cn": "ok",
                    "message_en": "ok",
                },
            }
        )

        result = engine.execute(dsl_text, {}, datasource_registry=cast(DatasourceRegistry, _UnusedRegistry()))

        self.assertTrue(result.passed)
        self.assertEqual(result.steps["const_step"], 123)

    @staticmethod
    def _parse_constant_step(
        node_parser: Any,
        mapping: Mapping[str, Any],
        node_path: str,
        consumes: Any,
    ) -> VariableStepNode:
        del consumes
        return VariableStepNode(
            type="constant",
            name=node_parser.helpers.expect_string(mapping.get("name"), f"{node_path}.name"),
            when=(),
            default=mapping.get("value"),
            default_specified="value" in mapping,
        )

    @staticmethod
    def _execute_constant_step(
        pipeline: Any,
        step: Any,
        _compiled_step_data: Any,
        state: Any,
        datasource_registry: Any,
    ) -> NodeExecutionResult:
        del pipeline, state, datasource_registry
        if not isinstance(step, VariableStepNode):
            raise AssertionError("constant step should reuse VariableStepNode")
        return NodeExecutionResult(
            raw_rows=(),
            exported_data=step.default,
            exported_fields=(),
        )


if __name__ == "__main__":
    unittest.main()
