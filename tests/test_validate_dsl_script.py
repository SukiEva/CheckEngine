"""generate-execdsl 校验脚本测试。"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class ValidateDslScriptTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[1]
        self.script_path = (
            self.repo_root
            / ".codex"
            / "skills"
            / "generate-execdsl"
            / "scripts"
            / "validate_dsl.py"
        )
        example_path = self.repo_root / "references" / "example.json"
        self.example_data = json.loads(example_path.read_text(encoding="utf-8"))

    def _run_validator(self, dsl_text: str, extra_args: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(self.script_path)] + extra_args,
            input=dsl_text,
            text=True,
            capture_output=True,
            cwd=self.repo_root,
            check=False,
        )

    def _parse_payload(self, completed: subprocess.CompletedProcess[str]) -> dict[str, object]:
        self.assertTrue(completed.stdout.strip(), msg=f"stdout 为空，stderr={completed.stderr}")
        return json.loads(completed.stdout)

    def test_validate_script_accepts_example_from_stdin(self) -> None:
        completed = self._run_validator(
            json.dumps(self.example_data, ensure_ascii=False),
            [],
        )

        payload = self._parse_payload(completed)

        self.assertEqual(completed.returncode, 0)
        self.assertTrue(payload["valid"])
        self.assertEqual(payload["source"], "stdin")
        self.assertIsNone(payload["stage"])
        self.assertIsNone(payload["error_type"])
        self.assertIsNone(payload["message"])

    def test_validate_script_accepts_example_from_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            dsl_path = Path(temp_dir) / "example.json"
            dsl_path.write_text(json.dumps(self.example_data, ensure_ascii=False), encoding="utf-8")

            completed = subprocess.run(
                [sys.executable, str(self.script_path), "--file", str(dsl_path)],
                text=True,
                capture_output=True,
                cwd=self.repo_root,
                check=False,
            )

        payload = self._parse_payload(completed)

        self.assertEqual(completed.returncode, 0)
        self.assertTrue(payload["valid"])
        self.assertEqual(payload["source"], "file")
        self.assertEqual(payload["file_path"], str(dsl_path.resolve()))

    def test_validate_script_reports_invalid_reference(self) -> None:
        invalid_data = json.loads(json.dumps(self.example_data))
        invalid_data["on_fail"]["decision"] = "$steps.exchange_rate > 100"

        completed = self._run_validator(json.dumps(invalid_data, ensure_ascii=False), [])
        payload = self._parse_payload(completed)

        self.assertEqual(completed.returncode, 1)
        self.assertFalse(payload["valid"])
        self.assertEqual(payload["stage"], "validate")
        self.assertEqual(payload["error_type"], "DSLValidationError")
        self.assertIn("sql step reference must include exported field", str(payload["message"]))

    def test_validate_script_reports_bare_exists(self) -> None:
        invalid_data = json.loads(json.dumps(self.example_data))
        invalid_data["on_fail"]["decision"] = "exists"

        completed = self._run_validator(json.dumps(invalid_data, ensure_ascii=False), [])
        payload = self._parse_payload(completed)

        self.assertEqual(completed.returncode, 1)
        self.assertFalse(payload["valid"])
        self.assertEqual(payload["stage"], "validate")
        self.assertEqual(payload["error_type"], "DSLValidationError")
        self.assertIn("does not support bare 'exists'", str(payload["message"]))

    def test_validate_script_reports_invalid_consumes(self) -> None:
        invalid_data = json.loads(json.dumps(self.example_data))
        invalid_data["steps"][2]["outputs"] = []

        completed = self._run_validator(json.dumps(invalid_data, ensure_ascii=False), [])
        payload = self._parse_payload(completed)

        self.assertEqual(completed.returncode, 1)
        self.assertFalse(payload["valid"])
        self.assertEqual(payload["stage"], "validate")
        self.assertEqual(payload["error_type"], "DSLValidationError")
        self.assertIn("consumes.from references step outputs that are not declared", str(payload["message"]))

    def test_validate_script_reports_invalid_sub_repeat_template(self) -> None:
        invalid_data = json.loads(json.dumps(self.example_data))
        invalid_data["steps"][0]["on_fail"]["message_cn"] = "存在汇率为空的记录: 记录{func}-{txn}-{rate_date}"

        completed = self._run_validator(json.dumps(invalid_data, ensure_ascii=False), [])
        payload = self._parse_payload(completed)

        self.assertEqual(completed.returncode, 1)
        self.assertFalse(payload["valid"])
        self.assertEqual(payload["stage"], "validate")
        self.assertEqual(payload["error_type"], "DSLValidationError")
        self.assertIn("must contain exactly one [] segment", str(payload["message"]))


if __name__ == "__main__":
    unittest.main()
