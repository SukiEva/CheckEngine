#!/usr/bin/env python3
"""使用项目内 DslEngine 校验 DSL 的命令行工具。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple


def _find_repo_root() -> Path:
    """从脚本位置向上定位仓库根目录。"""

    current_path = Path(__file__).resolve()
    for candidate in current_path.parents:
        if (candidate / "pyproject.toml").exists() and (candidate / "src" / "check_engine").exists():
            return candidate
    raise RuntimeError("无法定位项目根目录，找不到 pyproject.toml 或 src/check_engine。")


REPO_ROOT = _find_repo_root()
sys.path.insert(0, str(REPO_ROOT / "src"))

from check_engine import DSLExecutionError, DSLParseError, DSLValidationError, DslEngine


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="校验 ExecDSL JSON。")
    parser.add_argument(
        "--file",
        dest="file_path",
        help="从文件读取 DSL JSON。若不提供，则从标准输入读取。",
    )
    return parser.parse_args(argv)


def _read_dsl_text(file_path: Optional[str]) -> Tuple[str, str, Optional[str]]:
    if file_path is not None:
        source_path = Path(file_path).expanduser().resolve()
        dsl_text = source_path.read_text(encoding="utf-8")
        if not dsl_text.strip():
            raise ValueError("DSL 输入不能为空。")
        return dsl_text, "file", str(source_path)

    if sys.stdin.isatty():
        raise ValueError("请通过 --file 或标准输入提供 DSL JSON。")

    dsl_text = sys.stdin.read()
    if not dsl_text.strip():
        raise ValueError("DSL 输入不能为空。")
    return dsl_text, "stdin", None


def _build_result(
    valid: bool,
    stage: Optional[str],
    error_type: Optional[str],
    message: Optional[str],
    source: str,
    file_path: Optional[str],
) -> Dict[str, object]:
    return {
        "valid": valid,
        "stage": stage,
        "error_type": error_type,
        "message": message,
        "source": source,
        "file_path": file_path,
        "validator": "check_engine.DslEngine.validate",
    }


def _classify_stage(exc: Exception) -> str:
    if isinstance(exc, DSLParseError):
        return "parse"
    if isinstance(exc, DSLValidationError):
        return "validate"
    if isinstance(exc, DSLExecutionError):
        return "runtime"
    return "internal"


def _dump_result(payload: Dict[str, object]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False))
    sys.stdout.write("\n")


def main(argv: Sequence[str]) -> int:
    args = _parse_args(argv)

    try:
        dsl_text, source, file_path = _read_dsl_text(args.file_path)
    except (OSError, ValueError) as exc:
        _dump_result(
            _build_result(
                valid=False,
                stage="input",
                error_type=exc.__class__.__name__,
                message=str(exc),
                source="file" if args.file_path is not None else "stdin",
                file_path=args.file_path,
            )
        )
        return 2

    engine = DslEngine()
    try:
        engine.validate(dsl_text)
    except (DSLParseError, DSLValidationError, DSLExecutionError) as exc:
        _dump_result(
            _build_result(
                valid=False,
                stage=_classify_stage(exc),
                error_type=exc.__class__.__name__,
                message=str(exc),
                source=source,
                file_path=file_path,
            )
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        _dump_result(
            _build_result(
                valid=False,
                stage="internal",
                error_type=exc.__class__.__name__,
                message=str(exc),
                source=source,
                file_path=file_path,
            )
        )
        return 2

    _dump_result(
        _build_result(
            valid=True,
            stage=None,
            error_type=None,
            message=None,
            source=source,
            file_path=file_path,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
