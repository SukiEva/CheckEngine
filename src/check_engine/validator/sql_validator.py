"""SQL 安全校验器。"""

from __future__ import annotations

import re

from ..dsl.models import DslDocument, SqlNode
from ..exceptions import DSLValidationError, ValidationErrorCode


class SqlSafetyValidator:
    """保守地限制为只读 SQL。"""

    FORBIDDEN_PATTERN = re.compile(
        r"\b(insert|update|delete|merge|alter|drop|truncate|create|grant|revoke|comment)\b",
        re.IGNORECASE,
    )
    LEADING_PATTERN = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)

    def validate(self, document: DslDocument) -> None:
        if document.context is not None:
            self._validate_sql(document.context, "context")
        for index, precheck in enumerate(document.prechecks):
            self._validate_sql(precheck, "prechecks[{0}]".format(index))
        for index, step in enumerate(document.steps):
            self._validate_sql(step, "steps[{0}]".format(index))

    def _validate_sql(self, node: SqlNode, path: str) -> None:
        sql = node.sql_template.strip()
        normalized_sql = self._normalize_sql(sql)
        if not self.LEADING_PATTERN.search(normalized_sql):
            raise DSLValidationError(
                "{0}.sql_template only SELECT/WITH queries are allowed.".format(path),
                code=ValidationErrorCode.NON_READONLY_SQL,
            )
        if self.FORBIDDEN_PATTERN.search(normalized_sql):
            raise DSLValidationError(
                "{0}.sql_template contains non-read-only SQL keyword.".format(path),
                code=ValidationErrorCode.NON_READONLY_SQL,
            )
        if ";" in normalized_sql.rstrip(";"):
            raise DSLValidationError(
                "{0}.sql_template multiple statements are not supported.".format(path),
                code=ValidationErrorCode.NON_READONLY_SQL,
            )

    def _normalize_sql(self, sql: str) -> str:
        chars = list(sql)
        index = 0
        length = len(chars)
        while index < length:
            if sql.startswith("--", index):
                end = sql.find("\n", index)
                end = length if end == -1 else end
                for pointer in range(index, end):
                    chars[pointer] = " "
                index = end
                continue
            if sql.startswith("/*", index):
                end = sql.find("*/", index + 2)
                end = length - 2 if end == -1 else end
                for pointer in range(index, min(end + 2, length)):
                    chars[pointer] = " "
                index = min(end + 2, length)
                continue
            if sql[index] in {"'", '"'}:
                quote = sql[index]
                chars[index] = " "
                index += 1
                while index < length:
                    chars[index] = " "
                    if sql[index] == quote and sql[index - 1] != "\\":
                        index += 1
                        break
                    index += 1
                continue
            index += 1
        return "".join(chars)
