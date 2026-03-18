"""SQL 安全校验器。"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Optional

import sqlparse
from sqlparse import tokens as sql_tokens
from sqlparse.sql import Statement, Token

from ..dsl import DslDocument, SqlNode
from ..exceptions import DSLValidationError, ValidationErrorCode


class SqlSafetyValidator:
    """保守地限制为只读 SQL。"""

    # 仅校验前导 SELECT/WITH 不足以覆盖 PostgreSQL 的 writable CTE，
    # 例如 `WITH changed AS (DELETE ...) SELECT ...` 仍然会写数据。
    FORBIDDEN_KEYWORDS = frozenset(
        {
            "INSERT",
            "UPDATE",
            "DELETE",
            "MERGE",
            "ALTER",
            "DROP",
            "TRUNCATE",
            "CREATE",
            "GRANT",
            "REVOKE",
            "COMMENT",
        }
    )
    ALLOWED_LEADING_KEYWORDS = frozenset({"SELECT", "WITH"})

    def validate(self, document: DslDocument) -> None:
        if document.context is not None:
            self._validate_sql(document.context, "context")
        for index, precheck in enumerate(document.prechecks):
            self._validate_sql(precheck, "prechecks[{0}]".format(index))
        for index, step in enumerate(document.steps):
            self._validate_sql(step, "steps[{0}]".format(index))

    def _validate_sql(self, node: SqlNode, path: str) -> None:
        statements = self._parse_statements(node.sql_template)
        if len(statements) != 1:
            raise DSLValidationError(
                "{0}.sql_template multiple statements are not supported.".format(path),
                code=ValidationErrorCode.NON_READONLY_SQL,
            )

        statement = statements[0]
        leading_keyword = self._get_leading_keyword(statement)
        if leading_keyword not in self.ALLOWED_LEADING_KEYWORDS:
            raise DSLValidationError(
                "{0}.sql_template only SELECT/WITH queries are allowed.".format(path),
                code=ValidationErrorCode.NON_READONLY_SQL,
            )

        if self._find_forbidden_keyword(statement) is not None:
            raise DSLValidationError(
                "{0}.sql_template contains non-read-only SQL keyword.".format(path),
                code=ValidationErrorCode.NON_READONLY_SQL,
            )

    @staticmethod
    def _parse_statements(sql: str) -> list[Statement]:
        return [statement for statement in sqlparse.parse(sql) if str(statement).strip()]

    @staticmethod
    def _get_leading_keyword(statement: Statement) -> str:
        for token in SqlSafetyValidator._iter_significant_tokens(statement):
            return token.normalized
        return ""

    @staticmethod
    def _find_forbidden_keyword(statement: Statement) -> Optional[str]:
        for token in SqlSafetyValidator._iter_significant_tokens(statement):
            if not token.is_keyword:
                continue
            if token.normalized in SqlSafetyValidator.FORBIDDEN_KEYWORDS:
                return token.normalized
        return None

    @staticmethod
    def _iter_significant_tokens(statement: Statement) -> Iterator[Token]:
        for token in statement.flatten():
            if token.is_whitespace or token.ttype in sql_tokens.Comment:
                continue
            yield token
