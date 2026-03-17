"""SQL 安全校验器。"""

from __future__ import annotations

import re

from ..dsl.models import DslDocument, SqlNode
from ..exceptions import DSLValidationError


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
        if not self.LEADING_PATTERN.search(sql):
            raise DSLValidationError("{0}.sql_template only SELECT/WITH queries are allowed.".format(path))
        if self.FORBIDDEN_PATTERN.search(sql):
            raise DSLValidationError("{0}.sql_template contains non-read-only SQL keyword.".format(path))
        if ";" in sql.rstrip(";"):
            raise DSLValidationError("{0}.sql_template multiple statements are not supported.".format(path))
