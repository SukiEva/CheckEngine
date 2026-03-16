"""SQL 安全校验器。"""

from __future__ import annotations

import re

from check_engine.dsl.models import DslDocument, SqlNode
from check_engine.exceptions import DSLValidationError


class SqlSafetyValidator:
    """保守地限制为只读 SQL。"""

    FORBIDDEN_PATTERN = re.compile(
        r"\b(insert|update|delete|merge|alter|drop|truncate|create|grant|revoke|comment)\b",
        re.IGNORECASE,
    )
    LEADING_PATTERN = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)

    def validate(self, document: DslDocument) -> None:
        self._validate_sql(document.context, "context")
        for index, precheck in enumerate(document.prechecks):
            self._validate_sql(precheck, "prechecks[{0}]".format(index))
        for index, step in enumerate(document.steps):
            self._validate_sql(step, "steps[{0}]".format(index))

    def _validate_sql(self, node: SqlNode, path: str) -> None:
        sql = node.sql_template.strip()
        if not self.LEADING_PATTERN.search(sql):
            raise DSLValidationError("{0}.sql_template 仅支持 SELECT/WITH 查询。".format(path))
        if self.FORBIDDEN_PATTERN.search(sql):
            raise DSLValidationError("{0}.sql_template 包含非只读 SQL 关键字。".format(path))
        if ";" in sql.rstrip(";"):
            raise DSLValidationError("{0}.sql_template 不支持多语句。".format(path))
