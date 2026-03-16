"""ExecDSL 相关异常定义。"""


class DSLParseError(ValueError):
    """DSL 文本解析失败。"""


class DSLValidationError(ValueError):
    """DSL 静态校验失败。"""


class DSLExecutionError(RuntimeError):
    """DSL 运行时执行失败。"""
