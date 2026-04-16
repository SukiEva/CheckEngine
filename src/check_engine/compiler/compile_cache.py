"""DSL 编译缓存实现。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import OrderedDict
import hashlib
from typing import Generic, Optional, TypeVar

CompiledValueT = TypeVar("CompiledValueT")


class CompileCacheLike(ABC, Generic[CompiledValueT]):
    """编译缓存策略协议。"""

    @abstractmethod
    def get(self, dsl_text: str) -> Optional[CompiledValueT]:
        """读取缓存。"""

    @abstractmethod
    def put(self, dsl_text: str, value: CompiledValueT) -> None:
        """写入缓存。"""


class NoopCompileCache(CompileCacheLike[CompiledValueT]):
    """关闭缓存时使用的空实现。"""

    def get(self, dsl_text: str) -> Optional[CompiledValueT]:
        return None

    def put(self, dsl_text: str, value: CompiledValueT) -> None:
        return None


class HashedLruCompileCache(CompileCacheLike[CompiledValueT]):
    """基于哈希 key 的 LRU 编译缓存。

    以 SHA-256 摘要作为桶键加速查找，同时保留原始文本用于等值校验，
    消除极端情况下哈希碰撞导致返回错误缓存结果的风险。
    """

    def __init__(self, maxsize: int) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be greater than 0.")
        self._maxsize = maxsize
        self._entries: OrderedDict[str, tuple[str, CompiledValueT]] = OrderedDict()

    def get(self, dsl_text: str) -> Optional[CompiledValueT]:
        key = self._build_key(dsl_text)
        entry = self._entries.get(key)
        if entry is None:
            return None
        stored_text, value = entry
        if stored_text != dsl_text:
            return None
        self._entries.move_to_end(key)
        return value

    def put(self, dsl_text: str, value: CompiledValueT) -> None:
        key = self._build_key(dsl_text)
        self._entries[key] = (dsl_text, value)
        self._entries.move_to_end(key)
        if len(self._entries) > self._maxsize:
            self._entries.popitem(last=False)

    @staticmethod
    def _build_key(dsl_text: str) -> str:
        digest = hashlib.sha256(dsl_text.encode("utf-8")).hexdigest()
        return f"{len(dsl_text)}:{digest}"
