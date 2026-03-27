"""DSL 编译缓存实现。"""

from __future__ import annotations

from collections import OrderedDict, namedtuple
import hashlib
from typing import Generic, Optional, Protocol, TypeVar

CompiledValueT = TypeVar("CompiledValueT")
CompileCacheInfo = namedtuple("CompileCacheInfo", ["hits", "misses", "maxsize", "currsize"])


class CompileCacheLike(Protocol, Generic[CompiledValueT]):
    """编译缓存策略协议。"""

    def get(self, dsl_text: str) -> Optional[CompiledValueT]:
        ...

    def put(self, dsl_text: str, value: CompiledValueT) -> None:
        ...

    def clear(self) -> None:
        ...

    def info(self) -> Optional[CompileCacheInfo]:
        ...

    def debug_keys(self) -> tuple[str, ...]:
        ...


class NoopCompileCache(Generic[CompiledValueT]):
    """关闭缓存时使用的空实现。"""

    def get(self, dsl_text: str) -> Optional[CompiledValueT]:
        return None

    def put(self, dsl_text: str, value: CompiledValueT) -> None:
        return None

    def clear(self) -> None:
        return None

    def info(self) -> Optional[CompileCacheInfo]:
        return None

    def debug_keys(self) -> tuple[str, ...]:
        return tuple()


class HashedLruCompileCache(Generic[CompiledValueT]):
    """基于哈希 key 的 LRU 编译缓存。"""

    def __init__(self, maxsize: int) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be greater than 0.")
        self._maxsize = maxsize
        self._entries: OrderedDict[str, CompiledValueT] = OrderedDict()
        self._hits = 0
        self._misses = 0

    def get(self, dsl_text: str) -> Optional[CompiledValueT]:
        key = self._build_key(dsl_text)
        cached = self._entries.get(key)
        if cached is None:
            self._misses += 1
            return None
        self._hits += 1
        self._entries.move_to_end(key)
        return cached

    def put(self, dsl_text: str, value: CompiledValueT) -> None:
        key = self._build_key(dsl_text)
        self._entries[key] = value
        self._entries.move_to_end(key)
        if len(self._entries) > self._maxsize:
            self._entries.popitem(last=False)

    def clear(self) -> None:
        self._entries.clear()
        self._hits = 0
        self._misses = 0

    def info(self) -> Optional[CompileCacheInfo]:
        return CompileCacheInfo(
            hits=self._hits,
            misses=self._misses,
            maxsize=self._maxsize,
            currsize=len(self._entries),
        )

    def debug_keys(self) -> tuple[str, ...]:
        return tuple(self._entries.keys())

    @staticmethod
    def _build_key(dsl_text: str) -> str:
        digest = hashlib.sha256(dsl_text.encode("utf-8")).hexdigest()
        return f"{len(dsl_text)}:{digest}"
