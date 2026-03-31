from __future__ import annotations

from collections import OrderedDict
from typing import Generic, TypeVar


K = TypeVar("K")
V = TypeVar("V")


_MISSING = object()


class BoundedCache(Generic[K, V]):
    """A tiny LRU cache used by long-lived services to avoid unbounded growth."""

    def __init__(self, max_size: int):
        self._max_size = max(int(max_size), 0)
        self._data: OrderedDict[K, V] = OrderedDict()

    def get(self, key: K, default=None):
        if self._max_size == 0:
            return default
        value = self._data.pop(key, _MISSING)
        if value is _MISSING:
            return default
        self._data[key] = value
        return value

    def set(self, key: K, value: V) -> None:
        if self._max_size == 0:
            return
        self._data.pop(key, None)
        self._data[key] = value
        if len(self._data) > self._max_size:
            self._data.popitem(last=False)

    def clear(self) -> None:
        self._data.clear()

    def __contains__(self, key: K) -> bool:
        return key in self._data

    def __len__(self) -> int:
        return len(self._data)
