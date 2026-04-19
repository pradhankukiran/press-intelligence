from __future__ import annotations

from collections import OrderedDict
from typing import Any


class IdempotencyCache:
    def __init__(self, max_entries: int = 1024) -> None:
        self._store: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._max_entries = max_entries

    def get(self, key: str) -> dict[str, Any] | None:
        value = self._store.get(key)
        if value is not None:
            self._store.move_to_end(key)
        return value

    def set(self, key: str, value: dict[str, Any]) -> None:
        self._store[key] = value
        self._store.move_to_end(key)
        while len(self._store) > self._max_entries:
            self._store.popitem(last=False)

    def __len__(self) -> int:
        return len(self._store)
