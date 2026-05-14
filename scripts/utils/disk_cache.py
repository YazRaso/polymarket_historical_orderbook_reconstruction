"""Simple file-based JSON cache with atomic writes."""
import json
import os
from typing import Any


class DiskCache:
    def __init__(self, cache_dir: str) -> None:
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

    def _path(self, key: str) -> str:
        safe = key.replace("/", "_").replace(":", "_")
        return os.path.join(self.cache_dir, f"{safe}.json")

    def get(self, key: str) -> Any | None:
        path = self._path(key)
        if not os.path.exists(path):
            return None
        with open(path) as f:
            return json.load(f)

    def set(self, key: str, value: Any) -> None:
        path = self._path(key)
        tmp = f"{path}.tmp"
        with open(tmp, "w") as f:
            json.dump(value, f)
        os.replace(tmp, path)
