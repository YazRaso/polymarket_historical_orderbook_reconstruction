"""This module is a utility for writing and saving checkpoints during data collection."""
import json
import os
import threading
from typing import Dict

_locks: Dict[str, threading.Lock] = {}
_locks_lock = threading.Lock()


def _get_lock(path: str) -> threading.Lock:
    canonical = os.path.abspath(path)
    with _locks_lock:
        if canonical not in _locks:
            _locks[canonical] = threading.Lock()
        return _locks[canonical]


def load_checkpoint(path: str) -> Dict[str, object]:
    lock = _get_lock(path)
    with lock:
        if not os.path.exists(path):
            return {"completed_urls": {}, "job_status": "running"}
        with open(path) as f:
            data = json.load(f)
    data.setdefault("completed_urls", {})
    data.setdefault("job_status", "running")
    return data


def save_checkpoint(path: str, checkpoint: Dict[str, object]) -> None:
    lock = _get_lock(path)
    with lock:
        tmp = f"{path}.tmp"
        with open(tmp, "w") as f:
            json.dump(checkpoint, f, indent=2, sort_keys=True)
        os.replace(tmp, path)
