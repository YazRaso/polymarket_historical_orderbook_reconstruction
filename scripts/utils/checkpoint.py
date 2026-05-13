"""This module is a utility for writing and saving checkpoints during data collection."""
import json
import os
from typing import Dict

def load_checkpoint(path: str) -> Dict[str, object]:
    if not os.path.exists(path):
        return {"completed_urls": {}, "job_status": "running"}
    with open(path) as f:
        data = json.load(f)
    data.setdefault("completed_urls", {})
    data.setdefault("job_status", "running")
    return data


def save_checkpoint(path: str, checkpoint: Dict[str, object]) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(checkpoint, f, indent=2, sort_keys=True)
    os.replace(tmp, path)