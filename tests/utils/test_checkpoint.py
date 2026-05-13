import json
from scripts.utils import load_checkpoint, save_checkpoint

def test_load_checkpoint_nonexistent(tmp_path):
    path = tmp_path / "checkpoint.json"
    checkpoint = load_checkpoint(str(path))
    assert checkpoint == {"completed_urls": {}, "job_status": "running"}

def test_save_and_load_checkpoint(tmp_path):
    path = tmp_path / "checkpoint.json"
    data = {"completed_urls": {"url1": True}, "job_status": "running"}
    save_checkpoint(str(path), data)
    loaded = load_checkpoint(str(path))
    assert loaded == data

def test_save_checkpoint_atomic(tmp_path):
    path = tmp_path / "checkpoint.json"
    data = {"completed_urls": {"url1": True}, "job_status": "running"}
    save_checkpoint(str(path), data)
    assert path.exists()
    # Check that the file is not left in a half-written state
    with open(path) as f:
        loaded = json.load(f)
    assert loaded == data

def test_save_checkpoint_overwrite(tmp_path):
    path = tmp_path / "checkpoint.json"
    data1 = {"completed_urls": {"url1": True}, "job_status": "running"}
    data2 = {"completed_urls": {"url2": True}, "job_status": "completed"}
    save_checkpoint(str(path), data1)
    save_checkpoint(str(path), data2)
    loaded = load_checkpoint(str(path))
    assert loaded == data2

def test_save_checkpoint_invalid_path(tmp_path):
    path = tmp_path / "nonexistent_dir" / "checkpoint.json"
    data = {"completed_urls": {"url1": True}, "job_status": "running"}
    try:
        save_checkpoint(str(path), data)
        assert False, "Expected an exception for invalid path"
    except FileNotFoundError:
        pass

def test_save_checkpoint(tmp_path):
    path = tmp_path / "checkpoint.json"
    data = {"completed_urls": {"url1": True}, "job_status": "running"}
    save_checkpoint(str(path), data)
    assert path.exists()

def test_load_checkpoint(tmp_path):
    path = tmp_path / "checkpoint.json"
    data = {"completed_urls": {"url1": True}, "job_status": "running"}
    with open(path, "w") as f:
        json.dump(data, f)
    loaded = load_checkpoint(str(path))
    assert loaded == data

def test_load_checkpoint_invalid_id(tmp_path):
    path = tmp_path / "checkpoint.json"
    with open(path, "w") as f:
        f.write("not a valid json")
    try:
        load_checkpoint(str(path))
        assert False, "Expected an exception for invalid JSON"
    except json.JSONDecodeError:
        pass