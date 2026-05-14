import json
import threading
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


def test_load_checkpoint_invalid_json(tmp_path):
    path = tmp_path / "checkpoint.json"
    with open(path, "w") as f:
        f.write("not a valid json")
    try:
        load_checkpoint(str(path))
        assert False, "Expected an exception for invalid JSON"
    except json.JSONDecodeError:
        pass


def test_concurrent_saves_produce_valid_json(tmp_path):
    """Last writer wins, but every write must leave a valid JSON file."""
    path = str(tmp_path / "checkpoint.json")
    errors = []

    def writer(i):
        try:
            save_checkpoint(path, {"completed_urls": {f"url{i}": True}, "job_status": "running"})
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    loaded = load_checkpoint(path)
    assert "completed_urls" in loaded
    assert "job_status" in loaded


def test_concurrent_reads_while_writing(tmp_path):
    """Readers never see a corrupt or missing file once it exists."""
    path = str(tmp_path / "checkpoint.json")
    save_checkpoint(path, {"completed_urls": {}, "job_status": "running"})
    errors = []

    def reader():
        for _ in range(10):
            try:
                data = load_checkpoint(path)
                assert "completed_urls" in data
            except Exception as e:
                errors.append(e)

    def writer(i):
        for j in range(5):
            try:
                save_checkpoint(path, {"completed_urls": {f"url{i}_{j}": True}, "job_status": "running"})
            except Exception as e:
                errors.append(e)

    threads = [threading.Thread(target=reader) for _ in range(5)]
    threads += [threading.Thread(target=writer, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors


def test_independent_paths_use_separate_locks(tmp_path):
    """Operations on different files do not block each other."""
    path_a = str(tmp_path / "a.json")
    path_b = str(tmp_path / "b.json")
    results = []

    def save_a():
        save_checkpoint(path_a, {"completed_urls": {"a": True}, "job_status": "running"})
        results.append("a")

    def save_b():
        save_checkpoint(path_b, {"completed_urls": {"b": True}, "job_status": "running"})
        results.append("b")

    t1 = threading.Thread(target=save_a)
    t2 = threading.Thread(target=save_b)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert set(results) == {"a", "b"}
    assert load_checkpoint(path_a)["completed_urls"] == {"a": True}
    assert load_checkpoint(path_b)["completed_urls"] == {"b": True}
