from outlaw import store

def test_config_roundtrip(tmp_path):
    c = store.Config(tmp_path / "config.json")
    assert c.load() is None
    c.save("rahul")
    assert store.Config(tmp_path / "config.json").load() == "rahul"

def test_queue_enqueue_and_drain(tmp_path):
    q = store.QueueStore(tmp_path / "queue.json")
    q.enqueue("alice", {"t": "m", "id": "1"})
    q.enqueue("alice", {"t": "m", "id": "2"})
    assert q.drain("alice") == [{"t": "m", "id": "1"}, {"t": "m", "id": "2"}]
    assert q.drain("alice") == []  # drained

def test_queue_maxlen_drops_oldest(tmp_path):
    q = store.QueueStore(tmp_path / "queue.json", maxlen=2)
    for i in range(3):
        q.enqueue("bob", {"id": i})
    assert [m["id"] for m in q.drain("bob")] == [1, 2]

def test_queue_persist_and_reload(tmp_path):
    path = tmp_path / "queue.json"
    q = store.QueueStore(path)
    q.enqueue("alice", {"id": "x"})
    q.persist()
    q2 = store.QueueStore(path)
    q2.load()
    assert q2.drain("alice") == [{"id": "x"}]

def test_append_line_creates_and_appends(tmp_path):
    log = tmp_path / "sub" / "history.log"
    store.append_line(log, "line1")
    store.append_line(log, "line2")
    assert log.read_text(encoding="utf-8").splitlines() == ["line1", "line2"]

def _xfer(i, nbytes=1000):
    return {"id": f"x{i}", "header": {"t": "ih", "id": f"x{i}"},
            "chunks": [{"t": "im", "id": f"x{i}", "n": 0}], "bytes": nbytes}

def test_image_queue_keeps_recent_and_drains(tmp_path):
    q = store.QueueStore(tmp_path / "q.json")
    for i in range(2):
        q.enqueue_image("bob", _xfer(i))
    out = q.drain_images("bob")
    assert [t["id"] for t in out] == ["x0", "x1"]
    assert q.drain_images("bob") == []

def test_image_queue_count_cap_evicts_oldest(tmp_path):
    q = store.QueueStore(tmp_path / "q.json")
    for i in range(5):
        q.enqueue_image("bob", _xfer(i, nbytes=1000))
    ids = [t["id"] for t in q.drain_images("bob")]
    assert ids == ["x2", "x3", "x4"]  # only last 3 kept

def test_image_queue_byte_cap_evicts_oldest(tmp_path):
    q = store.QueueStore(tmp_path / "q.json")
    q.enqueue_image("bob", _xfer(0, nbytes=500000))
    q.enqueue_image("bob", _xfer(1, nbytes=500000))  # 1M > 768K -> evict x0
    ids = [t["id"] for t in q.drain_images("bob")]
    assert ids == ["x1"]

def test_image_queue_does_not_affect_text(tmp_path):
    q = store.QueueStore(tmp_path / "q.json")
    q.enqueue("bob", {"t": "m", "id": "t1"})
    q.enqueue_image("bob", _xfer(0))
    assert [m["id"] for m in q.drain("bob")] == ["t1"]

def test_image_queue_persist_reload(tmp_path):
    path = tmp_path / "q.json"
    q = store.QueueStore(path)
    q.enqueue_image("bob", _xfer(0))
    q.persist()
    q2 = store.QueueStore(path); q2.load()
    assert [t["id"] for t in q2.drain_images("bob")] == ["x0"]
