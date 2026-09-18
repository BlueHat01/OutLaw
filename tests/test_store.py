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
