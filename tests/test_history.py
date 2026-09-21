from outlaw import store

def test_append_and_load_history(tmp_path):
    path = tmp_path / "history.jsonl"
    store.append_history(path, {"ts": 1, "frm": "alice", "to": "__all__", "kind": "text", "text": "hi"})
    store.append_history(path, {"ts": 2, "frm": "bob", "to": "alice", "kind": "image", "path": "/x/a.jpg", "name": "a.jpg"})
    rows = store.load_recent_history(path)
    assert len(rows) == 2
    assert rows[0]["text"] == "hi"
    assert rows[1]["kind"] == "image" and rows[1]["path"] == "/x/a.jpg"

def test_load_recent_limit(tmp_path):
    path = tmp_path / "history.jsonl"
    for i in range(10):
        store.append_history(path, {"ts": i, "frm": "a", "to": "__all__", "kind": "text", "text": str(i)})
    rows = store.load_recent_history(path, limit=3)
    assert [r["text"] for r in rows] == ["7", "8", "9"]

def test_load_recent_skips_malformed(tmp_path):
    path = tmp_path / "history.jsonl"
    path.write_text('{"ts":1,"text":"ok"}\nGARBAGE\n{"ts":2,"text":"also"}\n', encoding="utf-8")
    rows = store.load_recent_history(path)
    assert [r["text"] for r in rows] == ["ok", "also"]

def test_load_recent_missing_file(tmp_path):
    assert store.load_recent_history(tmp_path / "nope.jsonl") == []
