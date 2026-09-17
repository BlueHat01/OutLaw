import pytest
from outlaw import protocol as p

def test_encode_decode_roundtrip():
    pkt = {"t": "m", "id": "a3f", "f": "rahul", "to": "__all__", "x": "hi", "s": 1, "n": 0, "c": 1}
    data = p.encode(pkt)
    assert isinstance(data, bytes)
    assert p.decode(data) == pkt

def test_decode_bad_data_returns_empty():
    assert p.decode(b"\xff\xfe not json") == {}
    assert p.decode(b"") == {}

def test_encode_rejects_oversize():
    pkt = {"t": "m", "f": "rahul", "to": "__all__", "x": "z" * 300, "s": 1, "id": "a3f", "n": 0, "c": 1}
    with pytest.raises(p.PacketTooLarge):
        p.encode(pkt)

def test_constants():
    assert p.MAX_DATAGRAM == 230
    assert p.TEXT_CAP == 140
    assert p.BROADCAST == "__all__"

def test_new_id_is_short_and_varies():
    ids = {p.new_id() for _ in range(50)}
    assert all(len(i) == 3 for i in ids)
    assert len(ids) > 1

def test_make_msg_shape():
    pkt = p.make_msg("rahul", "alice", "hey", "b7e", 0, 1, 42)
    assert pkt == {"t": "m", "id": "b7e", "f": "rahul", "to": "alice",
                   "x": "hey", "s": 42, "n": 0, "c": 1}

def test_chunk_short_text_single_chunk():
    assert p.chunk_text("hello") == ["hello"]

def test_chunk_long_text_splits_under_cap():
    text = "a" * 350
    chunks = p.chunk_text(text)
    assert len(chunks) == 3
    assert all(len(c.encode("utf-8")) <= p.TEXT_CAP for c in chunks)
    assert "".join(chunks) == text

def test_chunk_multibyte_not_broken():
    text = "e" * 138 + "😀"  # emoji is 4 bytes, would overflow a naive 140 split
    chunks = p.chunk_text(text)
    assert all(len(c.encode("utf-8")) <= p.TEXT_CAP for c in chunks)
    assert "".join(chunks) == text

def test_reassembler_single_chunk_completes():
    r = p.ChunkReassembler()
    assert r.add("a3f", 0, 1, "hi", now=0) == "hi"

def test_reassembler_multi_chunk_completes_in_order():
    r = p.ChunkReassembler()
    assert r.add("b7e", 0, 3, "foo", now=0) is None
    assert r.add("b7e", 2, 3, "baz", now=0) is None
    assert r.add("b7e", 1, 3, "bar", now=0) == "foobarbaz"

def test_reassembler_purges_stale():
    r = p.ChunkReassembler(timeout=30)
    r.add("c1", 0, 2, "x", now=0)
    r.purge(now=31)
    # after purge the partial is gone; second chunk alone cannot complete
    assert r.add("c1", 1, 2, "y", now=31) is None

def test_duplicate_filter():
    f = p.DuplicateFilter(maxlen=4)
    assert f.seen("a") is False
    assert f.seen("a") is True
    for k in "bcde":
        f.seen(k)
    # "a" evicted after 4 newer ids, so it is unseen again
    assert f.seen("a") is False
