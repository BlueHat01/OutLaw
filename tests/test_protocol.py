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

def test_make_img_header_shape():
    h = p.make_img_header("alice", "bob", "3f9", 210, "pic.jpg", "image/jpeg", 24576, 42)
    assert h == {"t": "ih", "id": "3f9", "f": "alice", "to": "bob", "c": 210,
                 "nm": "pic.jpg", "mt": "image/jpeg", "sz": 24576, "s": 42}

def test_make_img_chunk_shape_and_fits():
    c = p.make_img_chunk("3f9", 42, 210, "Q" * 150)
    assert c == {"t": "im", "id": "3f9", "n": 42, "c": 210, "d": "Q" * 150}
    assert len(p.encode(c)) <= p.MAX_DATAGRAM  # 150 b64 chars stays under 230

def test_img_header_with_long_name_still_encodes():
    h = p.make_img_header("aliceanderson12", "bobbytables9999", "abc", 999,
                          "vacation.jpg", "image/jpeg", 24576, 1699999999)
    assert len(p.encode(h)) <= p.MAX_DATAGRAM

def test_image_assembler_completes_with_header():
    a = p.ImageAssembler()
    a.add_header("3f9", "alice", "pic.jpg", "image/jpeg", 300, now=0)
    assert a.add_chunk("3f9", 0, 2, "AA", now=0) is None
    out = a.add_chunk("3f9", 1, 2, "BB", now=0)
    assert out["slices"] == ["AA", "BB"]
    assert out["frm"] == "alice" and out["name"] == "pic.jpg" and out["mime"] == "image/jpeg"

def test_image_assembler_completes_without_header_uses_defaults():
    a = p.ImageAssembler()
    out = a.add_chunk("z1", 0, 1, "AA", now=0)
    assert out["slices"] == ["AA"]
    assert out["mime"] == "image/jpeg" and out["frm"] == "peer"  # defaults when header missing

def test_image_assembler_purges_stale():
    a = p.ImageAssembler(timeout=120)
    a.add_chunk("c1", 0, 3, "AA", now=0)
    a.purge(now=200)
    assert a.add_chunk("c1", 1, 3, "BB", now=200) is None  # earlier partial dropped
