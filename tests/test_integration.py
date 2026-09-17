# tests/test_integration.py
from outlaw.client_net import ClientSession
from outlaw import protocol as proto

def make_session():
    got = {"msgs": [], "roster": [], "link": []}
    s = ClientSession(
        "rahul", ("10.0.0.1", 5005),
        on_message=lambda f, to, x, ts: got["msgs"].append((f, to, x, ts)),
        on_roster=lambda users: got["roster"].append(users),
        on_link=lambda up: got["link"].append(up),
    )
    sent = []
    s._raw_send = lambda packet: sent.append(packet)  # bypass real socket
    return s, got, sent

def test_feed_ack_sets_link_up_and_roster():
    s, got, sent = make_session()
    s.feed({"t": "ack", "on": ["rahul", "alice"]})
    assert got["link"][-1] is True
    assert got["roster"][-1] == ["rahul", "alice"]

def test_feed_single_message_delivered():
    s, got, sent = make_session()
    s.feed({"t": "m", "id": "a3f", "f": "alice", "to": "rahul", "x": "hi", "s": 42, "n": 0, "c": 1})
    assert got["msgs"][-1] == ("alice", "rahul", "hi", 42)
    assert any(p["t"] == "ma" and p["id"] == "a3f" for p in sent)  # client acks receipt

def test_feed_dedups_retransmit():
    s, got, sent = make_session()
    pkt = {"t": "m", "id": "d1", "f": "alice", "to": "rahul", "x": "yo", "s": 1, "n": 0, "c": 1}
    s.feed(pkt); s.feed(pkt)
    assert len(got["msgs"]) == 1

def test_send_text_chunks_long_message():
    s, got, sent = make_session()
    s.send_text("__all__", "a" * 350)
    msgs = [p for p in sent if p["t"] == "m"]
    assert len(msgs) == 3
    assert all(p["c"] == 3 for p in msgs)
    assert {p["n"] for p in msgs} == {0, 1, 2}

def test_send_reassembles_multichunk_on_feed():
    s, got, sent = make_session()
    mid = "z9"
    for n, part in enumerate(["foo", "bar"]):
        s.feed({"t": "m", "id": mid, "f": "alice", "to": "rahul", "x": part, "s": 1, "n": n, "c": 2})
    assert got["msgs"][-1] == ("alice", "rahul", "foobar", 1)

def test_ma_clears_pending():
    s, got, sent = make_session()
    s.send_text("alice", "hey")
    mid = [p for p in sent if p["t"] == "m"][0]["id"]
    assert mid in s._pending
    s.feed({"t": "ma", "id": mid})
    assert mid not in s._pending
