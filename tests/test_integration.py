# tests/test_integration.py
import asyncio
import pytest
from outlaw.client_net import ClientSession
from outlaw import protocol as proto
from outlaw.server import Registry, Router, ServerProtocol
from outlaw.store import QueueStore

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

def test_multichunk_shares_one_id_and_reassembles_end_to_end():
    # C2: a >140-byte message must be sent as chunks that all share ONE id,
    # with c == n_chunks and n covering 0..n-1, and must reassemble on the peer.
    s, got, sent = make_session()
    text = "".join(chr(ord("a") + (i % 26)) for i in range(400))  # 400 bytes ASCII
    s.send_text("__all__", text)
    msgs = [p for p in sent if p["t"] == "m"]
    n_chunks = len(proto.chunk_text(text))
    assert n_chunks > 1
    assert len(msgs) == n_chunks
    ids = {p["id"] for p in msgs}
    assert len(ids) == 1                              # ALL chunks share one id
    assert all(p["c"] == n_chunks for p in msgs)      # c == n_chunks
    assert {p["n"] for p in msgs} == set(range(n_chunks))  # n covers 0..n-1

    # Feed the exact emitted packets, scrambled, into a fresh receiving session.
    recv, rgot, rsent = make_session()
    scrambled = list(reversed(msgs))
    for pkt in scrambled:
        recv.feed(pkt)
    assert rgot["msgs"][-1][2] == text                # reassembled original text


def test_watchdog_marks_link_down_after_timeout():
    # I3: no inbound for > LINK_TIMEOUT while link is up -> on_link(False).
    s, got, sent = make_session()
    s.feed({"t": "ack", "on": ["rahul"]})             # brings link up, sets _last_rx
    assert s._link_up is True
    assert got["link"][-1] is True

    now = proto.now_ts()
    s._last_rx = now - (s.LINK_TIMEOUT + 5)            # simulate old last activity
    s._check_link(now)
    assert s._link_up is False
    assert got["link"][-1] is False


def test_ma_clears_pending():
    s, got, sent = make_session()
    s.send_text("alice", "hey")
    mid = [p for p in sent if p["t"] == "m"][0]["id"]
    assert mid in s._pending
    s.feed({"t": "ma", "id": mid})
    assert mid not in s._pending


def test_ma_per_chunk_ack_only_resends_unacked_chunks():
    # C2 regression: a multi-chunk message must not be dropped from _pending
    # until EVERY chunk's ack (carrying its own "n") has been seen. A lost
    # chunk must still be retransmitted on the next sweep.
    s, got, sent = make_session()
    s.send_text("__all__", "a" * 350)
    msgs = [p for p in sent if p["t"] == "m"]
    mid = msgs[0]["id"]
    assert len({p["id"] for p in msgs}) == 1
    assert msgs[0]["c"] >= 3
    assert len(msgs) >= 3

    # Server acks chunks 0 and 2, but chunk 1 is lost in transit.
    s.feed({"t": "ma", "id": mid, "n": 0})
    s.feed({"t": "ma", "id": mid, "n": 2})
    assert mid in s._pending    # not dropped: chunk 1 still unacked

    sent.clear()
    s._pending[mid]["at"] = 0   # force the entry to look overdue
    s._sweep(proto.now_ts())

    resent = [p for p in sent if p["t"] == "m"]
    assert len(resent) == 1
    assert resent[0]["n"] == 1

    s.feed({"t": "ma", "id": mid, "n": 1})
    assert mid not in s._pending

@pytest.mark.asyncio
async def test_two_clients_broadcast_and_dm_over_loopback(tmp_path):
    loop = asyncio.get_running_loop()
    reg = Registry()
    q = QueueStore(tmp_path / "q.json")
    holder = {}
    def send(addr, packet):
        try:
            holder["t"].sendto(proto.encode(packet), addr)
        except proto.PacketTooLarge:
            pass
    router = Router(reg, q, send)
    st, _ = await loop.create_datagram_endpoint(
        lambda: ServerProtocol(router, tmp_path / "d.log"), local_addr=("127.0.0.1", 0))
    holder["t"] = st
    server_addr = st.get_extra_info("sockname")

    a_got, b_got = [], []
    a = ClientSession("a", server_addr,
        on_message=lambda f, to, x, ts: a_got.append((f, x)),
        on_roster=lambda u: None, on_link=lambda up: None)
    b = ClientSession("b", server_addr,
        on_message=lambda f, to, x, ts: b_got.append((f, x)),
        on_roster=lambda u: None, on_link=lambda up: None)
    await a.start(); await b.start()
    await asyncio.sleep(0.2)                 # let registrations land

    a.send_text("__all__", "hello loop")
    await asyncio.sleep(0.2)
    assert ("a", "hello loop") in b_got      # broadcast reached b, not echoed to a

    b.send_text("a", "secret dm")
    await asyncio.sleep(0.2)
    assert ("b", "secret dm") in a_got

    a.close(); b.close(); st.close()

@pytest.mark.asyncio
async def test_offline_then_reconnect_delivery(tmp_path):
    loop = asyncio.get_running_loop()
    reg = Registry(); q = QueueStore(tmp_path / "q.json")
    holder = {}
    def send(addr, packet):
        holder["t"].sendto(proto.encode(packet), addr)
    router = Router(reg, q, send)
    st, _ = await loop.create_datagram_endpoint(
        lambda: ServerProtocol(router, tmp_path / "d.log"), local_addr=("127.0.0.1", 0))
    holder["t"] = st
    server_addr = st.get_extra_info("sockname")

    a = ClientSession("a", server_addr,
        on_message=lambda *x: None, on_roster=lambda u: None, on_link=lambda up: None)
    await a.start(); await asyncio.sleep(0.1)
    a.send_text("bob", "you were away")     # bob offline -> queued
    await asyncio.sleep(0.1)

    bob_got = []
    bob = ClientSession("bob", server_addr,
        on_message=lambda f, to, x, ts: bob_got.append((f, x)),
        on_roster=lambda u: None, on_link=lambda up: None)
    await bob.start(); await asyncio.sleep(0.2)   # register triggers queue flush
    assert ("a", "you were away") in bob_got
    a.close(); bob.close(); st.close()
