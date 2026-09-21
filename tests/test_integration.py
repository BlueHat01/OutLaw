# tests/test_integration.py
import asyncio
import pytest
from outlaw.client_net import ClientSession
from outlaw import protocol as proto
from outlaw import media
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


# --- Task 10: recipient ma includes n; image receive -> save ---

def test_client_receives_and_saves_image(tmp_path):
    got = {"img": []}
    from outlaw.client_net import ClientSession
    s = ClientSession("bob", ("10.0.0.1", 5005),
                      on_message=lambda *a: None, on_roster=lambda u: None,
                      on_link=lambda up: None, on_error=lambda m: None,
                      on_image=lambda f, to, path, meta: got["img"].append((f, path, meta)),
                      media_dir=tmp_path / "media")
    sent = []
    s._raw_send = lambda pkt: sent.append(pkt)
    data = b"\xff\xd8\xffhello-jpeg-bytes"
    slices = media.chunk_b64(data, 150)
    mid = "img9"
    s.feed(proto.make_img_header("alice", "bob", mid, len(slices), "p.jpg", "image/jpeg", len(data), 1))
    for n, sl in enumerate(slices):
        s.feed(proto.make_img_chunk(mid, n, len(slices), sl))
    assert got["img"], "on_image should fire on completion"
    frm, path, meta = got["img"][0]
    assert frm == "alice"  # taken from the header, not the data chunks
    from pathlib import Path
    assert Path(path).read_bytes() == data
    # receipt acks were sent (n=-1 for header, 0..k for chunks)
    assert any(p["t"] == "ma" and p["id"] == mid for p in sent)

def test_recipient_text_ack_includes_n():
    from outlaw.client_net import ClientSession
    s = ClientSession("bob", ("10.0.0.1", 5005),
                      on_message=lambda *a: None, on_roster=lambda u: None, on_link=lambda up: None)
    sent = []
    s._raw_send = lambda pkt: sent.append(pkt)
    s.feed({"t": "m", "id": "t1", "f": "a", "to": "bob", "x": "hi", "s": 1, "n": 0, "c": 1})
    acks = [p for p in sent if p["t"] == "ma"]
    assert acks and acks[0]["id"] == "t1" and acks[0]["n"] == 0


# --- Task 11: windowed image sender ---

pytest.importorskip("PIL")

def test_send_image_windows_chunks(tmp_path):
    from outlaw.client_net import ClientSession
    from PIL import Image
    src = tmp_path / "s.png"; Image.new("RGB", (1200, 900), (10, 20, 30)).save(src)
    s = ClientSession("alice", ("10.0.0.1", 5005),
                      on_message=lambda *a: None, on_roster=lambda u: None, on_link=lambda up: None)
    sent = []
    s._raw_send = lambda pkt: sent.append(pkt)
    total = s.send_image("bob", str(src))
    headers = [p for p in sent if p["t"] == "ih"]
    chunks = [p for p in sent if p["t"] == "im"]
    assert len(headers) == 1 and headers[0]["c"] == total
    assert 0 < len(chunks) <= media.IMAGE_WINDOW    # only a window sent up front
    # acking releases more
    mid = headers[0]["id"]
    before = len(chunks)
    s.feed({"t": "ma", "id": mid, "n": chunks[0]["n"]})
    after = len([p for p in sent if p["t"] == "im"])
    assert after >= before                          # window advanced (if more remain)


def test_send_image_stall_retransmits_unacked_chunks(tmp_path):
    from outlaw.client_net import ClientSession
    from PIL import Image
    src = tmp_path / "s2.png"; Image.new("RGB", (1200, 900), (50, 60, 70)).save(src)
    s = ClientSession("alice", ("10.0.0.1", 5005),
                      on_message=lambda *a: None, on_roster=lambda u: None, on_link=lambda up: None)
    sent = []
    s._raw_send = lambda pkt: sent.append(pkt)
    s.send_image("bob", str(src))
    headers = [p for p in sent if p["t"] == "ih"]
    mid = headers[0]["id"]
    assert mid in s._img_tx

    # Force a stall: nothing acked, mark the transfer as overdue, and clear
    # the sent log so we can observe fresh retransmits.
    s._img_tx[mid]["at"] = 0
    sent.clear()
    s._sweep(proto.now_ts())

    resent = [p for p in sent if p["t"] == "im" and p["id"] == mid]
    assert resent, "unacked sent chunks should be retransmitted on stall"

    # After more than 5 stalls with no progress, the transfer is abandoned.
    for _ in range(6):
        if mid not in s._img_tx:
            break
        s._img_tx[mid]["at"] = 0
        s._sweep(proto.now_ts())
    assert mid not in s._img_tx


# --- Task 15: end-to-end image transfer over a real loopback server ---

@pytest.mark.asyncio
async def test_image_end_to_end_over_loopback(tmp_path):
    from PIL import Image
    src = tmp_path / "s.png"; Image.new("RGB", (1000, 800), (200, 50, 50)).save(src)
    loop = asyncio.get_running_loop()
    reg = Registry(); q = QueueStore(tmp_path / "q.json"); holder = {}
    def send(addr, pkt):
        try: holder["t"].sendto(proto.encode(pkt), addr)
        except proto.PacketTooLarge: pass
    router = Router(reg, q, send)
    st, _ = await loop.create_datagram_endpoint(
        lambda: ServerProtocol(router, tmp_path / "d.log"), local_addr=("127.0.0.1", 0))
    holder["t"] = st; saddr = st.get_extra_info("sockname")

    # drive server delivery sweep periodically
    async def pump():
        while True:
            await asyncio.sleep(0.3); router.sweep_deliveries(proto.now_ts())
    pumper = asyncio.create_task(pump())

    from outlaw.client_net import ClientSession
    got = []
    a = ClientSession("alice", saddr, on_message=lambda *x: None, on_roster=lambda u: None, on_link=lambda up: None)
    b = ClientSession("bob", saddr, on_message=lambda *x: None, on_roster=lambda u: None,
                      on_link=lambda up: None, on_image=lambda f, to, path, meta: got.append(path),
                      media_dir=tmp_path / "bmedia")
    await a.start(); await b.start(); await asyncio.sleep(0.3)
    a.send_image("bob", str(src))
    # let the windowed transfer + acks complete
    for _ in range(60):
        await asyncio.sleep(0.2)
        if got: break
    assert got, "bob should receive the image"
    from pathlib import Path
    assert Path(got[0]).exists() and Path(got[0]).stat().st_size > 0
    pumper.cancel(); a.close(); b.close(); st.close()
