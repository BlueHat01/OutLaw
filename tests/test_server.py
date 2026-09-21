import asyncio
from outlaw.server import Registry, Router, ServerProtocol
from outlaw.store import QueueStore
from outlaw import protocol as proto


# Task 5: Registry tests
def test_register_and_lookup():
    r = Registry()
    r.register("rahul", ("10.0.0.2", 5005), now=0)
    assert r.addr_of("rahul") == ("10.0.0.2", 5005)
    assert r.nick_of(("10.0.0.2", 5005)) == "rahul"
    assert r.online() == ["rahul"]


def test_reregister_updates_addr():
    r = Registry()
    r.register("rahul", ("10.0.0.2", 5005), now=0)
    r.register("rahul", ("10.0.0.9", 6000), now=1)
    assert r.addr_of("rahul") == ("10.0.0.9", 6000)
    assert r.nick_of(("10.0.0.2", 5005)) is None


def test_evict_stale():
    r = Registry()
    r.register("a", ("1.1.1.1", 1), now=0)
    r.register("b", ("2.2.2.2", 2), now=50)
    evicted = r.evict_stale(now=100, timeout=90)
    assert evicted == ["a"]
    assert r.online() == ["b"]


def test_touch_keeps_alive():
    r = Registry()
    r.register("a", ("1.1.1.1", 1), now=0)
    r.touch("a", now=80)
    assert r.evict_stale(now=100, timeout=90) == []


# Task 6: Router tests and helpers
class Sink:
    def __init__(self):
        self.sent = []

    def __call__(self, addr, packet):
        self.sent.append((addr, packet))


def make_router(tmp_path):
    reg = Registry()
    q = QueueStore(tmp_path / "q.json")
    sink = Sink()
    return Router(reg, q, sink), reg, q, sink


def test_register_acks_and_lists_online(tmp_path):
    router, reg, q, sink = make_router(tmp_path)
    router.handle({"t": "reg", "f": "rahul"}, ("10.0.0.2", 1), now=0)
    ack = [p for a, p in sink.sent if p["t"] == "ack"]
    assert ack and "rahul" in ack[0]["on"]
    assert reg.addr_of("rahul") == ("10.0.0.2", 1)


def test_broadcast_delivers_to_others_only(tmp_path):
    router, reg, q, sink = make_router(tmp_path)
    router.handle({"t": "reg", "f": "a"}, ("1.1.1.1", 1), now=0)
    router.handle({"t": "reg", "f": "b"}, ("2.2.2.2", 2), now=0)
    sink.sent.clear()
    router.handle(
        {"t": "m", "id": "x", "f": "a", "to": "__all__", "x": "hi", "s": 1, "n": 0, "c": 1},
        ("1.1.1.1", 1),
        now=1,
    )
    targets = [a for a, p in sink.sent if p.get("t") == "m"]
    assert ("2.2.2.2", 2) in targets
    assert ("1.1.1.1", 1) not in targets  # not echoed to sender
    assert any(p["t"] == "ma" and p["id"] == "x" for a, p in sink.sent)  # sender acked


def test_dm_to_offline_is_queued_then_flushed(tmp_path):
    router, reg, q, sink = make_router(tmp_path)
    router.handle({"t": "reg", "f": "a"}, ("1.1.1.1", 1), now=0)
    msg = {"t": "m", "id": "y", "f": "a", "to": "bob", "x": "hey", "s": 1, "n": 0, "c": 1}
    router.handle(msg, ("1.1.1.1", 1), now=1)
    assert not any(p.get("t") == "m" for a, p in sink.sent)  # bob offline, nothing delivered
    sink.sent.clear()
    router.handle({"t": "reg", "f": "bob"}, ("3.3.3.3", 3), now=2)
    flushed = [p for a, p in sink.sent if a == ("3.3.3.3", 3) and p.get("t") == "m"]
    assert flushed and flushed[0]["id"] == "y"


# I1: nick-collision handling
def test_duplicate_nick_from_different_addr_is_rejected(tmp_path):
    router, reg, q, sink = make_router(tmp_path)
    router.handle({"t": "reg", "f": "a"}, ("1.1.1.1", 1), now=0)
    sink.sent.clear()
    router.handle({"t": "reg", "f": "a"}, ("2.2.2.2", 2), now=1)
    errs = [(a, p) for a, p in sink.sent if p.get("t") == "err"]
    assert errs and errs[0][0] == ("2.2.2.2", 2)
    assert errs[0][1]["m"] == "nick taken"
    # addr1 still owns the nick; addr2 was not registered.
    assert reg.addr_of("a") == ("1.1.1.1", 1)
    assert reg.nick_of(("2.2.2.2", 2)) is None


def test_reregister_same_addr_succeeds(tmp_path):
    router, reg, q, sink = make_router(tmp_path)
    router.handle({"t": "reg", "f": "a"}, ("1.1.1.1", 1), now=0)
    sink.sent.clear()
    router.handle({"t": "reg", "f": "a"}, ("1.1.1.1", 1), now=5)
    assert not any(p.get("t") == "err" for a, p in sink.sent)
    assert any(p.get("t") == "ack" for a, p in sink.sent)
    assert reg.addr_of("a") == ("1.1.1.1", 1)


# Task 7: ServerProtocol tests
def test_protocol_decodes_and_routes(tmp_path):
    router, reg, q, sink = make_router(tmp_path)
    # send() in router writes to sink; ServerProtocol wraps a transport send instead.
    sp = ServerProtocol(router, debug_path=tmp_path / "debug.log")
    data = proto.encode({"t": "reg", "f": "rahul"})
    sp.datagram_received(data, ("10.0.0.2", 5005))
    assert reg.addr_of("rahul") == ("10.0.0.2", 5005)


def test_protocol_ignores_garbage(tmp_path):
    router, reg, q, sink = make_router(tmp_path)
    sp = ServerProtocol(router, debug_path=tmp_path / "debug.log")
    sp.datagram_received(b"\xff\xffnope", ("10.0.0.2", 5005))  # must not raise
    assert reg.online() == []


# Task 8: server delivery confirmation table + ma handling + sweep
def test_delivery_pending_recorded_and_cleared_by_ma(tmp_path):
    router, reg, q, sink = make_router(tmp_path)
    router.handle({"t": "reg", "f": "a"}, ("1.1.1.1", 1), now=0)
    router.handle({"t": "reg", "f": "b"}, ("2.2.2.2", 2), now=0)
    sink.sent.clear()
    router.handle({"t": "m", "id": "x", "f": "a", "to": "b", "x": "hi", "s": 1, "n": 0, "c": 1}, ("1.1.1.1", 1), now=1)
    assert ("b", "x", 0) in router.pending
    # b acks receipt
    router.handle({"t": "ma", "id": "x", "n": 0}, ("2.2.2.2", 2), now=1)
    assert ("b", "x", 0) not in router.pending


def test_sweep_retransmits_then_queues_and_evicts(tmp_path):
    router, reg, q, sink = make_router(tmp_path)
    router.handle({"t": "reg", "f": "a"}, ("1.1.1.1", 1), now=0)
    router.handle({"t": "reg", "f": "b"}, ("2.2.2.2", 2), now=0)
    router.handle({"t": "m", "id": "x", "f": "a", "to": "b", "x": "hi", "s": 1, "n": 0, "c": 1}, ("1.1.1.1", 1), now=1)
    # b never acks; sweep at t=3,5,7 -> 3 retransmits, then queue+evict
    for t in (3, 5, 7, 9):
        router.sweep_deliveries(now=t)
    assert ("b", "x", 0) not in router.pending
    assert "b" not in reg.online()                  # evicted
    router.handle({"t": "reg", "f": "b"}, ("2.2.2.2", 2), now=10)  # reconnect
    flushed = [pk for ad, pk in sink.sent if ad == ("2.2.2.2", 2) and pk.get("t") == "m"]
    assert any(pk["id"] == "x" for pk in flushed)    # delivered on reconnect


def test_multi_message_offline_all_delivered_regression(tmp_path):
    # reproduces the reported bug: several msgs to a client that went away
    router, reg, q, sink = make_router(tmp_path)
    router.handle({"t": "reg", "f": "a"}, ("1.1.1.1", 1), now=0)
    router.handle({"t": "reg", "f": "b"}, ("2.2.2.2", 2), now=0)  # b online then vanishes
    for i, mid in enumerate(["m1", "m2", "m3"]):
        router.handle({"t": "m", "id": mid, "f": "a", "to": "b", "x": str(i), "s": 1, "n": 0, "c": 1}, ("1.1.1.1", 1), now=1)
    for t in (3, 5, 7, 9):
        router.sweep_deliveries(now=t)               # b never acks -> all requeued
    sink.sent.clear()
    router.handle({"t": "reg", "f": "b"}, ("2.2.2.2", 2), now=10)
    got = [pk["id"] for ad, pk in sink.sent if ad == ("2.2.2.2", 2) and pk.get("t") == "m"]
    assert set(got) == {"m1", "m2", "m3"}            # ALL three, not just the last


# Task 9: server offline image grouping
def test_offline_image_grouped_and_flushed(tmp_path):
    router, reg, q, sink = make_router(tmp_path)
    router.handle({"t": "reg", "f": "a"}, ("1.1.1.1", 1), now=0)
    # b is offline; a sends a 2-chunk image to b
    router.handle({"t": "ih", "id": "img1", "f": "a", "to": "b", "c": 2, "nm": "p.jpg", "mt": "image/jpeg", "sz": 6, "s": 1}, ("1.1.1.1", 1), now=1)
    router.handle({"t": "im", "id": "img1", "n": 0, "c": 2, "d": "AAA"}, ("1.1.1.1", 1), now=1)
    router.handle({"t": "im", "id": "img1", "n": 1, "c": 2, "d": "BBB"}, ("1.1.1.1", 1), now=1)
    # nothing delivered yet (b offline), but it's queued as one image transfer
    # (not drained here — draining would consume it before the reconnect flush below)
    sink.sent.clear()
    router.handle({"t": "reg", "f": "b"}, ("2.2.2.2", 2), now=2)
    kinds = [pk["t"] for ad, pk in sink.sent if ad == ("2.2.2.2", 2) and pk.get("t") in ("ih", "im")]
    assert kinds.count("ih") == 1 and kinds.count("im") == 2
