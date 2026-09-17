# Outlaw Chat Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Outlaw — a Python CLI chat app that carries UDP messages over an iodine DNS tunnel, with a VPS relay server and a Textual full-screen TUI client for Termux.

**Architecture:** Two asyncio programs. `server.py` (VPS, `10.0.0.1:5005`) is a relay hub that registers nicks, routes group/DM messages, buffers messages for offline users, and provides ACK/retransmit reliability. `client.py` runs a Textual TUI whose asyncio `DatagramProtocol` feeds events into the UI event loop. All datagrams are compact JSON kept ≤230 bytes to fit iodine's `-m 250` fragment ceiling.

**Tech Stack:** Python 3.9+, asyncio, Textual (TUI), pytest (tests). Standard library `json`, `socket`, `uuid`, `time`, `collections`.

**Spec:** `docs/superpowers/specs/2026-09-18-outlaw-chat-design.md`

## Global Constraints

- **Datagram size ceiling: 230 bytes.** `encode()` must raise if a packet exceeds this. iodine runs with `-m 250`.
- **Per-packet text cap: 140 bytes** (UTF-8), enforced by chunking.
- **Broadcast sentinel:** the string `"__all__"` in a packet's `to` field.
- **UDP port:** `5005`.
- **Keepalive:** client pings every 30s; server evicts clients unseen for 90s.
- **Offline queue:** max 500 messages per nick, persisted to `~/.outlaw/queue.json`.
- **Compact JSON keys only:** `t,f,to,x,s,id,n,c,on,m` — never long key names.
- **App data dir:** `~/.outlaw/` (`config.json`, `queue.json`, `history.log`, `debug.log`).
- **No crashes on bad input:** all datagram parsing wrapped in try/except; bad datagrams dropped and logged.

---

## File Structure

- `outlaw/__init__.py` — package marker, version.
- `outlaw/protocol.py` — packet encode/decode, size enforcement, id gen, text chunking, chunk reassembly, duplicate filter, packet constructors.
- `outlaw/store.py` — local config (nick), persisted offline queue, history/debug logging.
- `outlaw/server.py` — registry, router, reliable delivery, asyncio server protocol, `run_server()`.
- `outlaw/client_net.py` — asyncio client protocol + `ClientSession` (register/retry, ping loop, send-with-ack, receive dispatch via callbacks).
- `outlaw/commands.py` — pure `parse_input()` mapping typed input to an `Action`.
- `outlaw/ui.py` — Textual `OutlawApp`, widgets, banner, styling.
- `server.py` — thin entrypoint calling `outlaw.server.run_server`.
- `client.py` — thin entrypoint wiring `ClientSession` + `OutlawApp`.
- `requirements.txt`, `README.md`.
- `tests/test_protocol.py`, `tests/test_store.py`, `tests/test_server.py`, `tests/test_commands.py`, `tests/test_integration.py`.

---

## Task 1: Protocol — encode/decode + size enforcement

**Files:**
- Create: `outlaw/__init__.py`, `outlaw/protocol.py`
- Test: `tests/test_protocol.py`

**Interfaces:**
- Produces:
  - `MAX_DATAGRAM = 230`, `TEXT_CAP = 140`, `BROADCAST = "__all__"`
  - `encode(packet: dict) -> bytes` — JSON+UTF-8; raises `PacketTooLarge` if len > MAX_DATAGRAM
  - `decode(data: bytes) -> dict` — returns `{}` on any parse error
  - `PacketTooLarge(Exception)`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_protocol.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_protocol.py -v`
Expected: FAIL (module `outlaw.protocol` not found).

- [ ] **Step 3: Write minimal implementation**

```python
# outlaw/__init__.py
__version__ = "1.0.0"
```

```python
# outlaw/protocol.py
import json

MAX_DATAGRAM = 230
TEXT_CAP = 140
BROADCAST = "__all__"

class PacketTooLarge(Exception):
    pass

def encode(packet: dict) -> bytes:
    data = json.dumps(packet, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(data) > MAX_DATAGRAM:
        raise PacketTooLarge(f"{len(data)} > {MAX_DATAGRAM}")
    return data

def decode(data: bytes) -> dict:
    try:
        obj = json.loads(data.decode("utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_protocol.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add outlaw/__init__.py outlaw/protocol.py tests/test_protocol.py
git commit -m "feat: packet encode/decode with 230-byte ceiling"
```

---

## Task 2: Protocol — ids, packet constructors, text chunking

**Files:**
- Modify: `outlaw/protocol.py`
- Test: `tests/test_protocol.py`

**Interfaces:**
- Produces:
  - `new_id() -> str` — 3-hex-char id
  - `now_ts() -> int`
  - `make_msg(frm, to, text, mid, n, c, ts) -> dict` — builds a `{"t":"m",...}` packet
  - `chunk_text(text: str) -> list[str]` — splits on ≤140 UTF-8 bytes without breaking chars

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_protocol.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_protocol.py -v`
Expected: FAIL (`new_id` not defined).

- [ ] **Step 3: Write minimal implementation**

```python
# add to outlaw/protocol.py
import time
import uuid

def new_id() -> str:
    return uuid.uuid4().hex[:3]

def now_ts() -> int:
    return int(time.time())

def make_msg(frm, to, text, mid, n, c, ts) -> dict:
    return {"t": "m", "id": mid, "f": frm, "to": to, "x": text, "s": ts, "n": n, "c": c}

def chunk_text(text: str) -> list:
    chunks, cur, cur_bytes = [], [], 0
    for ch in text:
        b = len(ch.encode("utf-8"))
        if cur_bytes + b > TEXT_CAP:
            chunks.append("".join(cur))
            cur, cur_bytes = [ch], b
        else:
            cur.append(ch)
            cur_bytes += b
    if cur or not chunks:
        chunks.append("".join(cur))
    return chunks
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_protocol.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add outlaw/protocol.py tests/test_protocol.py
git commit -m "feat: message ids, constructors, utf-8-safe text chunking"
```

---

## Task 3: Protocol — chunk reassembly + duplicate filter

**Files:**
- Modify: `outlaw/protocol.py`
- Test: `tests/test_protocol.py`

**Interfaces:**
- Produces:
  - `ChunkReassembler(timeout=30)` with `add(mid, n, c, text, now) -> str | None` (returns full text when complete) and `purge(now)`
  - `DuplicateFilter(maxlen=512)` with `seen(mid) -> bool` (returns True if already seen; records it)

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_protocol.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_protocol.py -v`
Expected: FAIL (`ChunkReassembler` not defined).

- [ ] **Step 3: Write minimal implementation**

```python
# add to outlaw/protocol.py
from collections import OrderedDict, deque

class ChunkReassembler:
    def __init__(self, timeout=30):
        self.timeout = timeout
        self._parts = {}   # mid -> {"c": total, "at": now, "chunks": {n: text}}

    def add(self, mid, n, c, text, now):
        entry = self._parts.get(mid)
        if entry is None:
            entry = {"c": c, "at": now, "chunks": {}}
            self._parts[mid] = entry
        entry["chunks"][n] = text
        if len(entry["chunks"]) == entry["c"]:
            full = "".join(entry["chunks"][i] for i in range(entry["c"]))
            del self._parts[mid]
            return full
        return None

    def purge(self, now):
        stale = [m for m, e in self._parts.items() if now - e["at"] > self.timeout]
        for m in stale:
            del self._parts[m]

class DuplicateFilter:
    def __init__(self, maxlen=512):
        self._seen = deque(maxlen=maxlen)
        self._set = set()

    def seen(self, mid) -> bool:
        if mid in self._set:
            return True
        if len(self._seen) == self._seen.maxlen:
            self._set.discard(self._seen[0])
        self._seen.append(mid)
        self._set.add(mid)
        return False
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_protocol.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add outlaw/protocol.py tests/test_protocol.py
git commit -m "feat: chunk reassembly with timeout and duplicate filter"
```

---

## Task 4: Store — config, offline queue, logging

**Files:**
- Create: `outlaw/store.py`
- Test: `tests/test_store.py`

**Interfaces:**
- Produces:
  - `Config(path)` with `.load() -> str | None`, `.save(nick)`
  - `QueueStore(path, maxlen=500)` with `enqueue(nick, packet)`, `drain(nick) -> list`, `persist()`, `load()`
  - `append_line(path, line)` — appends a line to a log file, creating parent dirs

- [ ] **Step 1: Write the failing test**

```python
# tests/test_store.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_store.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Write minimal implementation**

```python
# outlaw/store.py
import json
from collections import defaultdict, deque
from pathlib import Path

class Config:
    def __init__(self, path):
        self.path = Path(path)

    def load(self):
        try:
            return json.loads(self.path.read_text(encoding="utf-8")).get("nick")
        except Exception:
            return None

    def save(self, nick):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"nick": nick}), encoding="utf-8")

class QueueStore:
    def __init__(self, path, maxlen=500):
        self.path = Path(path)
        self.maxlen = maxlen
        self._q = defaultdict(lambda: deque(maxlen=self.maxlen))

    def enqueue(self, nick, packet):
        self._q[nick].append(packet)

    def drain(self, nick):
        msgs = list(self._q.get(nick, []))
        if nick in self._q:
            self._q[nick].clear()
        return msgs

    def persist(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {n: list(dq) for n, dq in self._q.items() if dq}
        self.path.write_text(json.dumps(data), encoding="utf-8")

    def load(self):
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return
        for n, msgs in data.items():
            dq = deque(msgs, maxlen=self.maxlen)
            self._q[n] = dq

def append_line(path, line):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_store.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add outlaw/store.py tests/test_store.py
git commit -m "feat: local config, persisted offline queue, log append"
```

---

## Task 5: Server — registry

**Files:**
- Create: `outlaw/server.py`
- Test: `tests/test_server.py`

**Interfaces:**
- Produces:
  - `Registry()` with `register(nick, addr, now)`, `touch(nick, now)`, `addr_of(nick) -> tuple | None`, `nick_of(addr) -> str | None`, `online() -> list[str]`, `evict_stale(now, timeout) -> list[str]`, `remove(nick)`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_server.py
from outlaw.server import Registry

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_server.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Write minimal implementation**

```python
# outlaw/server.py
class Registry:
    def __init__(self):
        self._by_nick = {}   # nick -> {"addr": addr, "seen": now}
        self._by_addr = {}   # addr -> nick

    def register(self, nick, addr, now):
        old = self._by_nick.get(nick)
        if old and old["addr"] in self._by_addr:
            del self._by_addr[old["addr"]]
        self._by_nick[nick] = {"addr": addr, "seen": now}
        self._by_addr[addr] = nick

    def touch(self, nick, now):
        if nick in self._by_nick:
            self._by_nick[nick]["seen"] = now

    def addr_of(self, nick):
        e = self._by_nick.get(nick)
        return e["addr"] if e else None

    def nick_of(self, addr):
        return self._by_addr.get(addr)

    def online(self):
        return list(self._by_nick.keys())

    def remove(self, nick):
        e = self._by_nick.pop(nick, None)
        if e:
            self._by_addr.pop(e["addr"], None)

    def evict_stale(self, now, timeout):
        stale = [n for n, e in self._by_nick.items() if now - e["seen"] > timeout]
        for n in stale:
            self.remove(n)
        return stale
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_server.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add outlaw/server.py tests/test_server.py
git commit -m "feat: server registry with eviction"
```

---

## Task 6: Server — routing engine (online delivery vs offline queue)

**Files:**
- Modify: `outlaw/server.py`
- Test: `tests/test_server.py`

**Interfaces:**
- Consumes: `Registry`, `outlaw.store.QueueStore`, `outlaw.protocol`
- Produces:
  - `Router(registry, queue, send)` where `send(addr, packet)` is a callback.
    - `handle(packet, addr, now)` — processes one decoded inbound packet, driving registration, routing, acks, queue flush. Returns None.

Routing rules inside `handle`:
- `t=="reg"`: register nick→addr; reply `{"t":"ack","on":[...]}`; broadcast online list; flush queued messages for that nick (send each, one datagram each).
- `t=="pi"`: touch; reply `{"t":"po"}`.
- `t=="lv"`: remove; broadcast online list.
- `t=="m"`: ack sender `{"t":"ma","id":...}`; if `to=="__all__"` deliver to all other online nicks (queue offline ones); else deliver to that nick if online else queue.
- `t=="ma"`: forwarded recipient ack — clear any pending retransmit (handled in Task 7's ReliableDelivery; here just ignore if unknown).

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_server.py
from outlaw.server import Router
from outlaw.store import QueueStore

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
    router.handle({"t": "m", "id": "x", "f": "a", "to": "__all__", "x": "hi", "s": 1, "n": 0, "c": 1}, ("1.1.1.1", 1), now=1)
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_server.py -v`
Expected: FAIL (`Router` not defined).

- [ ] **Step 3: Write minimal implementation**

```python
# add to outlaw/server.py
from outlaw import protocol as proto

class Router:
    def __init__(self, registry, queue, send):
        self.reg = registry
        self.q = queue
        self.send = send

    def _broadcast_online(self):
        pkt = {"t": "ack", "on": self.reg.online()}
        for nick in self.reg.online():
            self.send(self.reg.addr_of(nick), pkt)

    def _deliver_or_queue(self, nick, packet):
        addr = self.reg.addr_of(nick)
        if addr:
            self.send(addr, packet)
        else:
            self.q.enqueue(nick, packet)

    def handle(self, packet, addr, now):
        t = packet.get("t")
        if t == "reg":
            nick = packet.get("f")
            if not nick:
                return
            self.reg.register(nick, addr, now)
            self.send(addr, {"t": "ack", "on": self.reg.online()})
            self._broadcast_online()
            for m in self.q.drain(nick):
                self.send(addr, m)
        elif t == "pi":
            self.reg.touch(packet.get("f"), now)
            self.send(addr, {"t": "po"})
        elif t == "lv":
            self.reg.remove(packet.get("f"))
            self._broadcast_online()
        elif t == "m":
            self.reg.touch(packet.get("f"), now)
            self.send(addr, {"t": "ma", "id": packet.get("id")})
            to = packet.get("to")
            if to == proto.BROADCAST:
                for nick in self.reg.online():
                    if nick != packet.get("f"):
                        self._deliver_or_queue(nick, packet)
            elif to:
                self._deliver_or_queue(to, packet)
        # t == "ma": recipient ack; consumed by reliable layer, ignore here
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_server.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add outlaw/server.py tests/test_server.py
git commit -m "feat: server routing for broadcast, dm, and offline queue flush"
```

---

## Task 7: Server — asyncio protocol, heartbeat loop, run_server()

**Files:**
- Modify: `outlaw/server.py`
- Create: `server.py`
- Test: `tests/test_server.py`

**Interfaces:**
- Consumes: `Router`, `Registry`, `QueueStore`, `outlaw.protocol`
- Produces:
  - `ServerProtocol(router, debug_path)` — asyncio `DatagramProtocol`; `datagram_received` decodes and dispatches, wrapping errors.
  - `async def run_server(host="0.0.0.0", port=5005, data_dir="~/.outlaw")` — binds socket, loads queue, starts eviction+persist task, runs forever.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_server.py
import asyncio
from outlaw.server import ServerProtocol
from outlaw import protocol as proto

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_server.py -v`
Expected: FAIL (`ServerProtocol` not defined).

- [ ] **Step 3: Write minimal implementation**

```python
# add to outlaw/server.py
import asyncio
import os
from pathlib import Path
from outlaw.store import QueueStore, append_line

class ServerProtocol(asyncio.DatagramProtocol):
    def __init__(self, router, debug_path):
        self.router = router
        self.debug_path = Path(debug_path)
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        try:
            packet = proto.decode(data)
            if packet:
                self.router.handle(packet, addr, proto.now_ts())
        except Exception as e:
            try:
                append_line(self.debug_path, f"err {addr}: {e!r}")
            except Exception:
                pass

async def run_server(host="0.0.0.0", port=5005, data_dir="~/.outlaw"):
    data_dir = Path(os.path.expanduser(data_dir))
    queue = QueueStore(data_dir / "queue.json")
    queue.load()
    registry = Registry()
    loop = asyncio.get_running_loop()

    holder = {}
    def send(addr, packet):
        try:
            holder["transport"].sendto(proto.encode(packet), addr)
        except proto.PacketTooLarge:
            pass  # never emit an oversize datagram

    router = Router(registry, queue, send)
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: ServerProtocol(router, data_dir / "debug.log"),
        local_addr=(host, port),
    )
    holder["transport"] = transport
    print(f"[outlaw] relay up on {host}:{port}")

    async def housekeeping():
        while True:
            await asyncio.sleep(15)
            for nick in registry.evict_stale(proto.now_ts(), timeout=90):
                router._broadcast_online()
            queue.persist()

    try:
        await housekeeping()
    finally:
        transport.close()
        queue.persist()
```

```python
# server.py
import asyncio
from outlaw.server import run_server

if __name__ == "__main__":
    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        print("\n[outlaw] relay stopped")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_server.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add outlaw/server.py server.py tests/test_server.py
git commit -m "feat: asyncio server protocol, heartbeat/persist loop, entrypoint"
```

---

## Task 8: Commands — pure input parser

**Files:**
- Create: `outlaw/commands.py`
- Test: `tests/test_commands.py`

**Interfaces:**
- Produces:
  - `Action` = dict with a `"kind"` key. Kinds: `send` (`text`), `dm_mode` (`nick`), `group_mode`, `oneoff_dm` (`nick`,`text`), `nick` (`name`), `who`, `clear`, `help`, `quit`, `noop`, `error` (`msg`).
  - `parse_input(text: str) -> Action`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_commands.py
from outlaw.commands import parse_input

def test_plain_text_is_send():
    assert parse_input("hello world") == {"kind": "send", "text": "hello world"}

def test_empty_is_noop():
    assert parse_input("   ") == {"kind": "noop"}

def test_dm_mode():
    assert parse_input("/dm alice") == {"kind": "dm_mode", "nick": "alice"}

def test_group_mode():
    assert parse_input("/group") == {"kind": "group_mode"}

def test_oneoff_dm():
    assert parse_input("/msg bob hey there") == {"kind": "oneoff_dm", "nick": "bob", "text": "hey there"}

def test_nick_change():
    assert parse_input("/nick ghost") == {"kind": "nick", "name": "ghost"}

def test_simple_commands():
    assert parse_input("/who") == {"kind": "who"}
    assert parse_input("/clear") == {"kind": "clear"}
    assert parse_input("/help") == {"kind": "help"}
    assert parse_input("/quit") == {"kind": "quit"}

def test_unknown_command_is_error():
    assert parse_input("/frobnicate")["kind"] == "error"

def test_dm_without_arg_is_error():
    assert parse_input("/dm")["kind"] == "error"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_commands.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Write minimal implementation**

```python
# outlaw/commands.py
def parse_input(text: str) -> dict:
    s = text.strip()
    if not s:
        return {"kind": "noop"}
    if not s.startswith("/"):
        return {"kind": "send", "text": s}
    parts = s[1:].split(" ", 1)
    cmd = parts[0].lower()
    rest = parts[1].strip() if len(parts) > 1 else ""
    if cmd == "who":
        return {"kind": "who"}
    if cmd == "group":
        return {"kind": "group_mode"}
    if cmd == "clear":
        return {"kind": "clear"}
    if cmd == "help":
        return {"kind": "help"}
    if cmd == "quit":
        return {"kind": "quit"}
    if cmd == "dm":
        if not rest:
            return {"kind": "error", "msg": "usage: /dm <nick>"}
        return {"kind": "dm_mode", "nick": rest.split(" ", 1)[0]}
    if cmd == "nick":
        if not rest:
            return {"kind": "error", "msg": "usage: /nick <name>"}
        return {"kind": "nick", "name": rest.split(" ", 1)[0]}
    if cmd == "msg":
        bits = rest.split(" ", 1)
        if len(bits) < 2 or not bits[1].strip():
            return {"kind": "error", "msg": "usage: /msg <nick> <text>"}
        return {"kind": "oneoff_dm", "nick": bits[0], "text": bits[1].strip()}
    return {"kind": "error", "msg": f"unknown command: /{cmd}"}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_commands.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add outlaw/commands.py tests/test_commands.py
git commit -m "feat: pure command parser for slash commands"
```

---

## Task 9: Client network — ClientSession with register/retry, ping, send-with-ack

**Files:**
- Create: `outlaw/client_net.py`
- Test: `tests/test_integration.py` (session-level tests here; full loopback in Task 12)

**Interfaces:**
- Consumes: `outlaw.protocol`
- Produces:
  - `ClientSession(nick, server_addr, on_message, on_roster, on_link)` where callbacks are: `on_message(from, to, text, ts)`, `on_roster(list[str])`, `on_link(up: bool)`.
  - `async def start()` — creates datagram endpoint, registers, launches ping loop + retransmit sweeper.
  - `send_text(to, text)` — chunks text, assigns ids, sends each chunk, tracks unacked for retransmit.
  - `feed(packet)` — process one decoded inbound packet (splitting this out makes it unit-testable without sockets).
  - `set_nick(name)` — re-register under a new nick.
  - `leave()` — send `lv`.

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_integration.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Write minimal implementation**

```python
# outlaw/client_net.py
import asyncio
from outlaw import protocol as proto

class ClientSession:
    def __init__(self, nick, server_addr, on_message, on_roster, on_link):
        self.nick = nick
        self.server_addr = server_addr
        self.on_message = on_message
        self.on_roster = on_roster
        self.on_link = on_link
        self.transport = None
        self._pending = {}       # id -> {"packet": pkt, "tries": int, "at": ts}
        self._dupes = proto.DuplicateFilter()
        self._reasm = proto.ChunkReassembler()
        self._link_up = False
        self._closing = False

    # --- transport seam (overridden in tests) ---
    def _raw_send(self, packet):
        try:
            self.transport.sendto(proto.encode(packet), self.server_addr)
        except proto.PacketTooLarge:
            pass

    # --- inbound ---
    def feed(self, packet):
        t = packet.get("t")
        if t == "ack":
            if not self._link_up:
                self._link_up = True
                self.on_link(True)
            self.on_roster(packet.get("on", []))
        elif t == "po":
            if not self._link_up:
                self._link_up = True
                self.on_link(True)
        elif t == "ma":
            self._pending.pop(packet.get("id"), None)
        elif t == "m":
            mid = packet.get("id")
            self._raw_send({"t": "ma", "id": mid})
            if self._dupes.seen(mid + ":" + str(packet.get("n"))):
                return
            full = self._reasm.add(mid, packet.get("n", 0), packet.get("c", 1),
                                   packet.get("x", ""), proto.now_ts())
            if full is not None:
                self.on_message(packet.get("f"), packet.get("to"), full, packet.get("s"))

    # --- outbound ---
    def send_text(self, to, text):
        for n, chunk in enumerate(proto.chunk_text(text)):
            mid = proto.new_id()
            total = len(proto.chunk_text(text))
            pkt = proto.make_msg(self.nick, to, chunk, mid, n, total, proto.now_ts())
            self._pending[mid] = {"packet": pkt, "tries": 0, "at": proto.now_ts()}
            self._raw_send(pkt)

    def register(self):
        self._raw_send({"t": "reg", "f": self.nick})

    def set_nick(self, name):
        self.nick = name
        self.register()

    def leave(self):
        self._raw_send({"t": "lv", "f": self.nick})

    async def _ping_loop(self):
        while not self._closing:
            await asyncio.sleep(30)
            self._raw_send({"t": "pi", "f": self.nick})

    async def _retransmit_loop(self):
        while not self._closing:
            await asyncio.sleep(2)
            now = proto.now_ts()
            for mid, e in list(self._pending.items()):
                if now - e["at"] >= 2:
                    if e["tries"] >= 3:
                        del self._pending[mid]
                    else:
                        e["tries"] += 1
                        e["at"] = now
                        self._raw_send(e["packet"])

    async def _register_retry_loop(self):
        while not self._closing:
            if not self._link_up:
                self.register()
            await asyncio.sleep(10)

    async def start(self):
        loop = asyncio.get_running_loop()
        session = self
        class _Proto(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                session.transport = transport
            def datagram_received(self, data, addr):
                pkt = proto.decode(data)
                if pkt:
                    session.feed(pkt)
            def error_received(self, exc):
                pass
        await loop.create_datagram_endpoint(lambda: _Proto(), remote_addr=self.server_addr)
        self.register()
        asyncio.create_task(self._ping_loop())
        asyncio.create_task(self._retransmit_loop())
        asyncio.create_task(self._register_retry_loop())

    def close(self):
        self._closing = True
        if self.transport:
            self.transport.close()
```

> Note: `send_text` calls `chunk_text` twice; that is intentional simplicity — the list is small. If a reviewer objects, hoist it to a local `chunks = proto.chunk_text(text)` and use `len(chunks)`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_integration.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add outlaw/client_net.py tests/test_integration.py
git commit -m "feat: client session with register/retry, ping, ack-tracked send"
```

---

## Task 10: Client UI — Outlaw Textual app

**Files:**
- Create: `outlaw/ui.py`
- Manual test only (Textual UIs are validated via the pilot harness + manual run; logic lives in tested modules).

**Interfaces:**
- Consumes: `outlaw.commands.parse_input`, `ClientSession`, `outlaw.store`
- Produces:
  - `OutlawApp(session, nick, tunnel_ip)` — Textual `App` subclass. Wires input submission → `parse_input` → session calls; renders roster, messages, status.
  - Methods the session callbacks target (thread-safe via `call_from_thread`/`post_message`):
    - `show_message(frm, to, text, ts)`, `set_roster(users)`, `set_link(up)`.

- [ ] **Step 1: Write the app**

```python
# outlaw/ui.py
from datetime import datetime
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Header, Footer, Input, RichLog, Static
from outlaw.commands import parse_input
from outlaw import protocol as proto, store

BANNER = r"""
 ▄██████▄  ██    ██ ████████ ██       ▄███▄  ██     ██
██    ██  ██    ██    ██    ██      ██   ██  ██  █  ██
██    ██  ██    ██    ██    ██      ███████  ██ ███ ██
▀██████▀   ▀████▀     ██    ██████  ██   ██   ███ ███
        [ dns0 covert channel · udp relay · v1.0 ]
"""

class OutlawApp(App):
    CSS = """
    Screen { background: #000000; }
    #peers { width: 22; border: round #00ff41; color: #00ff41; }
    #stream { border: round #00ff41; }
    #status { height: 1; color: #ffb000; background: #0a0a0a; }
    Input { border: round #ffb000; color: #00ff41; background: #000000; }
    """
    BINDINGS = [("ctrl+c", "quit", "quit")]

    def __init__(self, session, nick, tunnel_ip, history_path):
        super().__init__()
        self.session = session
        self.nick = nick
        self.tunnel_ip = tunnel_ip
        self.history_path = history_path
        self.mode = ("group", None)   # ("group", None) or ("dm", nick)
        self.link_up = False
        self.tx = self.rx = 0

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal():
            yield Static("", id="peers")
            yield RichLog(id="stream", wrap=True, markup=True)
        yield Static(self._status_text(), id="status")
        yield Input(placeholder="type message or /command", id="entry")
        yield Footer()

    def on_mount(self):
        log = self.query_one("#stream", RichLog)
        for line in BANNER.splitlines():
            log.write(f"[#00ff41]{line}[/]")
        log.write("[#666666]> establishing tunnel ...[/]")
        self.query_one("#entry", Input).focus()

    def _status_text(self):
        mode = "group" if self.mode[0] == "group" else f"dm:{self.mode[1]}"
        link = "[#00ff41]● LINK UP[/]" if self.link_up else "[#ff3333]● LINK DOWN[/]"
        return f"[{mode}] {link}  tx:{self.tx} rx:{self.rx}  /who /dm /group /help /quit"

    def _refresh_status(self):
        self.query_one("#status", Static).update(self._status_text())

    def on_input_submitted(self, event: Input.Submitted):
        self.query_one("#entry", Input).value = ""
        action = parse_input(event.value)
        self._dispatch(action)

    def _dispatch(self, action):
        kind = action["kind"]
        log = self.query_one("#stream", RichLog)
        if kind == "noop":
            return
        if kind == "send":
            to = "__all__" if self.mode[0] == "group" else self.mode[1]
            self.session.send_text(to, action["text"])
            self.tx += 1
            self._echo_own(to, action["text"])
        elif kind == "oneoff_dm":
            self.session.send_text(action["nick"], action["text"])
            self.tx += 1
            self._echo_own(action["nick"], action["text"])
        elif kind == "dm_mode":
            self.mode = ("dm", action["nick"])
        elif kind == "group_mode":
            self.mode = ("group", None)
        elif kind == "nick":
            self.session.set_nick(action["name"])
            self.nick = action["name"]
            log.write(f"[#ffb000]* nick set to {action['name']}[/]")
        elif kind == "who":
            log.write("[#ffb000]* use the peers panel ←[/]")
        elif kind == "clear":
            log.clear()
        elif kind == "help":
            log.write("[#ffb000]/dm <nick> · /group · /msg <nick> <text> · /nick <name> · /who · /clear · /quit[/]")
        elif kind == "quit":
            self.session.leave()
            self.exit()
        elif kind == "error":
            log.write(f"[#ff3333]! {action['msg']}[/]")
        self._refresh_status()

    def _echo_own(self, to, text):
        ts = datetime.now().strftime("%H:%M")
        tgt = "" if to == "__all__" else f" » {to}"
        self.query_one("#stream", RichLog).write(f"[#ffb000]{ts} {self.nick}{tgt}  {text}[/]")
        store.append_line(self.history_path, f"{ts} {self.nick}{tgt}: {text}")

    # --- session callbacks (invoked from asyncio loop; same loop as Textual) ---
    def show_message(self, frm, to, text, ts):
        self.rx += 1
        stamp = datetime.now().strftime("%H:%M")
        tag = "" if to == "__all__" else " » you"
        self.query_one("#stream", RichLog).write(f"[#00ff41]{stamp} {frm}{tag}  {text}[/]")
        store.append_line(self.history_path, f"{stamp} {frm}{tag}: {text}")
        self._refresh_status()

    def set_roster(self, users):
        lines = "\n".join(f"[#00ff41]●[/] {u}" if u != self.nick else f"[#ffb000]●[/] {u} (you)" for u in users)
        self.query_one("#peers", Static).update(f"PEERS\n\n{lines}\n\n{len(users)} online")

    def set_link(self, up):
        self.link_up = up
        self._refresh_status()
```

- [ ] **Step 2: Smoke-test import & compose**

Run: `python -c "from outlaw.ui import OutlawApp; print('import ok')"`
Expected: prints `import ok` (after `pip install textual`).

- [ ] **Step 3: Commit**

```bash
git add outlaw/ui.py
git commit -m "feat: Outlaw Textual TUI with hacker aesthetic"
```

---

## Task 11: Client entrypoint — wire session to UI

**Files:**
- Create: `client.py`
- Modify: `requirements.txt`
- Manual test.

**Interfaces:**
- Consumes: `OutlawApp`, `ClientSession`, `outlaw.store.Config`

- [ ] **Step 1: Write the entrypoint**

```python
# client.py
import asyncio
import os
import sys
from pathlib import Path
from outlaw.store import Config
from outlaw.client_net import ClientSession
from outlaw.ui import OutlawApp

DATA_DIR = Path(os.path.expanduser("~/.outlaw"))
SERVER = (os.environ.get("OUTLAW_SERVER", "10.0.0.1"), int(os.environ.get("OUTLAW_PORT", "5005")))

def resolve_nick():
    cfg = Config(DATA_DIR / "config.json")
    nick = cfg.load()
    if not nick:
        nick = input("enter alias: ").strip() or "outlaw"
        cfg.save(nick)
    return nick

async def main():
    nick = resolve_nick()
    tunnel_ip = os.environ.get("OUTLAW_IP", "dns0")
    app = OutlawApp(session=None, nick=nick, tunnel_ip=tunnel_ip,
                    history_path=DATA_DIR / "history.log")
    session = ClientSession(
        nick, SERVER,
        on_message=lambda f, to, x, ts: app.call_from_thread(app.show_message, f, to, x, ts),
        on_roster=lambda users: app.call_from_thread(app.set_roster, users),
        on_link=lambda up: app.call_from_thread(app.set_link, up),
    )
    app.session = session
    await session.start()
    await app.run_async()
    session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
```

```
# requirements.txt
textual>=0.60
pytest>=7.0
```

- [ ] **Step 2: Smoke test**

Run: `pip install -r requirements.txt && python -c "import client; print('wired')"`
Expected: prints `wired`.

- [ ] **Step 3: Commit**

```bash
git add client.py requirements.txt
git commit -m "feat: client entrypoint wiring session to Outlaw UI"
```

---

## Task 12: Loopback integration test — end to end over real sockets

**Files:**
- Modify: `tests/test_integration.py`

**Interfaces:**
- Consumes: `run_server` (or a lighter in-process server harness), `ClientSession`

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_integration.py
import asyncio
import pytest
from outlaw.server import Registry, Router, ServerProtocol
from outlaw.store import QueueStore
from outlaw import protocol as proto

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
```

Add to `requirements.txt`: `pytest-asyncio>=0.21`. Add `tests/conftest.py` or `pytest.ini` with `asyncio_mode = auto`.

- [ ] **Step 2: Run and verify it fails, then passes**

Run: `pip install pytest-asyncio && python -m pytest tests/test_integration.py -v`
Expected: after wiring `asyncio_mode=auto`, all pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_integration.py requirements.txt pytest.ini
git commit -m "test: loopback integration for broadcast, dm, offline delivery"
```

---

## Task 13: README + Termux/iodine setup + manual checklist

**Files:**
- Create: `README.md`

**Interfaces:** none (docs).

- [ ] **Step 1: Write README**

Include, verbatim and complete:
- **What Outlaw is** (one paragraph): UDP chat relayed over an iodine DNS tunnel.
- **Server-side iodine setup:** `iodined -f -c -P <password> -m 250 10.0.0.1 tunnel.example.com`
- **Client-side iodine (Termux):** `pkg install iodine`, `iodine -f -P <password> tunnel.example.com`, confirm `dns0` and `ip addr` shows `10.0.0.x`.
- **Run the relay (VPS):** `python3 server.py`
- **Run Outlaw (Termux):** `pip install -r requirements.txt`, `OUTLAW_SERVER=10.0.0.1 python3 client.py`, enter alias.
- **Commands table:** `/who /dm /group /msg /nick /clear /help /quit`.
- **Manual test checklist:**
  1. Start iodined with `-m 250`; start `server.py`.
  2. Connect device 1 and device 2 via iodine; run `client.py` on both.
  3. Verify both appear in each other's peer panel.
  4. Send a group message from device 1; confirm it appears on device 2 (not echoed twice on device 1).
  5. `/dm <peer>` and send a private message; confirm only the target sees it.
  6. Send a message > 140 bytes; confirm it arrives intact (chunk reassembly).
  7. Close device 2's client; send it a DM from device 1; reopen device 2; confirm the queued message replays.
  8. Kill and restart `server.py`; confirm clients re-register within ~10s and the persisted queue survives.

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: README with iodine setup and manual test checklist"
```

---

## Self-Review Notes

- **Spec coverage:** relay hub (T5-7), nicks/identity (T4, T8, T11), group+DM (T6, T9-10), offline queue+replay (T4, T6, T12), 230-byte ceiling (T1), 140-byte cap + chunking (T2-3, T9), ACK/retransmit (T6, T9), duplicate suppression (T3, T9), heartbeat/eviction (T7, T9), persistence (T4, T7), TUI (T10), error handling (T1 decode, T7 protocol guard), tests (T1-9, T12), README/manual (T13). All spec sections mapped.
- **Type consistency:** `send(addr, packet)` callback signature consistent across Router/ServerProtocol/run_server/integration. `feed`, `send_text`, `_raw_send`, `_pending`, callback signatures `on_message(f,to,x,ts)/on_roster(list)/on_link(bool)` consistent between Task 9 and Task 11. Packet key set fixed in Task 1-2 and reused throughout.
- **Known simplification:** `send_text` computes `chunk_text` twice (noted inline) — acceptable, flagged for reviewer.
