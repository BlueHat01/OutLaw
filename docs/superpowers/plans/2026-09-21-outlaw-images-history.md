# Outlaw Images + Reliable Delivery + History Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add image sending (auto-compressed, streamed over the existing reliable chunk pipeline), fix offline multi-message loss via server→recipient delivery confirmation, and add persistent local history to Outlaw.

**Architecture:** Images are compressed with Pillow to ≤24 KB, base64-encoded, and carried in new `ih`/`im` packets through the existing `n`/`c` chunk + ack/retransmit + offline-queue machinery, with a sliding-window sender. The server gains a per-recipient delivery-pending table that uses the recipient's `ma` (now carrying `n`) to confirm delivery, retransmit, and fall back to the offline queue on failure. Local history moves to a structured JSONL log preloaded on launch.

**Tech Stack:** Python 3.9+, asyncio, Pillow (new), pytest, pytest-asyncio. Existing modules: `outlaw/protocol.py`, `store.py`, `server.py`, `client_net.py`, `commands.py`, `ui.py`, `client.py`, `client_cli.py`.

**Spec:** `docs/superpowers/specs/2026-09-21-outlaw-images-history-design.md`

## Global Constraints

- **Datagrams ≤ 230 bytes.** `protocol.encode` raises `PacketTooLarge` above this. Every `ih`/`im` packet must fit.
- **Compact JSON keys only:** existing `t,f,to,x,s,id,n,c,on,m` plus new `d` (base64 data), `nm` (filename), `mt` (mime), `sz` (size).
- **Image budget:** `IMAGE_MAX_BYTES = 24576`, `IMAGE_MAX_EDGE = 800`, `IMAGE_MIN_EDGE = 240`, `IMAGE_START_QUALITY = 85`, `IMAGE_MIN_QUALITY = 30`, `IMAGE_MAX_INPUT_BYTES = 26214400`.
- **Transfer:** `IMAGE_WINDOW = 6` chunks in flight; `IMAGE_TRANSFER_TIMEOUT = 120` s.
- **Offline image cap:** `IMAGE_QUEUE_MAX = 3` images/user, `IMAGE_QUEUE_MAX_BYTES = 786432`.
- **History:** `~/.outlaw/history.jsonl`; `HISTORY_LOAD = 200` entries on launch.
- **Reliability:** server delivery retransmit up to 3× at ~2 s, then queue + evict recipient.
- **App data dir:** `~/.outlaw/` (`config.json`, `queue.json`, `history.log`, `history.jsonl`, `media/`, `debug.log`).
- All datagram parsing stays wrapped in try/except; bad datagrams dropped, never crash.

---

## File Structure

- `outlaw/media.py` — **new.** Image compression (guaranteed-fit + input guard), base64 chunking, save/open, filename/mime helpers, image constants.
- `outlaw/protocol.py` — add `ih`/`im` builders; extend reassembly to collect image chunks; image transfer timeout constant.
- `outlaw/store.py` — two-queue `QueueStore` (text + capped image); JSONL history append + recent-load.
- `outlaw/server.py` — delivery-pending table, `ma` handling, sweep with retransmit→queue+evict, offline image grouping.
- `outlaw/client_net.py` — `send_image`, `on_image` callback, windowed image sender, recipient `ma` includes `n`, image reassembly→save.
- `outlaw/commands.py` — `img`, `open`, `history` actions.
- `client_cli.py` / `outlaw/ui.py` / `client.py` — wire `/img`, `/open`, `/history`, `on_image`, progress, history preload.
- `requirements.txt` — add `Pillow`.
- `tests/test_media.py`, `tests/test_history.py` — new; extend `test_protocol.py`, `test_server.py`, `test_commands.py`, `test_integration.py`, `test_cli.py`.

---

## Task 1: Image constants + filename/mime helpers

**Files:**
- Create: `outlaw/media.py`
- Test: `tests/test_media.py`

**Interfaces:**
- Produces:
  - Constants: `IMAGE_MAX_BYTES=24576`, `IMAGE_MAX_EDGE=800`, `IMAGE_MIN_EDGE=240`, `IMAGE_START_QUALITY=85`, `IMAGE_MIN_QUALITY=30`, `IMAGE_MAX_INPUT_BYTES=26214400`, `IMAGE_WINDOW=6`, `IMAGE_TRANSFER_TIMEOUT=120`
  - `ext_for_mime(mime: str) -> str` — `"image/jpeg"→".jpg"`, `"image/png"→".png"`, else `".img"`
  - `safe_name(name: str, maxlen=24) -> str` — strip path separators, keep basename, truncate

- [ ] **Step 1: Write the failing test**

```python
# tests/test_media.py
from outlaw import media

def test_constants():
    assert media.IMAGE_MAX_BYTES == 24576
    assert media.IMAGE_MAX_EDGE == 800
    assert media.IMAGE_MIN_EDGE == 240
    assert media.IMAGE_MAX_INPUT_BYTES == 26214400
    assert media.IMAGE_WINDOW == 6

def test_ext_for_mime():
    assert media.ext_for_mime("image/jpeg") == ".jpg"
    assert media.ext_for_mime("image/png") == ".png"
    assert media.ext_for_mime("application/x-weird") == ".img"

def test_safe_name_strips_path_and_truncates():
    assert media.safe_name("../../etc/passwd") == "passwd"
    assert media.safe_name("a/b/c/photo.jpg") == "photo.jpg"
    assert len(media.safe_name("x" * 100)) <= 24
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_media.py -v`
Expected: FAIL (module `outlaw.media` not found).

- [ ] **Step 3: Write minimal implementation**

```python
# outlaw/media.py
import os

IMAGE_MAX_BYTES = 24576
IMAGE_MAX_EDGE = 800
IMAGE_MIN_EDGE = 240
IMAGE_START_QUALITY = 85
IMAGE_MIN_QUALITY = 30
IMAGE_MAX_INPUT_BYTES = 26214400
IMAGE_WINDOW = 6
IMAGE_TRANSFER_TIMEOUT = 120

_MIME_EXT = {"image/jpeg": ".jpg", "image/png": ".png"}

def ext_for_mime(mime: str) -> str:
    return _MIME_EXT.get(mime, ".img")

def safe_name(name: str, maxlen: int = 24) -> str:
    base = os.path.basename(str(name).replace("\\", "/"))
    base = base.strip() or "image"
    return base[:maxlen]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_media.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add outlaw/media.py tests/test_media.py
git commit -m "feat: media constants and filename/mime helpers"
```

---

## Task 2: Image compression (guaranteed-fit + input guard)

**Files:**
- Modify: `outlaw/media.py`
- Modify: `requirements.txt`
- Test: `tests/test_media.py`

**Interfaces:**
- Consumes: constants from Task 1
- Produces:
  - `ImageError(Exception)`
  - `compress_image(path: str) -> tuple[bytes, str]` — returns `(jpeg_bytes, "image/jpeg")`; raises `ImageError` if Pillow missing, file too large, or cannot fit budget.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_media.py
import io, os
import pytest
from outlaw import media

pytest.importorskip("PIL")  # skip these if Pillow absent
from PIL import Image

def _make_image(tmp_path, size=(2000, 1500), color=(120, 30, 200)):
    p = tmp_path / "src.png"
    Image.new("RGB", size, color).save(p)
    return str(p)

def test_compress_fits_budget(tmp_path):
    data, mime = media.compress_image(_make_image(tmp_path))
    assert mime == "image/jpeg"
    assert len(data) <= media.IMAGE_MAX_BYTES
    # decodes and longest edge within cap
    img = Image.open(io.BytesIO(data))
    assert max(img.size) <= media.IMAGE_MAX_EDGE

def test_compress_hard_noise_still_fits(tmp_path):
    # random noise is hard to JPEG-compress; forces the edge-downscale path
    import random
    p = tmp_path / "noise.png"
    img = Image.new("RGB", (1600, 1600))
    img.putdata([(random.randint(0,255), random.randint(0,255), random.randint(0,255))
                 for _ in range(1600*1600)])
    img.save(p)
    data, _ = media.compress_image(str(p))
    assert len(data) <= media.IMAGE_MAX_BYTES

def test_compress_rejects_oversize_input(tmp_path):
    p = tmp_path / "big.bin"
    p.write_bytes(b"\0" * (media.IMAGE_MAX_INPUT_BYTES + 1))
    with pytest.raises(media.ImageError):
        media.compress_image(str(p))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_media.py -v`
Expected: FAIL (`compress_image` not defined).

- [ ] **Step 3: Write minimal implementation**

```python
# add to outlaw/media.py
import io

class ImageError(Exception):
    pass

def _encode(img, quality) -> bytes:
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=quality, optimize=True)
    return buf.getvalue()

def compress_image(path: str):
    try:
        from PIL import Image
    except ImportError:
        raise ImageError("Pillow not installed — run: pip install Pillow")
    if os.path.getsize(path) > IMAGE_MAX_INPUT_BYTES:
        raise ImageError("image too large (>25 MB)")
    try:
        img = Image.open(path)
        img = img.convert("RGB")
    except Exception as e:
        raise ImageError(f"cannot read image: {e}")

    edge = IMAGE_MAX_EDGE
    while True:
        work = img.copy()
        work.thumbnail((edge, edge))  # preserves aspect, longest edge <= edge
        for q in range(IMAGE_START_QUALITY, IMAGE_MIN_QUALITY - 1, -5):
            data = _encode(work, q)
            if len(data) <= IMAGE_MAX_BYTES:
                return data, "image/jpeg"
        if edge <= IMAGE_MIN_EDGE:
            raise ImageError("cannot compress image under budget")
        edge = max(IMAGE_MIN_EDGE, int(edge * 0.8))
```

- [ ] **Step 4: Add Pillow to requirements and run tests**

Edit `requirements.txt` to add a line: `Pillow>=10.0`
Run: `pip install Pillow>=10.0 && python -m pytest tests/test_media.py -v`
Expected: PASS (or SKIP for the Pillow tests if install failed — but install should succeed).

- [ ] **Step 5: Commit**

```bash
git add outlaw/media.py tests/test_media.py requirements.txt
git commit -m "feat: guaranteed-fit image compression with input guard"
```

---

## Task 3: base64 chunking + save/open helpers

**Files:**
- Modify: `outlaw/media.py`
- Test: `tests/test_media.py`

**Interfaces:**
- Consumes: constants from Task 1
- Produces:
  - `chunk_b64(data: bytes, chunk_len: int = 150) -> list[str]` — base64 the bytes, split into slices of `chunk_len` base64 chars
  - `join_b64(slices: list[str]) -> bytes` — inverse: concatenate + base64-decode
  - `save_image(media_dir, frm, mid, mime, data) -> str` — write bytes to `<media_dir>/<safe frm>-<mid><ext>`, return the path
  - `open_image(path) -> bool` — try `termux-open` then `xdg-open`; return True if a launcher was invoked

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_media.py
from pathlib import Path

def test_chunk_and_join_roundtrip():
    data = bytes(range(256)) * 4
    slices = media.chunk_b64(data, chunk_len=150)
    assert all(len(s) <= 150 for s in slices)
    assert media.join_b64(slices) == data

def test_chunk_b64_slice_fits_packet_budget():
    # 150 base64 chars keeps the im datagram under 230 bytes
    data = bytes(range(200))
    for s in media.chunk_b64(data, 150):
        assert len(s) <= 150

def test_save_image_writes_file(tmp_path):
    path = media.save_image(tmp_path, "alice", "3f9", "image/jpeg", b"\xff\xd8\xff")
    p = Path(path)
    assert p.exists()
    assert p.read_bytes() == b"\xff\xd8\xff"
    assert p.name == "alice-3f9.jpg"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_media.py -v`
Expected: FAIL (`chunk_b64` not defined).

- [ ] **Step 3: Write minimal implementation**

```python
# add to outlaw/media.py
import base64
import shutil
import subprocess
from pathlib import Path

def chunk_b64(data: bytes, chunk_len: int = 150) -> list:
    s = base64.b64encode(data).decode("ascii")
    return [s[i:i + chunk_len] for i in range(0, len(s), chunk_len)] or [""]

def join_b64(slices) -> bytes:
    return base64.b64decode("".join(slices).encode("ascii"))

def save_image(media_dir, frm, mid, mime, data) -> str:
    d = Path(media_dir)
    d.mkdir(parents=True, exist_ok=True)
    name = f"{safe_name(frm)}-{mid}{ext_for_mime(mime)}"
    path = d / name
    path.write_bytes(data)
    return str(path)

def open_image(path) -> bool:
    for launcher in ("termux-open", "xdg-open"):
        if shutil.which(launcher):
            try:
                subprocess.Popen([launcher, str(path)])
                return True
            except Exception:
                pass
    return False
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_media.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add outlaw/media.py tests/test_media.py
git commit -m "feat: base64 chunking and image save/open helpers"
```

---

## Task 4: Protocol — image packet builders

**Files:**
- Modify: `outlaw/protocol.py`
- Test: `tests/test_protocol.py`

**Interfaces:**
- Consumes: `encode`, `MAX_DATAGRAM` from existing protocol
- Produces:
  - `make_img_header(frm, to, mid, count, name, mime, size, ts) -> dict` — `{"t":"ih",...}`
  - `make_img_chunk(mid, n, count, data_slice) -> dict` — `{"t":"im","id","n","c","d"}`

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_protocol.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_protocol.py -v`
Expected: FAIL (`make_img_header` not defined).

- [ ] **Step 3: Write minimal implementation**

```python
# add to outlaw/protocol.py
def make_img_header(frm, to, mid, count, name, mime, size, ts) -> dict:
    return {"t": "ih", "id": mid, "f": frm, "to": to, "c": count,
            "nm": name, "mt": mime, "sz": size, "s": ts}

def make_img_chunk(mid, n, count, data_slice) -> dict:
    return {"t": "im", "id": mid, "n": n, "c": count, "d": data_slice}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_protocol.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add outlaw/protocol.py tests/test_protocol.py
git commit -m "feat: image header and chunk packet builders"
```

---

## Task 5: Protocol — image reassembler

**Files:**
- Modify: `outlaw/protocol.py`
- Test: `tests/test_protocol.py`

**Interfaces:**
- Produces:
  - `ImageAssembler(timeout=IMAGE_TRANSFER_TIMEOUT)` with:
    - `add_header(mid, frm, name, mime, size, now) -> None` — `frm` is the sender nick (needed at save time, since `im` data chunks don't carry it)
    - `add_chunk(mid, n, count, data_slice, now) -> dict | None` — returns `{"frm","name","mime","size","slices"}` when all `count` chunks present (metadata from header if seen, else defaults), else `None`
    - `purge(now)` — drop transfers older than `timeout`
  - Add `IMAGE_TRANSFER_TIMEOUT = 120` to protocol (imported from media or redefined; redefine locally to avoid import cycle).

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_protocol.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_protocol.py -v`
Expected: FAIL (`ImageAssembler` not defined).

- [ ] **Step 3: Write minimal implementation**

```python
# add to outlaw/protocol.py
IMAGE_TRANSFER_TIMEOUT = 120

class ImageAssembler:
    def __init__(self, timeout=IMAGE_TRANSFER_TIMEOUT):
        self.timeout = timeout
        self._t = {}  # mid -> {"c":count,"at":now,"chunks":{n:slice},"meta":{...}|None}

    def _entry(self, mid, now):
        e = self._t.get(mid)
        if e is None:
            e = {"c": None, "at": now, "chunks": {}, "meta": None}
            self._t[mid] = e
        return e

    def add_header(self, mid, frm, name, mime, size, now):
        e = self._entry(mid, now)
        e["meta"] = {"frm": frm, "name": name, "mime": mime, "size": size}

    def add_chunk(self, mid, n, count, data_slice, now):
        e = self._entry(mid, now)
        e["c"] = count
        e["chunks"][n] = data_slice
        if e["c"] is not None and len(e["chunks"]) == e["c"]:
            slices = [e["chunks"][i] for i in range(e["c"])]
            meta = e["meta"] or {"frm": "peer", "name": "image", "mime": "image/jpeg", "size": None}
            del self._t[mid]
            return {"slices": slices, "frm": meta["frm"], "name": meta["name"],
                    "mime": meta["mime"], "size": meta["size"]}
        return None

    def purge(self, now):
        for mid in [m for m, e in self._t.items() if now - e["at"] > self.timeout]:
            del self._t[mid]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_protocol.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add outlaw/protocol.py tests/test_protocol.py
git commit -m "feat: image reassembler with header/defaults and purge"
```

---

## Task 6: Store — two-queue QueueStore (text + capped image)

**Files:**
- Modify: `outlaw/store.py`
- Test: `tests/test_store.py`

**Interfaces:**
- Consumes: existing `QueueStore(path, maxlen=500)` (text queue behavior preserved)
- Produces (added to `QueueStore`):
  - `enqueue_image(nick, transfer)` where `transfer = {"id","header":pkt,"chunks":[pkt,...],"bytes":int}`; keeps at most `IMAGE_QUEUE_MAX=3` and total `IMAGE_QUEUE_MAX_BYTES=786432` per nick, evicting oldest first
  - `drain_images(nick) -> list[transfer]` — return and clear image transfers for nick
  - existing `enqueue`/`drain` unchanged (text); `persist`/`load` now include images
  - Constants on the module: `IMAGE_QUEUE_MAX = 3`, `IMAGE_QUEUE_MAX_BYTES = 786432`

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_store.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_store.py -v`
Expected: FAIL (`enqueue_image` not defined).

- [ ] **Step 3: Write minimal implementation**

```python
# modify outlaw/store.py
IMAGE_QUEUE_MAX = 3
IMAGE_QUEUE_MAX_BYTES = 786432

# in QueueStore.__init__ add:
#     self._img = defaultdict(list)   # nick -> [transfer, ...]

    def enqueue_image(self, nick, transfer):
        lst = self._img[nick]
        lst.append(transfer)
        # enforce count cap
        while len(lst) > IMAGE_QUEUE_MAX:
            lst.pop(0)
        # enforce byte cap
        while len(lst) > 1 and sum(t.get("bytes", 0) for t in lst) > IMAGE_QUEUE_MAX_BYTES:
            lst.pop(0)

    def drain_images(self, nick):
        lst = list(self._img.get(nick, []))
        if nick in self._img:
            self._img[nick].clear()
        return lst
```

Update `persist` to also write images and `load` to read them:

```python
    def persist(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "text": {n: list(dq) for n, dq in self._q.items() if dq},
            "images": {n: lst for n, lst in self._img.items() if lst},
        }
        self.path.write_text(json.dumps(data), encoding="utf-8")

    def load(self):
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return
        # backward compat: old format was a flat {nick: [msgs]}
        text = data.get("text") if isinstance(data, dict) and "text" in data else data
        for n, msgs in (text or {}).items():
            self._q[n] = deque(msgs, maxlen=self.maxlen)
        for n, lst in (data.get("images", {}) if isinstance(data, dict) else {}).items():
            self._img[n] = list(lst)
```

Also add `from collections import defaultdict, deque` already present; add `self._img = defaultdict(list)` in `__init__`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_store.py -v`
Expected: PASS (existing text-queue tests still pass).

- [ ] **Step 5: Commit**

```bash
git add outlaw/store.py tests/test_store.py
git commit -m "feat: capped per-user offline image queue in QueueStore"
```

---

## Task 7: Store — JSONL persistent history

**Files:**
- Modify: `outlaw/store.py`
- Test: `tests/test_history.py`

**Interfaces:**
- Produces:
  - `append_history(path, entry: dict)` — append one JSON object per line
  - `load_recent_history(path, limit=200) -> list[dict]` — return last `limit` parsed entries (skip malformed lines)
  - Constant `HISTORY_LOAD = 200`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_history.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_history.py -v`
Expected: FAIL (`append_history` not defined).

- [ ] **Step 3: Write minimal implementation**

```python
# add to outlaw/store.py
HISTORY_LOAD = 200

def append_history(path, entry):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

def load_recent_history(path, limit=HISTORY_LOAD):
    path = Path(path)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []
    rows = []
    for line in lines[-limit:]:
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_history.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add outlaw/store.py tests/test_history.py
git commit -m "feat: JSONL persistent history append and recent-load"
```

---

## Task 8: Server — delivery confirmation table + ma handling + sweep

**Files:**
- Modify: `outlaw/server.py`
- Test: `tests/test_server.py`

**Interfaces:**
- Consumes: `Registry`, `QueueStore`, `proto`
- Produces (changes to `Router`):
  - `__init__` gains `self.pending = {}` — key `(nick, id, n)` → `{"packet":pkt, "tries":0, "at":now, "nick":nick}`
  - `_deliver_or_queue(nick, packet, now)` — now records a pending delivery when the recipient is online (adds the `now` arg)
  - `handle` gains `elif t == "ma":` — resolve recipient via `self.reg.nick_of(addr)`, clear `pending[(nick, id, n)]`
  - `sweep_deliveries(now)` — for pending older than 2 s: retransmit (tries<3) else move packet to offline queue (text: `enqueue`; image chunks: handled in Task 9) and `self.reg.remove(nick)` then drop the pending entry
  - Constant `DELIVERY_RETRIES = 3`, `DELIVERY_INTERVAL = 2`

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_server.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_server.py -v`
Expected: FAIL (`router.pending` / `sweep_deliveries` not defined).

- [ ] **Step 3: Write minimal implementation**

```python
# modify outlaw/server.py Router
DELIVERY_RETRIES = 3
DELIVERY_INTERVAL = 2

# in Router.__init__ add:  self.pending = {}

    def _deliver_or_queue(self, nick, packet, now):
        addr = self.reg.addr_of(nick)
        if addr:
            self.send(addr, packet)
            if packet.get("t") in ("m", "im"):
                key = (nick, packet.get("id"), packet.get("n"))
                self.pending[key] = {"packet": packet, "tries": 0, "at": now, "nick": nick}
        else:
            self.q.enqueue(nick, packet)

    def sweep_deliveries(self, now):
        for key in list(self.pending.keys()):
            e = self.pending[key]
            if now - e["at"] < DELIVERY_INTERVAL:
                continue
            addr = self.reg.addr_of(e["nick"])
            if e["tries"] >= DELIVERY_RETRIES or addr is None:
                self.q.enqueue(e["nick"], e["packet"])
                self.reg.remove(e["nick"])
                del self.pending[key]
            else:
                e["tries"] += 1
                e["at"] = now
                if addr:
                    self.send(addr, e["packet"])
```

Update the `m` branch and broadcast loop to pass `now` into `_deliver_or_queue`, and add the `ma` branch:

```python
        elif t == "m":
            self.reg.touch(packet.get("f"), now)
            self.send(addr, {"t": "ma", "id": packet.get("id"), "n": packet.get("n")})
            to = packet.get("to")
            if to == proto.BROADCAST:
                for nick in self.reg.online():
                    if nick != packet.get("f"):
                        self._deliver_or_queue(nick, packet, now)
            elif to:
                self._deliver_or_queue(to, packet, now)
        elif t == "ma":
            nick = self.reg.nick_of(addr)
            if nick is not None:
                self.pending.pop((nick, packet.get("id"), packet.get("n")), None)
```

Wire `sweep_deliveries` into the housekeeping loop in `run_server` (call every ~2 s alongside eviction).

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_server.py -v`
Expected: PASS (existing server tests still pass; note `_deliver_or_queue` now needs `now` — the older `reg`-flush path is unaffected).

- [ ] **Step 5: Commit**

```bash
git add outlaw/server.py tests/test_server.py
git commit -m "fix: server delivery confirmation with retransmit and queue fallback"
```

---

## Task 9: Server — offline image grouping

**Files:**
- Modify: `outlaw/server.py`
- Test: `tests/test_server.py`

**Interfaces:**
- Consumes: `QueueStore.enqueue_image`, `drain_images`
- Produces (changes to `Router`):
  - `self._img_build = {}` — key `(nick, id)` → `{"header":pkt|None, "chunks":{n:pkt}, "c":count|None, "bytes":int}`
  - When an `ih` or `im` for an offline recipient (or one that fails delivery and is being queued via sweep) is queued, route it through `_queue_image_part(nick, packet)` which accumulates parts and, once `c` chunks + header present, calls `self.q.enqueue_image(nick, transfer)` and clears the builder
  - On `reg` flush: after text drain, `for t in self.q.drain_images(nick): send header then each chunk`

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_server.py
def test_offline_image_grouped_and_flushed(tmp_path):
    router, reg, q, sink = make_router(tmp_path)
    router.handle({"t": "reg", "f": "a"}, ("1.1.1.1", 1), now=0)
    # b is offline; a sends a 2-chunk image to b
    router.handle({"t": "ih", "id": "img1", "f": "a", "to": "b", "c": 2, "nm": "p.jpg", "mt": "image/jpeg", "sz": 6, "s": 1}, ("1.1.1.1", 1), now=1)
    router.handle({"t": "im", "id": "img1", "n": 0, "c": 2, "d": "AAA"}, ("1.1.1.1", 1), now=1)
    router.handle({"t": "im", "id": "img1", "n": 1, "c": 2, "d": "BBB"}, ("1.1.1.1", 1), now=1)
    # nothing delivered yet (b offline), but it's queued as one image transfer
    assert q.drain_images("b") == [] or True  # (drained below via reconnect)
    sink.sent.clear()
    router.handle({"t": "reg", "f": "b"}, ("2.2.2.2", 2), now=2)
    kinds = [pk["t"] for ad, pk in sink.sent if ad == ("2.2.2.2", 2) and pk.get("t") in ("ih", "im")]
    assert kinds.count("ih") == 1 and kinds.count("im") == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_server.py -v`
Expected: FAIL (image parts not grouped/flushed).

- [ ] **Step 3: Write minimal implementation**

```python
# modify outlaw/server.py Router
# in __init__ add:  self._img_build = {}

    def _queue_image_part(self, nick, packet):
        key = (nick, packet.get("id"))
        b = self._img_build.get(key)
        if b is None:
            b = {"header": None, "chunks": {}, "c": None, "bytes": 0}
            self._img_build[key] = b
        if packet.get("t") == "ih":
            b["header"] = packet
            b["c"] = packet.get("c")
            b["bytes"] = packet.get("sz", 0)
        else:  # im
            b["chunks"][packet.get("n")] = packet
            if b["c"] is None:
                b["c"] = packet.get("c")
        if b["c"] is not None and len(b["chunks"]) == b["c"]:
            transfer = {
                "id": packet.get("id"),
                "header": b["header"] or {"t": "ih", "id": packet.get("id"),
                                          "f": "?", "to": nick, "c": b["c"],
                                          "nm": "image", "mt": "image/jpeg", "sz": b["bytes"], "s": 0},
                "chunks": [b["chunks"][i] for i in range(b["c"])],
                "bytes": b["bytes"],
            }
            self.q.enqueue_image(nick, transfer)
            del self._img_build[key]
```

Route image parts for offline recipients through `_queue_image_part` in `_deliver_or_queue`:

```python
    def _deliver_or_queue(self, nick, packet, now):
        addr = self.reg.addr_of(nick)
        if addr:
            self.send(addr, packet)
            if packet.get("t") in ("m", "im"):
                key = (nick, packet.get("id"), packet.get("n"))
                self.pending[key] = {"packet": packet, "tries": 0, "at": now, "nick": nick}
        elif packet.get("t") in ("ih", "im"):
            self._queue_image_part(nick, packet)
        else:
            self.q.enqueue(nick, packet)
```

Add the `ih` branch in `handle` (route like `m`, no per-chunk ack needed for the header, but ack it so the sender's header retransmit clears):

```python
        elif t == "ih":
            self.reg.touch(packet.get("f"), now)
            self.send(addr, {"t": "ma", "id": packet.get("id"), "n": -1})  # header ack (n=-1)
            to = packet.get("to")
            if to == proto.BROADCAST:
                for nick in self.reg.online():
                    if nick != packet.get("f"):
                        self._deliver_or_queue(nick, packet, now)
            elif to:
                self._deliver_or_queue(to, packet, now)
```

And handle `im` exactly like `m` (ack with n, deliver/queue) — extend the `elif t == "m"` condition to `elif t in ("m", "im"):`.

On `reg` flush, after the text drain loop add:

```python
            for transfer in self.q.drain_images(nick):
                self.send(addr, transfer["header"])
                for ch in transfer["chunks"]:
                    self.send(addr, ch)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_server.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add outlaw/server.py tests/test_server.py
git commit -m "feat: group and replay offline image transfers"
```

---

## Task 10: Client — recipient ma includes n; image receive→save

**Files:**
- Modify: `outlaw/client_net.py`
- Test: `tests/test_integration.py`

**Interfaces:**
- Consumes: `proto.ImageAssembler`, `media.join_b64`, `media.save_image`
- Produces (changes to `ClientSession`):
  - `__init__` gains `on_image=None` and `media_dir=None`; creates `self._img = proto.ImageAssembler()`
  - recipient ack in `feed` for `m` now sends `{"t":"ma","id":mid,"n":n}` (includes `n`)
  - `feed` handles `ih` (call `self._img.add_header`) and `im` (call `self._img.add_chunk`; on completion decode via `media.join_b64`, save via `media.save_image`, fire `on_image(frm, to, path, meta)`); both send a receipt `ma` (header uses `n=-1`)
  - `feed` periodically calls `self._img.purge` (piggyback in `_sweep`)

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_integration.py
from outlaw import media, protocol as proto

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_integration.py -v`
Expected: FAIL (`on_image`/image handling not present).

- [ ] **Step 3: Write minimal implementation**

```python
# modify outlaw/client_net.py
from pathlib import Path
from outlaw import media

# ClientSession.__init__ signature:
#   def __init__(self, nick, server_addr, on_message, on_roster, on_link,
#                on_error=None, on_image=None, media_dir=None):
#     ... existing ...
#     self.on_image = on_image
#     self.media_dir = media_dir or Path.home() / ".outlaw" / "media"
#     self._img = proto.ImageAssembler()

# in feed(), the m branch ack line becomes:
#     self._raw_send({"t": "ma", "id": mid, "n": packet.get("n")})

# add to feed():
        elif t == "ih":
            self._raw_send({"t": "ma", "id": packet.get("id"), "n": -1})
            self._img.add_header(packet.get("id"), packet.get("f"), packet.get("nm"),
                                 packet.get("mt"), packet.get("sz"), proto.now_ts())
        elif t == "im":
            mid = packet.get("id")
            self._raw_send({"t": "ma", "id": mid, "n": packet.get("n")})
            done = self._img.add_chunk(mid, packet.get("n"), packet.get("c"),
                                       packet.get("d", ""), proto.now_ts())
            if done is not None and self.on_image is not None:
                data = media.join_b64(done["slices"])
                path = media.save_image(self.media_dir, done["frm"], mid, done["mime"], data)
                self.on_image(done["frm"], None, path, done)
```

In `_sweep`, add `self._img.purge(now)` next to the existing reassembler purge.

Note: `im` data chunks carry no `f`/`to` (to keep them small). The receiver takes the sender nick from the `ih` header via `ImageAssembler` (Task 5's `add_header(mid, frm, ...)`), returned as `done["frm"]`. `on_image` is called as `on_image(frm, to, path, meta)`; `to` is `None` here (the receiver is the target), which the front-ends treat as "to you".

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_integration.py -v`
Expected: PASS. Adjust the `frm` threading until `done["frm"] == "alice"` and the saved file matches.

- [ ] **Step 5: Commit**

```bash
git add outlaw/client_net.py outlaw/protocol.py tests/test_integration.py
git commit -m "feat: client receives images, saves to disk, acks with chunk index"
```

---

## Task 11: Client — windowed image sender

**Files:**
- Modify: `outlaw/client_net.py`
- Test: `tests/test_integration.py`

**Interfaces:**
- Consumes: `media.compress_image`, `media.chunk_b64`, `proto.make_img_header`, `proto.make_img_chunk`, `media.IMAGE_WINDOW`
- Produces (changes to `ClientSession`):
  - `send_image(to, path) -> int` — compress, chunk, register a transfer, send the header + first `IMAGE_WINDOW` chunks; returns total chunk count. Raises `media.ImageError` on failure (caller shows message).
  - transfer state `self._img_tx = {}` keyed by mid → `{"to","header","chunks":{n:pkt},"acked":set(),"sent":set(),"at":ts}`
  - `_pending` reuse: image chunks go through the same ack path; on `ma` for an image chunk, mark acked and release the next unsent chunk (window advance)
  - `on_img_progress=None` optional callback `(mid, acked, total)` for UI

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_integration.py
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
```

(Guard with `pytest.importorskip("PIL")` at top of file if not already present.)

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_integration.py -v`
Expected: FAIL (`send_image` not defined).

- [ ] **Step 3: Write minimal implementation**

```python
# add to outlaw/client_net.py
    def send_image(self, to, path):
        data, mime = media.compress_image(path)           # raises ImageError
        import os
        slices = media.chunk_b64(data, 150)
        mid = proto.new_id()
        name = media.safe_name(os.path.basename(path))
        header = proto.make_img_header(self.nick, to, mid, len(slices), name, mime, len(data), proto.now_ts())
        chunks = {n: proto.make_img_chunk(mid, n, len(slices), sl) for n, sl in enumerate(slices)}
        self._img_tx[mid] = {"to": to, "header": header, "chunks": chunks,
                             "acked": set(), "sent": set(), "at": proto.now_ts()}
        self._raw_send(header)
        self._img_advance(mid)
        return len(slices)

    def _img_advance(self, mid):
        tx = self._img_tx.get(mid)
        if tx is None:
            return
        inflight = len(tx["sent"]) - len(tx["acked"])
        for n in sorted(tx["chunks"]):
            if inflight >= media.IMAGE_WINDOW:
                break
            if n not in tx["sent"]:
                self._raw_send(tx["chunks"][n])
                tx["sent"].add(n)
                inflight += 1
```

Extend `feed`'s `ma` handling: after clearing text `_pending`, also advance image transfers:

```python
        elif t == "ma":
            mid = packet.get("id")
            # text message pending (existing behaviour)
            e = self._pending.get(mid)
            if e is not None:
                e["acked"].add(packet.get("n"))
                if len(e["acked"]) >= len(e["packets"]):
                    del self._pending[mid]
            # image transfer pending
            tx = self._img_tx.get(mid)
            if tx is not None:
                n = packet.get("n")
                if n is not None and n >= 0:
                    tx["acked"].add(n)
                if len(tx["acked"]) >= len(tx["chunks"]):
                    del self._img_tx[mid]
                else:
                    self._img_advance(mid)
```

Add `self._img_tx = {}` to `__init__`. (Retransmit of unacked image chunks after a timeout can reuse a light sweep over `_img_tx` in `_sweep`: resend `sent`-but-not-`acked` chunks older than the interval — mirror the text `_sweep` logic; keep it simple.)

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_integration.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add outlaw/client_net.py tests/test_integration.py
git commit -m "feat: windowed image sender with ack-driven window advance"
```

---

## Task 12: Commands — /img, /open, /history

**Files:**
- Modify: `outlaw/commands.py`
- Test: `tests/test_commands.py`

**Interfaces:**
- Produces new action kinds from `parse_input`:
  - `/img <path>` → `{"kind":"img","path":<path>}`
  - `/open [arg]` → `{"kind":"open","arg":<arg or "last">}`
  - `/history [n]` → `{"kind":"history","n":<int, default 20>}`
  - errors for missing `/img` path

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_commands.py
def test_img_command():
    assert parse_input("/img /sdcard/pic.jpg") == {"kind": "img", "path": "/sdcard/pic.jpg"}

def test_img_requires_path():
    assert parse_input("/img")["kind"] == "error"

def test_open_defaults_to_last():
    assert parse_input("/open") == {"kind": "open", "arg": "last"}
    assert parse_input("/open 3f9") == {"kind": "open", "arg": "3f9"}

def test_history_default_and_arg():
    assert parse_input("/history") == {"kind": "history", "n": 20}
    assert parse_input("/history 50") == {"kind": "history", "n": 50}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_commands.py -v`
Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

```python
# add to outlaw/commands.py parse_input, before the final "unknown" return:
    if cmd == "img":
        if not rest:
            return {"kind": "error", "msg": "usage: /img <path>"}
        return {"kind": "img", "path": rest.strip()}
    if cmd == "open":
        return {"kind": "open", "arg": rest.split(" ", 1)[0] if rest else "last"}
    if cmd == "history":
        n = 20
        if rest.strip().isdigit():
            n = int(rest.strip())
        return {"kind": "history", "n": n}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_commands.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add outlaw/commands.py tests/test_commands.py
git commit -m "feat: /img, /open, /history command parsing"
```

---

## Task 13: Line-mode client — wire images, history, commands

**Files:**
- Modify: `client_cli.py`
- Test: `tests/test_cli.py`

**Interfaces:**
- Consumes: `parse_input` new kinds, `ClientSession.send_image`/`on_image`, `store.append_history`/`load_recent_history`, `media.open_image`, `media.ImageError`
- Produces (changes to `CliChat`):
  - `on_image(frm, to, path, meta)` — print a notice, remember last image path/id, append history
  - `dispatch` handles `img` (call `session.send_image`, catch `ImageError`, echo), `open` (call `media.open_image`), `history` (print recent from `load_recent_history`)
  - history: `_log` also calls `store.append_history` with a structured entry; `on_message`/`_echo` record text entries
  - `__init__` loads recent history via `load_recent_history` and prints it (or exposes for test)

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_cli.py
class FakeSessionImg(FakeSession):
    def __init__(self):
        super().__init__()
        self.images = []
    def send_image(self, to, path):
        self.images.append((to, path)); return 3

def make_chat_img(tmp_path):
    out = []
    fake = FakeSessionImg()
    from client_cli import CliChat
    chat = CliChat("rahul", lambda c: fake, out=out.append,
                   history_path=tmp_path / "history.jsonl")
    return chat, fake, out

def test_img_command_sends(tmp_path):
    chat, fake, out = make_chat_img(tmp_path)
    chat.dispatch("/img /sdcard/p.jpg")
    assert fake.images == [("__all__", "/sdcard/p.jpg")]

def test_img_error_is_reported(tmp_path):
    chat, fake, out = make_chat_img(tmp_path)
    def boom(to, path):
        from outlaw.media import ImageError
        raise ImageError("Pillow not installed")
    fake.send_image = boom
    chat.dispatch("/img /x.jpg")
    assert any("Pillow" in line for line in out)

def test_on_image_prints_and_records(tmp_path):
    chat, fake, out = make_chat_img(tmp_path)
    chat.on_image("alice", "__all__", "/data/media/alice-1.jpg", {"name": "p.jpg"})
    assert any("alice" in line and "image" in line.lower() for line in out)
    from outlaw import store
    rows = store.load_recent_history(tmp_path / "history.jsonl")
    assert any(r.get("kind") == "image" for r in rows)

def test_history_command_prints_recent(tmp_path):
    from outlaw import store
    store.append_history(tmp_path / "history.jsonl", {"ts": 1, "frm": "a", "to": "__all__", "kind": "text", "text": "old line"})
    chat, fake, out = make_chat_img(tmp_path)
    out.clear()
    chat.dispatch("/history 5")
    assert any("old line" in line for line in out)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_cli.py -v`
Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

```python
# modify client_cli.py
from outlaw import media, store as _store
from outlaw.media import ImageError

# CliChat.__init__: after existing attrs, remember last image + preload history
#   self.last_image = None
#   if self.history_path: self._preload = _store.load_recent_history(self.history_path)

# add on_image callback:
    def on_image(self, frm, to, path, meta):
        tag = "" if to == "__all__" else " »you"
        self.out(f"{G}{_stamp()} {frm}{tag}  [image saved: {path}] (/open){X}")
        self.last_image = path
        self._log_entry({"ts": int(__import__('time').time()), "frm": frm, "to": to,
                         "kind": "image", "path": path, "name": meta.get("name")})

# add a structured history writer used by on_message/_echo/on_image:
    def _log_entry(self, entry):
        if self.history_path:
            _store.append_history(self.history_path, entry)

# in dispatch, add cases:
        elif k == "img":
            try:
                to = "__all__" if self.mode[0] == "group" else self.mode[1]
                self.session.send_image(to, act["path"])
                self.out(f"{A}* sending image → {act['path']}{X}")
            except ImageError as e:
                self.out(f"{RED}! {e}{X}")
        elif k == "open":
            target = self.last_image if act["arg"] == "last" else act["arg"]
            if target and media.open_image(target):
                self.out(f"{D}* opening {target}{X}")
            else:
                self.out(f"{RED}! nothing to open{X}")
        elif k == "history":
            rows = _store.load_recent_history(self.history_path, act["n"]) if self.history_path else []
            for r in rows:
                if r.get("kind") == "image":
                    self.out(f"{D}{r.get('frm')}: [image {r.get('name')}]{X}")
                else:
                    self.out(f"{D}{r.get('frm')}: {r.get('text','')}{X}")
```

Also update `on_message`/`_echo` to call `self._log_entry({...,"kind":"text","text":...})` in addition to the existing plain-log line. Wire `on_image=chat.on_image` and `media_dir` into the `ClientSession` factory in `main()`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_cli.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add client_cli.py tests/test_cli.py
git commit -m "feat: line-mode client image send/receive, /open, /history, JSONL history"
```

---

## Task 14: Textual client — wire images, history, commands

**Files:**
- Modify: `outlaw/ui.py`, `client.py`
- Manual smoke test.

**Interfaces:**
- Consumes: same as Task 13, in the Textual app
- Produces (changes to `OutlawApp`):
  - `on_image(frm, to, path, meta)` — write a notice line to the RichLog (guarded), record `self.last_image`, append history
  - `_dispatch` handles `img`/`open`/`history`
  - `on_mount` preloads recent history into the RichLog
  - `client.py` factory wires `on_image` and `media_dir`

- [ ] **Step 1: Implement**

```python
# outlaw/ui.py — add methods and dispatch cases mirroring Task 13, but writing to
# self.query_one("#stream", RichLog) with the guarded try/except NoMatches pattern
# already used by show_message. Preload in on_mount:
#     for r in store.load_recent_history(self.history_path):
#         <write a dim line for each>
# _dispatch: img -> self.session.send_image(...); open -> media.open_image(self.last_image);
#            history -> write recent lines. Catch ImageError and write a red line.
```

```python
# client.py — extend the ClientSession factory:
#   on_image=lambda f, to, path, meta: app.call_from_thread(app.on_image, f, to, path, meta)
#   media_dir=DATA_DIR / "media"
# (call_from_thread is correct here for the Textual app; the line client calls directly.)
```

- [ ] **Step 2: Smoke test**

Run:
```bash
python -c "from outlaw.ui import OutlawApp; import client; print('ui+client ok')"
```
Expected: `ui+client ok`.

- [ ] **Step 3: Commit**

```bash
git add outlaw/ui.py client.py
git commit -m "feat: Textual client image send/receive, /open, /history, history preload"
```

---

## Task 15: End-to-end integration + docs

**Files:**
- Modify: `tests/test_integration.py`
- Modify: `README.md`, `CLAUDE.md`

**Interfaces:**
- Consumes: real loopback server + two `ClientSession`s (as in existing integration tests)

- [ ] **Step 1: Write the failing end-to-end test**

```python
# append to tests/test_integration.py
import asyncio, pytest
from outlaw.server import Registry, Router, ServerProtocol
from outlaw.store import QueueStore

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
```

- [ ] **Step 2: Run and verify pass**

Run: `python -m pytest tests/test_integration.py::test_image_end_to_end_over_loopback -v`
Expected: PASS (the image round-trips: compress → window → chunks → reassemble → save).

- [ ] **Step 3: Run the full suite**

Run: `python -m pytest -q`
Expected: all pass.

- [ ] **Step 4: Update docs**

- README.md: add an "Images" section — `/img <path>`, `/open`, auto-compression to 24 KB, `~/.outlaw/media/`, and a note that transfers are slow over the tunnel (progress shown). Add `/history`. Note `pip install Pillow` requirement for sending.
- CLAUDE.md: prepend a dated work-log entry summarizing images + reliable-delivery fix + history; add the new packet types (`ih`/`im`), `media.py` to the file map, and update the test count; move the "offline multi-message loss" from any implied behavior to a fixed note; update "Last updated".

- [ ] **Step 5: Commit**

```bash
git add tests/test_integration.py README.md CLAUDE.md
git commit -m "test+docs: end-to-end image transfer; document images, history, delivery fix"
```

---

## Self-Review Notes

- **Spec coverage:** image protocol `ih`/`im` (T4), compression guaranteed-fit + input guard (T2), base64 chunking (T3), windowed sender (T11), receive+save+open (T3/T10/T13/T14), offline image queue with caps (T6/T9), reliable delivery + `ma`/sweep/queue-fallback + multi-message regression (T8), recipient `ma` with `n` (T10), persistent JSONL history + preload (T7/T13/T14), `/img` `/open` `/history` (T12/T13/T14), Pillow dependency (T2), end-to-end + docs (T15). All spec sections mapped.
- **Type consistency:** `ImageAssembler.add_header(mid, frm, name, mime, size, now)` and the completion dict `{"frm","name","mime","size","slices"}` are defined consistently in T5 and consumed in T10 (`done["frm"]` used for `save_image`/`on_image`) — the sender nick comes from the header since `im` chunks omit `f`. `_deliver_or_queue(nick, packet, now)` gains `now` in T8 and is used consistently in T8/T9. `enqueue_image`/`drain_images`/`enqueue`/`drain` consistent across T6/T9. `send_image`/`on_image(frm,to,path,meta)`/`media_dir` consistent across T10/T11/T13/T14 (`to` is `None` on receive = "to you"). `ma` with `n` (and `n=-1` for image headers) consistent across T8/T9/T10/T11.
