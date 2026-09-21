import json
import time
import uuid
from collections import deque

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

def make_img_header(frm, to, mid, count, name, mime, size, ts) -> dict:
    return {"t": "ih", "id": mid, "f": frm, "to": to, "c": count,
            "nm": name, "mt": mime, "sz": size, "s": ts}

def make_img_chunk(mid, n, count, data_slice) -> dict:
    return {"t": "im", "id": mid, "n": n, "c": count, "d": data_slice}

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
