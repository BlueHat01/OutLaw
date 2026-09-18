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
