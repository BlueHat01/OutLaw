import json
from collections import defaultdict, deque
from pathlib import Path

IMAGE_QUEUE_MAX = 3
IMAGE_QUEUE_MAX_BYTES = 786432
HISTORY_LOAD = 200

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
        self._img = defaultdict(list)

    def enqueue(self, nick, packet):
        self._q[nick].append(packet)

    def drain(self, nick):
        msgs = list(self._q.get(nick, []))
        if nick in self._q:
            self._q[nick].clear()
        return msgs

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

def append_line(path, line):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

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
