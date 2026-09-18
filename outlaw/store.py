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
