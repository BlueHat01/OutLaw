import asyncio
import os
from pathlib import Path
from outlaw import protocol as proto
from outlaw.store import QueueStore, append_line

DELIVERY_RETRIES = 3
DELIVERY_INTERVAL = 2


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


class Router:
    def __init__(self, registry, queue, send):
        self.reg = registry
        self.q = queue
        self.send = send
        self.pending = {}
        self._img_build = {}
        self._img_route = {}  # transfer id -> {"f": sender, "to": recipient}; "im" chunks carry no f/to

    def _broadcast_online(self):
        pkt = {"t": "ack", "on": self.reg.online()}
        for nick in self.reg.online():
            self.send(self.reg.addr_of(nick), pkt)

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
            self._img_route.pop(packet.get("id"), None)

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

    def handle(self, packet, addr, now):
        t = packet.get("t")
        if t == "reg":
            nick = packet.get("f")
            if not nick:
                return
            owner = self.reg.addr_of(nick)
            if owner is not None and owner != addr:
                # Nick already owned by a different, still-live client.
                self.send(addr, {"t": "err", "m": "nick taken"})
                return
            self.reg.register(nick, addr, now)
            self.send(addr, {"t": "ack", "on": self.reg.online()})
            self._broadcast_online()
            for m in self.q.drain(nick):
                self.send(addr, m)
            for transfer in self.q.drain_images(nick):
                self.send(addr, transfer["header"])
                for ch in transfer["chunks"]:
                    self.send(addr, ch)
        elif t == "pi":
            self.reg.touch(packet.get("f"), now)
            self.send(addr, {"t": "po"})
        elif t == "lv":
            self.reg.remove(packet.get("f"))
            self._broadcast_online()
        elif t == "ih":
            frm = packet.get("f")
            to = packet.get("to")
            self._img_route[packet.get("id")] = {"f": frm, "to": to}
            self.reg.touch(frm, now)
            self.send(addr, {"t": "ma", "id": packet.get("id"), "n": -1})  # header ack (n=-1)
            if to == proto.BROADCAST:
                for nick in self.reg.online():
                    if nick != frm:
                        self._deliver_or_queue(nick, packet, now)
            elif to:
                self._deliver_or_queue(to, packet, now)
        elif t in ("m", "im"):
            frm = packet.get("f")
            to = packet.get("to")
            if t == "im" and (frm is None or to is None):
                # "im" data chunks carry no f/to (kept small); recover routing
                # from the transfer's "ih" header, seen earlier for this id.
                route = self._img_route.get(packet.get("id"), {})
                frm = frm if frm is not None else route.get("f")
                to = to if to is not None else route.get("to")
            self.reg.touch(frm, now)
            self.send(addr, {"t": "ma", "id": packet.get("id"), "n": packet.get("n")})
            if to == proto.BROADCAST:
                for nick in self.reg.online():
                    if nick != frm:
                        self._deliver_or_queue(nick, packet, now)
            elif to:
                self._deliver_or_queue(to, packet, now)
        elif t == "ma":
            nick = self.reg.nick_of(addr)
            if nick is not None:
                self.pending.pop((nick, packet.get("id"), packet.get("n")), None)


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
            await asyncio.sleep(2)
            router.sweep_deliveries(proto.now_ts())
            for nick in registry.evict_stale(proto.now_ts(), timeout=90):
                router._broadcast_online()
            queue.persist()

    try:
        await housekeeping()
    finally:
        transport.close()
        queue.persist()
