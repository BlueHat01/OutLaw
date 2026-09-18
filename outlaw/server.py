import asyncio
import os
from pathlib import Path
from outlaw import protocol as proto
from outlaw.store import QueueStore, append_line


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
