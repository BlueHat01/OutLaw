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
