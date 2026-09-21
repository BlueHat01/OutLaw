# outlaw/client_net.py
import asyncio
from pathlib import Path
from outlaw import protocol as proto
from outlaw import media

class ClientSession:
    LINK_TIMEOUT = 90            # seconds without inbound => link considered down

    def __init__(self, nick, server_addr, on_message, on_roster, on_link, on_error=None,
                 on_image=None, media_dir=None):
        self.nick = (nick or "")[:proto.MAX_NICK]
        self.server_addr = server_addr
        self.on_message = on_message
        self.on_roster = on_roster
        self.on_link = on_link
        self.on_error = on_error
        self.on_image = on_image
        self.media_dir = media_dir or Path.home() / ".outlaw" / "media"
        self.transport = None
        self._pending = {}       # mid -> {"packets": {n: pkt}, "tries": int, "at": ts, "acked": set(n)}
        self._dupes = proto.DuplicateFilter()
        self._reasm = proto.ChunkReassembler()
        self._img = proto.ImageAssembler()
        self._img_tx = {}        # mid -> {"to","header","chunks":{n:pkt},"acked":set(),"sent":set(),"at":ts}
        self._link_up = False
        self._closing = False
        self._last_rx = proto.now_ts()

    # --- transport seam (overridden in tests) ---
    def _raw_send(self, packet):
        try:
            self.transport.sendto(proto.encode(packet), self.server_addr)
        except proto.PacketTooLarge:
            pass

    # --- inbound ---
    def feed(self, packet):
        t = packet.get("t")
        self._last_rx = proto.now_ts()
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
                if n == -1:
                    tx["header_acked"] = True
                elif n is not None and n >= 0:
                    tx["acked"].add(n)
                tx["at"] = proto.now_ts()
                if len(tx["acked"]) >= len(tx["chunks"]):
                    del self._img_tx[mid]
                else:
                    self._img_advance(mid)
        elif t == "err":
            if self.on_error is not None:
                self.on_error(packet.get("m", ""))
        elif t == "m":
            mid = packet.get("id")
            self._raw_send({"t": "ma", "id": mid, "n": packet.get("n")})
            if self._dupes.seen(mid + ":" + str(packet.get("n"))):
                return
            full = self._reasm.add(mid, packet.get("n", 0), packet.get("c", 1),
                                   packet.get("x", ""), proto.now_ts())
            if full is not None:
                self.on_message(packet.get("f"), packet.get("to"), full, packet.get("s"))
        elif t == "ih":
            self._raw_send({"t": "ma", "id": packet.get("id"), "n": -1})
            self._img.add_header(packet.get("id"), packet.get("f"), packet.get("to"),
                                 packet.get("nm"), packet.get("mt"), packet.get("sz"),
                                 proto.now_ts())
        elif t == "im":
            mid = packet.get("id")
            self._raw_send({"t": "ma", "id": mid, "n": packet.get("n")})
            done = self._img.add_chunk(mid, packet.get("n"), packet.get("c"),
                                       packet.get("d", ""), proto.now_ts())
            if done is not None and self.on_image is not None:
                data = media.join_b64(done["slices"])
                path = media.save_image(self.media_dir, done["frm"], mid, done["mime"], data)
                self.on_image(done["frm"], done.get("to"), path, done)

    # --- outbound ---
    def send_text(self, to, text):
        chunks = proto.chunk_text(text)
        total = len(chunks)
        mid = proto.new_id()
        packets = []
        for n, chunk in enumerate(chunks):
            pkt = proto.make_msg(self.nick, to, chunk, mid, n, total, proto.now_ts())
            packets.append(pkt)
        self._pending[mid] = {
            "packets": {n: pkt for n, pkt in enumerate(packets)},
            "tries": 0,
            "at": proto.now_ts(),
            "acked": set(),
        }
        for pkt in packets:
            self._raw_send(pkt)

    def register(self):
        self._raw_send({"t": "reg", "f": self.nick})

    def set_nick(self, name):
        self.nick = (name or "")[:proto.MAX_NICK]
        self.register()

    def leave(self):
        self._raw_send({"t": "lv", "f": self.nick})

    async def _ping_loop(self):
        while not self._closing:
            await asyncio.sleep(30)
            self._raw_send({"t": "pi", "f": self.nick})

    def send_image(self, to, path):
        data, mime = media.compress_image(path)           # raises ImageError
        import os
        slices = media.chunk_b64(data, 150)
        mid = proto.new_id()
        name = media.safe_name(os.path.basename(path))
        header = proto.make_img_header(self.nick, to, mid, len(slices), name, mime, len(data), proto.now_ts())
        chunks = {n: proto.make_img_chunk(mid, n, len(slices), sl) for n, sl in enumerate(slices)}
        self._img_tx[mid] = {"to": to, "header": header, "chunks": chunks,
                             "acked": set(), "sent": set(), "at": proto.now_ts(),
                             "header_acked": False}
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

    def _sweep(self, now):
        self._reasm.purge(now)
        self._img.purge(now)
        for mid, e in list(self._pending.items()):
            if now - e["at"] >= 2:
                if e["tries"] >= 3:
                    del self._pending[mid]
                else:
                    e["tries"] += 1
                    e["at"] = now
                    for n, pkt in e["packets"].items():
                        if n not in e["acked"]:
                            self._raw_send(pkt)
        for mid, tx in list(self._img_tx.items()):
            if now - tx["at"] >= 2:
                tx["tries"] = tx.get("tries", 0) + 1
                if tx["tries"] > 5:
                    del self._img_tx[mid]
                else:
                    tx["at"] = now
                    if not tx.get("header_acked"):
                        self._raw_send(tx["header"])
                    for n in sorted(tx["sent"]):
                        if n not in tx["acked"]:
                            self._raw_send(tx["chunks"][n])

    async def _retransmit_loop(self):
        while not self._closing:
            await asyncio.sleep(2)
            self._sweep(proto.now_ts())

    def _check_link(self, now):
        # Watchdog: if no inbound traffic for LINK_TIMEOUT while we think the
        # link is up, mark it down so _register_retry_loop resumes re-registering.
        if self._link_up and now - self._last_rx > self.LINK_TIMEOUT:
            self._link_up = False
            self.on_link(False)

    async def _watchdog_loop(self):
        while not self._closing:
            await asyncio.sleep(3)
            self._check_link(proto.now_ts())

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
        asyncio.create_task(self._watchdog_loop())

    def close(self):
        self._closing = True
        if self.transport:
            self.transport.close()
