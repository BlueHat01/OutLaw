# Outlaw — UDP Chat over iodine DNS Tunnel

**Date:** 2026-09-18
**Status:** Approved design, ready for implementation planning

## Summary

Outlaw is a Python CLI chat application that runs on Termux (Android) and a
VPS. Devices communicate over UDP carried inside an **iodine DNS tunnel**.
iodine independently creates a `dns0` tun interface on every mobile device and
on the VPS; Outlaw simply uses ordinary UDP sockets over that interface. The
VPS acts as a **relay hub** because iodine is strictly hub-and-spoke — clients
cannot reach each other directly, only via the server at `10.0.0.1`.

The app supports a shared broadcast room plus private 1-on-1 DMs, user-chosen
nicknames as IDs, offline message buffering with replay on reconnect, and a
full-screen "hacker-vibe" TUI built with Textual.

## Transport Constraints (iodine)

- **Hub-and-spoke:** all client traffic flows through the VPS (`10.0.0.1`).
  Clients get sequential IPs (`10.0.0.2`, `.3`, ...). This is why the relay
  model is the only viable design, not merely a preference.
- **Fragment ceiling `-m 250`:** the iodine server is started with `-m 250`.
  Every UDP datagram must target **≤ 230 bytes** so it is not IP-fragmented
  across multiple DNS round-trips (a single lost fragment loses the datagram).
- **60s session timeout:** iodine drops idle tunnels. Outlaw sends a `ping`
  every 30s to keep both the tunnel and app session alive.
- **Lossy + higher latency:** RTT ranges roughly 1–30ms per fragment but real
  throughput is low (tens of kbps to low Mbps). Design for small packets and
  application-level reliability.

## Architecture

Two programs, both plain `.py` files runnable directly:

- `server.py` — UDP relay hub, runs on the VPS at `10.0.0.1:5005`.
- `client.py` — the Outlaw TUI, runs on each Termux device.

Both use **asyncio**. The client integrates an asyncio `DatagramProtocol`
with the Textual event loop, so no manual threads/locks are needed.

### Server (`server.py`)

Responsibilities:

- **Registry:** `{nick -> (ip, port, last_seen)}` for connected clients.
- **Routing:** inspect each datagram's `to` field; deliver to the target
  client if online, else enqueue.
- **Offline queue:** `{nick -> deque(maxlen=500)}`; flushed (one datagram per
  message) when the user reconnects, then cleared.
- **Heartbeat/eviction:** clients ping every 30s; evict clients unseen for 90s
  and broadcast their departure.
- **Persistence:** offline queue written to `~/.outlaw/queue.json`, fsync'd
  periodically, so a VPS restart does not lose buffered messages.
- **ACK:** on receiving a `msg`, the server returns `ma` (msg-ack) to the
  sender, then attempts delivery to the recipient and awaits the recipient's
  `ma`; after 3 failed retransmits (2s apart) it moves the message to the
  offline queue.

### Client (`client.py` — Outlaw TUI)

- **Framework:** Textual (pip-installable; runs in Termux).
- **Aesthetic:** matrix-green `#00ff41` + amber `#ffb000` on black, box-drawing
  borders, ASCII "OUTLAW" banner, a fake boot/handshake sequence on connect.
- **Layout:** header (nick@ip, link status) · left peer rail (online/offline) ·
  center message stream (color-coded) · input bar (shows active mode) · status
  bar (command hints, msg/tx/rx counters, clock, link indicator).
- **Local log:** every displayed message appended to `~/.outlaw/history.log`.

## Identity

- User-chosen nickname on first run, stored locally in `~/.outlaw/config.json`.
- First-come-first-served on the server; collisions rejected with an `err`
  packet, client re-prompts.

## Chat Modes

- **Group (default):** `to: "__all__"` broadcast to every connected client.
- **DM:** `to: "<nick>"` private to one peer.
- Input bar shows the active target: `[group]` or `[dm:alice]`.

## Packet Protocol

Newline-free single-datagram JSON with short keys. Target ≤ 230 bytes.

| Type | Example | Direction |
|------|---------|-----------|
| register | `{"t":"reg","f":"rahul"}` | client → server |
| ack | `{"t":"ack","on":["phone2","alice"]}` | server → client |
| error | `{"t":"err","m":"nick taken"}` | server → client |
| ping | `{"t":"pi","f":"rahul"}` (every 30s) | client → server |
| pong | `{"t":"po"}` | server → client |
| msg/dm | `{"t":"m","id":"a3f","f":"rahul","to":"__all__","x":"hi","s":1699999999,"n":0,"c":1}` | both |
| msg-ack | `{"t":"ma","id":"a3f"}` | both |
| leave | `{"t":"lv","f":"rahul"}` | client → server |

Keys: `t`=type, `f`=from, `to`=recipient (`__all__`=broadcast), `x`=text,
`s`=unix timestamp, `id`=message id, `n`=chunk index, `c`=chunk count,
`on`=online list, `m`=error message.

### Size discipline & chunking

- Envelope + two 16-char nicks ≈ 85 bytes, leaving ~145 for text.
- **Per-packet text cap: 140 bytes.**
- Text over 140 bytes is split into numbered chunks (`n`/`c`) and reassembled
  by `id` on the recipient before display. Incomplete sets are dropped after
  30s.

### Reliability

- Each `msg` carries a unique `id`.
- Sender retransmits up to 3× (2s apart) until it receives an `ma`.
- Server ACKs sender on receipt; server delivers to recipient and awaits their
  `ma`, retransmitting up to 3× before falling back to the offline queue.
- Recipients keep a bounded set of recently-seen `id`s to suppress duplicates.

## Commands

- `/who` — list online users
- `/dm <nick>` — switch input to private mode
- `/group` — switch back to broadcast
- `/msg <nick> <text>` — one-off DM without switching mode
- `/nick <name>` — change nickname (re-register)
- `/clear` — clear message pane
- `/help` — command overlay
- `/quit` — send `leave`, exit cleanly
- Plain text — sent to the active mode target

## Error Handling & Resilience

- **Tunnel drop:** no `pong`/`ack` for 90s → client shows `● LINK DOWN` (red),
  retries `register` every 10s; outgoing messages queue locally and flush on
  reconnect.
- **Server restart:** persisted queue reloaded; clients re-register via retry.
- **Malformed/oversized datagrams:** parsing wrapped in try/except on both
  ends; bad datagrams dropped silently and logged to a debug file; UI never
  crashes.
- **Nick collision:** `err` → client re-prompts for a new alias.
- **Chunk timeout:** incomplete multi-chunk messages discarded after 30s.
- **Duplicate suppression:** bounded recent-`id` set prevents double-display of
  retransmits.

## Testing Strategy

- **Protocol unit tests (pytest):** encode/decode round-trips, 140-byte cap,
  chunk split + reassemble, duplicate suppression, envelope ≤ 230 bytes.
- **Server unit tests:** registry add/evict, online vs offline routing, queue
  flush on reconnect, nick-collision rejection — via a mock UDP transport.
- **Integration tests:** real server on `127.0.0.1` (loopback stands in for
  `dns0`), 2–3 asyncio clients; assert broadcast, DM, offline→reconnect
  delivery, ACK/retransmit under simulated packet loss.
- **Manual Termux checklist:** documented end-to-end steps against a real
  iodine tunnel.

## Deliverables

- `server.py`, `client.py`
- `outlaw/` package: `protocol.py`, `net.py`, `ui.py`, `store.py`
- `requirements.txt` (textual, pytest)
- `tests/` (protocol, server, integration)
- `README.md` — iodine setup (server `-m 250`, client), install & run steps,
  manual test checklist

## Out of Scope (v1)

- Encryption (the DNS tunnel is the trust boundary for v1; note as a future
  enhancement).
- File transfer / media.
- Group rooms beyond the single shared channel.
- Message editing/deletion.
