# Outlaw — Image Sending, Reliable Delivery & Persistent History

**Date:** 2026-09-21
**Status:** Approved design, ready for implementation planning
**Builds on:** docs/superpowers/specs/2026-09-18-outlaw-chat-design.md (base chat app)

## Summary

Three additions to Outlaw (UDP chat over an iodine DNS tunnel, VPS relay hub):

1. **Image sending** — send a compressed image to the group or a DM. The
   sender downscales/re-encodes to a tight byte budget, then streams it over
   the existing reliable chunk pipeline with windowed pacing. Receivers save
   it to disk and open it on demand.
2. **Reliable delivery + offline fix** — fix an observed bug where, when
   multiple messages are sent to a recipient who dropped off ungracefully,
   only the most recent survives. Add server→recipient delivery confirmation
   with fallback to the offline queue.
3. **Persistent local history** — a structured per-device transcript that
   survives restarts and loads recent history on launch.

The VPS remains a relay: it buffers only undelivered/offline content
transiently (now including a strictly-capped image queue), and never keeps a
permanent transcript. Full history lives locally on each device.

## Transport constraints (unchanged, still binding)

- Datagrams ≤ 230 bytes (`protocol.encode` raises `PacketTooLarge`); iodine `-m 250`.
- Text chunked at 140 bytes; images chunked so each `im` datagram stays ≤ 230 bytes.
- Compact JSON keys only. UDP port 5005. Ping 30s; eviction/watchdog 90s.
- Relay-through-VPS only; no client-to-client reachability.

---

## Part 1 — Image sending

### 1.1 Protocol additions

Two new packet types, both ≤ 230 bytes, both flowing through the existing
reliability (ack/retransmit), dedup, and offline-queue machinery:

```
ih  {"t":"ih","id":"3f9","f":"alice","to":"bob","c":210,"nm":"pic.jpg","mt":"image/jpeg","sz":24576,"s":1699999999}
im  {"t":"im","id":"3f9","n":42,"c":210,"d":"<base64 slice>"}
```

- `ih` (header): transfer `id`, `f` from, `to` recipient (`__all__` for group),
  `c` total chunk count, `nm` sanitized+truncated filename, `mt` mime,
  `sz` decoded byte size, `s` timestamp.
- `im` (data): `id`, `n` chunk index, `c` total count (repeated so reassembly
  never depends on header arrival order), `d` base64 slice. Envelope ≈ 30 bytes,
  leaving ~200 bytes for base64 ≈ 150 raw bytes/chunk.
- Acks reuse `ma` with `n`. Image chunks are acked/retransmitted like text.

### 1.2 Compression (sender)

- Uses **Pillow**. Downscale so the longest edge ≤ 800px; re-encode JPEG,
  lowering quality until the encoded size ≤ **24 KB** (floor quality guard).
- Constants: `IMAGE_MAX_EDGE = 800`, `IMAGE_MAX_BYTES = 24576`.
- If Pillow is not installed, `/img` fails with a clear message and sends
  nothing (never sends a multi-MB original).

### 1.3 Chunking & windowed transfer

- Compressed bytes → base64 → split so each `im` datagram is ≤ 230 bytes.
- Sender emits the `ih` header, then streams `im` chunks through a **sliding
  window** (`IMAGE_WINDOW = 6` chunks in flight); each ack releases the next.
  This prevents flooding the DNS tunnel (which would cause mass loss).
- Per-chunk retransmit reuses the existing `_sweep`/`_pending` mechanism.
- Sender shows progress: `sending pic.jpg → <acked>/<c>`.

### 1.4 Receive & reassembly

- Receiver buffers `im` slices by `id`; completion when all `c` chunks present.
- Progress: `receiving pic.jpg from alice → <have>/<c>`.
- On completion: base64-decode, write to `~/.outlaw/media/<from>-<id>.<ext>`
  (extension from mime), print a notice with the path and an `/open` handle,
  and append an image entry to history.
- Incomplete transfers purged after `IMAGE_TRANSFER_TIMEOUT = 120` s.

### 1.5 Commands & callbacks

- `/img <path>` — send image to the current target (group or active DM).
- `/open [id|last]` — open a saved image via `termux-open`, falling back to
  `xdg-open`; defaults to the most recently received/sent image.
- `ClientSession` gains `send_image(to, path)` and an
  `on_image(frm, to, path, meta)` callback. Both front-ends wire it.
- New module `outlaw/media.py`: compression, base64 chunking, save/open,
  filename sanitization, mime↔extension.

---

## Part 2 — Reliable delivery + offline fix

### 2.1 Root cause (observed bug)

`Router._deliver_or_queue` treats a recipient as online if it is still in the
registry. A client that drops ungracefully (tunnel dies / app killed, no `lv`)
stays "online" for up to 90 s until heartbeat eviction. Messages sent in that
window are `send()`-ed to a dead address and lost — never queued. Only messages
sent after eviction are queued, so on reconnect the recipient receives only the
later ("most recent") ones. Recipient receipt-acks reach the server but are
currently ignored, so the server never learns delivery failed.

### 2.2 Fix — server→recipient confirmation with queue fallback

- Recipient receipt-ack includes the chunk index: `{"t":"ma","id","n"}`
  (currently omits `n`).
- On delivering a chunk to an online recipient, the server records a pending
  delivery keyed by `(recipient_nick, id, n)` with `{packet, tries, at}`.
- An **incoming** `ma` at the server (only recipients send `ma`; senders receive
  it) clears the matching pending delivery, identified via `nick_of(addr)`.
- A server sweep every ~2 s retransmits pending deliveries older than 2 s, up to
  3 tries; if still unacked, it **moves the chunk to the recipient's offline
  queue and removes the recipient from the registry**.
- Applies to text and image chunks, and to broadcast (tracked per recipient).

This guarantees every chunk is eventually delivered or queued, closing the 90 s
hole.

### 2.3 Client changes

- Recipient path (`feed` on `m`/`im`) sends `ma` with `n`.
- Incoming `ma` at the client still means "server confirmed my sent chunk"
  (clears the client's `_pending`) — unchanged semantics, unaffected by 2.2.

---

## Part 3 — Offline image queue (strict cap)

- `QueueStore` keeps two per-user queues, both persisted to `~/.outlaw/queue.json`:
  - **text queue** — unchanged (count-bounded, max 500).
  - **image queue** — image transfers grouped as **one logical entry per
    transfer** (header + chunks). Bounded strictly: at most `IMAGE_QUEUE_MAX = 3`
    most-recent images per user *and* a total byte budget
    `IMAGE_QUEUE_MAX_BYTES = 786432` (~768 KB); oldest evicted first.
  - Images never evict text; text never evicts images.
- The server groups offline `ih`/`im` chunks by transfer `id` into image-queue
  entries.
- On reconnect: text flushes first, then queued images re-stream through the
  windowed sender.

---

## Part 4 — Persistent local history

- `~/.outlaw/history.jsonl` — one JSON object per line, the source of truth for
  scrollback: `{ts, frm, to, kind:"text"|"image", text?, path?, name?}`.
  The existing plain `history.log` is retained for grep-ability.
- Image entries store the saved media **path + metadata**, never the bytes.
- On client launch, the last `HISTORY_LOAD = 200` entries load and render in the
  scrollback immediately (survives restarts).
- Every sent/received message and image is appended as it happens.
- `/history [n]` (line-mode) prints the last `n` entries; Textual uses scrollback.

---

## New / changed modules

- `outlaw/media.py` (new) — `compress_image(path) -> bytes`,
  `chunk_b64(data) -> list[str]`, `save_image(from, id, mime, data) -> path`,
  `open_image(path)`, filename/mime helpers, image constants.
- `outlaw/protocol.py` — `ih`/`im` builders; generalize reassembly to collect
  image chunks; image transfer timeout.
- `outlaw/client_net.py` — `send_image`, `on_image` callback, windowed image
  sender, recipient `ma` includes `n`, image reassembly → save.
- `outlaw/server.py` — server-side delivery pending table + sweep + `ma` handling
  + eviction-on-failure; offline image grouping.
- `outlaw/store.py` — two-queue `QueueStore` (text + capped image);
  JSONL history read/write + recent-load.
- `outlaw/commands.py` — `img`, `open`, `history` actions.
- `outlaw/ui.py`, `client.py`, `client_cli.py` — wire `/img`, `/open`,
  `/history`, `on_image`, progress lines, history preload.
- `requirements.txt` — add `Pillow`.

## Testing strategy

- **protocol:** `ih`/`im` encode/decode ≤ 230; base64 chunk round-trip; image
  reassembly → original bytes; transfer purge timeout.
- **media:** compression hits ≤ 24 KB / ≤ 800px on a generated image; Pillow-absent
  path fails cleanly (guarded/skipped).
- **sender windowing:** ≤ window chunks in flight; acks release more.
- **reliable delivery:** recipient acks → cleared; no ack → requeued + evicted
  after retries; **regression test reproducing the multi-message-offline loss**
  (all delivered on reconnect).
- **offline image cap:** exceeding `IMAGE_QUEUE_MAX` / byte budget evicts oldest;
  text unaffected.
- **history:** JSONL persist/load; recent-entry load on launch; image entries
  store path.
- **commands/integration:** `/img`, `/open`, `/history` dispatch (pure, fake
  session); loopback end-to-end image send between two clients, including
  offline-then-reconnect image delivery.

## Out of scope (this iteration)

- Video/audio/arbitrary file transfer (image-only).
- Large images / resumable multi-MB transfers.
- Server-side permanent history or cross-device history sync.
- Encryption (tunnel remains the trust boundary).
- Inline in-terminal image rendering (sixel/kitty).
