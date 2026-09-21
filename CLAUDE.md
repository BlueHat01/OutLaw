# Outlaw — Project Context & Work Log

> **Purpose of this file:** a living hand-off record so any AI coding agent (or human) can pick up where the last session left off. It describes what Outlaw is, how the code is organized, what has been built, what is deliberately deferred, and the real-world deployment gotchas discovered while running it. **Keep it updated** at the end of any work session (see "Maintaining this file" at the bottom).

Last updated: 2026-09-21 · Branch: `master` · Tests: **101 passing**

---

## What Outlaw is

A peer-to-peer-style **CLI chat app that carries UDP over an iodine DNS tunnel**, runnable from Termux on phones. iodine runs *independently* and creates a `dns0` tun interface on each device and on a VPS; Outlaw just speaks ordinary UDP over that interface. Because iodine is strictly **hub-and-spoke**, clients cannot reach each other directly — all traffic flows through a relay server on the VPS. Features: user-chosen nicknames as IDs, a shared group channel plus private DMs, and offline message buffering with replay on reconnect.

Design authority: [docs/superpowers/specs/2026-09-18-outlaw-chat-design.md](docs/superpowers/specs/2026-09-18-outlaw-chat-design.md).
Implementation plan (as built): [docs/superpowers/plans/2026-09-18-outlaw-chat.md](docs/superpowers/plans/2026-09-18-outlaw-chat.md).

## Hard constraints (do not violate)

- **Datagrams ≤ 230 bytes.** iodine runs with `-m 250`; `protocol.encode()` raises `PacketTooLarge` above 230. Keep every packet under this.
- **Text chunked at 140 bytes** (UTF-8-safe), reassembled by message id on the receiver.
- **Compact JSON keys only:** `t` type, `f` from, `to` recipient (`__all__`=broadcast), `x` text, `s` unix ts, `id` msg id, `n` chunk index, `c` chunk count, `on` online list, `m` error text.
- **UDP port 5005.** Client keepalive ping every 30s; server evicts a client after 90s of silence; client link-down watchdog also fires at 90s.
- **Relay-through-VPS only** — never assume client-to-client reachability.

## File map

| File | Responsibility |
|------|----------------|
| `outlaw/protocol.py` | Packet `encode`/`decode` (230B ceiling, `PacketTooLarge`), `new_id`, `now_ts`, `make_msg`, `chunk_text` (140B UTF-8-safe), `ChunkReassembler` (timeout purge), `DuplicateFilter`, `make_img_header`/`make_img_chunk` (`ih`/`im`), `ImageAssembler` (image reassembly + purge) |
| `outlaw/media.py` | Image constants (`IMAGE_MAX_BYTES` etc.), `compress_image` (guaranteed-fit JPEG ≤24 KB), `chunk_b64`/`join_b64`, `save_image`/`open_image`, `ext_for_mime`/`safe_name` |
| `outlaw/store.py` | `Config` (local nick), `QueueStore` (per-nick offline text queue max 500 + capped offline image queue `enqueue_image`/`drain_images`, JSON persist/load), `append_line` (legacy log), `append_history`/`load_recent_history` (JSONL persistent history) |
| `outlaw/server.py` | `Registry` (nick↔addr, eviction), `Router` (`reg`/`pi`/`lv`/`m`/`ma`/`err`/`ih`/`im`; nick-collision reject; broadcast/DM; offline queue flush incl. images; delivery-pending table + `sweep_deliveries` retransmit→queue+evict), `ServerProtocol` (asyncio, garbage-safe), `run_server` (heartbeat evict + queue persist + delivery sweep loop) |
| `outlaw/client_net.py` | `ClientSession` — UI-agnostic network core. Register/retry, ping loop, per-chunk ack + retransmit (`_sweep`), link-down watchdog (`_check_link`, `LINK_TIMEOUT=90`), reassembly, dedup, `send_image`/`on_image`/`media_dir` (windowed image sender + receive→save) |
| `outlaw/commands.py` | `parse_input(text) -> action dict` — pure slash-command parser, incl. `/img`, `/open`, `/history` |
| `outlaw/ui.py` | `OutlawApp` — full-screen Textual TUI (hacker aesthetic). **Known issue on Termux, see below.** |
| `client.py` | Entrypoint for the Textual TUI client |
| `client_cli.py` | **Line-mode client** (`CliChat`, plain `print()`, stdlib-only). Recommended on Termux. Reuses the same `ClientSession`. |
| `server.py` | Relay entrypoint (`asyncio.run(run_server())`) |
| `tests/` | pytest suite (see below). Run: `python -m pytest -q` |

## Protocol packets

```
reg   {"t":"reg","f":"<nick>"}                        client→server register
ack   {"t":"ack","on":[<nicks>]}                      server→client (online list; sets link up)
err   {"t":"err","m":"nick taken"}                    server→client (collision etc.)
pi/po {"t":"pi","f":"<nick>"} / {"t":"po"}             keepalive (30s)
m     {"t":"m","id","f","to","x","s","n","c"}          chat message chunk
ma    {"t":"ma","id","n"}                              recipient/server delivery ack, carries chunk index `n` (`n=-1` for an image header ack); server also uses it to clear its delivery-pending table
ih    {"t":"ih","id","f","to","c","nm","mt","sz","s"}  image header (chunk count, filename, mime, size)
im    {"t":"im","id","n","c","d"}                      image chunk (base64 slice `d`); reassembled + acked like `m`
lv    {"t":"lv","f":"<nick>"}                          leave
```
Reliability: sender retransmits unacked chunks up to 3× (2s apart); recipient dedups by `id:n` and reassembles by `id`. Offline recipients: server queues (max 500/nick for text, capped 3 images/768KB for images, persisted to `~/.outlaw/queue.json`) and flushes on reconnect. Server also tracks a per-`(nick,id,n)` delivery-pending table for online recipients: retransmits up to 3× at ~2s on missing `ma`, then falls back to the offline queue and evicts the stale registry entry — this closes the old online-peer packet-loss gap (see Work log, 2026-09-21).

## Runtime data (per device, `~/.outlaw/`)

- `config.json` — saved nick. `history.log` — legacy local received/sent lines. `history.jsonl` — structured persistent history (preloaded on launch, last 200 entries). `queue.json` — (server) offline text+image queue. `media/` — saved received images. `debug.log` — (server) malformed-datagram errors. **Message contents are NOT logged on the server** (relay is blind by design).

## Test suite (101 passing)

`test_protocol` 19 · `test_media` 9 · `test_store` 10 · `test_history` 4 · `test_server` 15 · `test_commands` 13 · `test_integration` 16 (real-socket loopback: broadcast, DM, offline replay, per-chunk retransmit, end-to-end image transfer) · `test_cli` 15. UI (`ui.py`) has no automated test (needs a real terminal); it is smoke-imported.

## Deployment (how to run)

VPS: `sudo iodined -f -c -P <pw> -m 250 <TUN_IP> tunnel.example.com` then `python3 server.py`.
Phone (Termux): `iodine -f -P <pw> tunnel.example.com`, then `OUTLAW_SERVER=<TUN_IP> python3 client_cli.py`. Env: `OUTLAW_SERVER` (default `10.0.0.1`), `OUTLAW_PORT` (5005). See [README.md](README.md) for the full manual test checklist.

## Known limitations & real-world gotchas (READ before debugging)

1. **Textual TUI is unreliable on Termux.** Symptom: gibberish self-typing in the input and a UI stuck at `LINK DOWN` even when the network works — Textual's terminal-capability handshake isn't handled by many Termux terminals. **Fix/workaround: use `client_cli.py` (line mode).** The backend is unaffected.
2. **Choose the tunnel subnet carefully.** `10.0.0.1` (the original default) collides with common home-router LANs; on a colliding network the OS routes `10.0.0.1` to the local router, not `dns0`. Use an obscure range (e.g. `172.16.99.0/24`).
3. **IPv6-only cellular (464XLAT) breaks routing into `dns0`.** Android uses per-network policy routing; app traffic goes out the cellular table (`v4-ccmni0`, src `192.0.0.4`), not `dns0`, so packets never enter the tunnel (VPS sees nothing inbound) even though `ping` "works" (answered off-tunnel). Fix (needs root): `ip rule add to <TUN_IP> lookup main pref 100`, verify with `ip route get <TUN_IP>` showing `dev dns0`. A cleaner app-level fix (`SO_BINDTODEVICE dns0` via an `OUTLAW_IFACE` option) is proposed but not yet built.
4. **FIXED (2026-09-21): offline multi-message loss.** Previously, several messages sent to a client that had gone silent could collapse to only the last one being redelivered on reconnect. Root cause was the old "no server→recipient retransmit" gap combined with best-effort single-shot delivery. Now fixed via the server's delivery-pending table + `ma`-confirmed retransmit (3× at ~2s) + queue fallback + eviction (see Work log and Protocol section above) — regression-tested in `test_server.py::test_multi_message_offline_all_delivered_regression`.
5. **No local outgoing queue while link is down.** Messages composed during a sustained outage can drop after the 3-try retransmit window. Deferred.
6. Cosmetic deferrals: `show_message` uses local receive time (not packet `s`); queue persistence is non-atomic; `/nick` leaves the old nick in the registry until 90s eviction.

## Work log (most recent first)

- **2026-09-21** — Implemented **image sending, reliable delivery fix, and persistent history** end-to-end (subagent-driven from `docs/superpowers/plans/2026-09-21-outlaw-images-history.md`), 101 tests passing:
  - **Images:** new `outlaw/media.py` compresses with Pillow to a guaranteed ≤24 KB JPEG (downscale+requality loop), base64-chunks it, and streams it through new `ih`/`im` packets over a sliding window (`IMAGE_WINDOW=6` chunks in flight, ack-driven advance, retransmit on stall). Receiver reassembles via `ImageAssembler`, decodes, and saves to `~/.outlaw/media/`; `/img <path>` sends, `/open [id|last]` views (termux-open/xdg-open). Offline recipients get images grouped whole and queued (capped 3/768KB per user) for replay on reconnect.
  - **Reliable delivery fix:** server now tracks a per-`(nick,id,n)` delivery-pending table for *online* recipients and requires the recipient's `ma` (now carrying `n`) to confirm receipt; unconfirmed deliveries retransmit up to 3× (~2s apart) then fall back to the offline queue + evict the stale registry entry. This fixes the reported bug where several messages sent to a client that went silent only redelivered the last one on reconnect — all messages now redeliver (regression test: `test_multi_message_offline_all_delivered_regression`).
  - **History:** local chat history is now also persisted as structured JSONL (`~/.outlaw/history.jsonl`, one JSON object/line for text and image entries) via `append_history`/`load_recent_history`, preloaded (last 200) on launch; `/history [n]` replays it. Both `client.py` and `client_cli.py` wire images, `/open`, `/history`, and history preload identically through the shared `ClientSession`.
- **2026-09-19..21** — Field debugging over real iodine tunnels. Root-caused two non-app issues: subnet collision (`10.0.0.0/8` vs home LAN) and IPv6-only cellular policy routing (464XLAT) preventing traffic entering `dns0`. Documented fixes in gotchas above.
- **2026-09-18** — Built line-mode client `client_cli.py` (no Textual) after the Textual TUI produced input gibberish + stuck `LINK DOWN` on Termux; verified end-to-end over loopback; 11 CLI tests. (commit `466e2ee`)
- **2026-09-18** — Implemented the full app via subagent-driven development from the plan: protocol, store, server, client session, command parser, Textual TUI, integration tests, README. Final review caught and fixed: multi-chunk messages sharing no id (never reassembled); Textual `call_from_thread`/pre-mount crash; then a per-chunk-ack regression. Merged `outlaw-impl` → `master` (merge `75d2925`).

## Maintaining this file

At the end of a work session, update: the "Last updated / Tests" line, the **Work log** (prepend a dated entry: what changed, why, key commit), any new **Known limitations**, and the **File map**/**Protocol** if modules or packet types changed. Keep entries terse and factual. This file is the first thing the next agent should read.
