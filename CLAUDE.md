# Outlaw — Project Context & Work Log

> **Purpose of this file:** a living hand-off record so any AI coding agent (or human) can pick up where the last session left off. It describes what Outlaw is, how the code is organized, what has been built, what is deliberately deferred, and the real-world deployment gotchas discovered while running it. **Keep it updated** at the end of any work session (see "Maintaining this file" at the bottom).

Last updated: 2026-09-21 · Branch: `master` · Tests: **60 passing**

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
| `outlaw/protocol.py` | Packet `encode`/`decode` (230B ceiling, `PacketTooLarge`), `new_id`, `now_ts`, `make_msg`, `chunk_text` (140B UTF-8-safe), `ChunkReassembler` (timeout purge), `DuplicateFilter` |
| `outlaw/store.py` | `Config` (local nick), `QueueStore` (per-nick offline queue, max 500, JSON persist/load), `append_line` (history/log) |
| `outlaw/server.py` | `Registry` (nick↔addr, eviction), `Router` (`reg`/`pi`/`lv`/`m`/`ma`/`err`; nick-collision reject; broadcast/DM; offline queue flush), `ServerProtocol` (asyncio, garbage-safe), `run_server` (heartbeat evict + queue persist loop) |
| `outlaw/client_net.py` | `ClientSession` — UI-agnostic network core. Register/retry, ping loop, per-chunk ack + retransmit (`_sweep`), link-down watchdog (`_check_link`, `LINK_TIMEOUT=90`), reassembly, dedup. Talks to any front-end via callbacks `on_message(f,to,x,ts)`, `on_roster(list)`, `on_link(bool)`, `on_error(msg)` |
| `outlaw/commands.py` | `parse_input(text) -> action dict` — pure slash-command parser |
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
ma    {"t":"ma","id","n"}                              per-chunk ack (server→sender carries n)
lv    {"t":"lv","f":"<nick>"}                          leave
```
Reliability: sender retransmits unacked chunks up to 3× (2s apart); recipient dedups by `id:n` and reassembles by `id`. Offline recipients: server queues (max 500/nick, persisted to `~/.outlaw/queue.json`) and flushes on reconnect.

## Runtime data (per device, `~/.outlaw/`)

- `config.json` — saved nick. `history.log` — local received/sent lines. `queue.json` — (server) offline queue. `debug.log` — (server) malformed-datagram errors. **Message contents are NOT logged on the server** (relay is blind by design).

## Test suite (60 passing)

`test_protocol` 13 · `test_store` 5 · `test_server` 11 · `test_commands` 9 · `test_integration` 11 (real-socket loopback: broadcast, DM, offline replay, per-chunk retransmit) · `test_cli` 11. UI (`ui.py`) has no automated test (needs a real terminal); it is smoke-imported.

## Deployment (how to run)

VPS: `sudo iodined -f -c -P <pw> -m 250 <TUN_IP> tunnel.example.com` then `python3 server.py`.
Phone (Termux): `iodine -f -P <pw> tunnel.example.com`, then `OUTLAW_SERVER=<TUN_IP> python3 client_cli.py`. Env: `OUTLAW_SERVER` (default `10.0.0.1`), `OUTLAW_PORT` (5005). See [README.md](README.md) for the full manual test checklist.

## Known limitations & real-world gotchas (READ before debugging)

1. **Textual TUI is unreliable on Termux.** Symptom: gibberish self-typing in the input and a UI stuck at `LINK DOWN` even when the network works — Textual's terminal-capability handshake isn't handled by many Termux terminals. **Fix/workaround: use `client_cli.py` (line mode).** The backend is unaffected.
2. **Choose the tunnel subnet carefully.** `10.0.0.1` (the original default) collides with common home-router LANs; on a colliding network the OS routes `10.0.0.1` to the local router, not `dns0`. Use an obscure range (e.g. `172.16.99.0/24`).
3. **IPv6-only cellular (464XLAT) breaks routing into `dns0`.** Android uses per-network policy routing; app traffic goes out the cellular table (`v4-ccmni0`, src `192.0.0.4`), not `dns0`, so packets never enter the tunnel (VPS sees nothing inbound) even though `ping` "works" (answered off-tunnel). Fix (needs root): `ip rule add to <TUN_IP> lookup main pref 100`, verify with `ip route get <TUN_IP>` showing `dev dns0`. A cleaner app-level fix (`SO_BINDTODEVICE dns0` via an `OUTLAW_IFACE` option) is proposed but not yet built.
4. **No server→recipient retransmit.** Server acks the sender and does a single best-effort deliver-or-queue; an online peer can miss a message under packet loss on the server→client hop (offline users are covered by the queue). Deferred.
5. **No local outgoing queue while link is down.** Messages composed during a sustained outage can drop after the 3-try retransmit window. Deferred.
6. Cosmetic deferrals: `show_message` uses local receive time (not packet `s`); queue persistence is non-atomic; `/nick` leaves the old nick in the registry until 90s eviction.

## Work log (most recent first)

- **2026-09-21** — Started design for **image sending + preserved chat history** (architectural; in brainstorming). Created this context file (`CLAUDE.md`).
- **2026-09-19..21** — Field debugging over real iodine tunnels. Root-caused two non-app issues: subnet collision (`10.0.0.0/8` vs home LAN) and IPv6-only cellular policy routing (464XLAT) preventing traffic entering `dns0`. Documented fixes in gotchas above.
- **2026-09-18** — Built line-mode client `client_cli.py` (no Textual) after the Textual TUI produced input gibberish + stuck `LINK DOWN` on Termux; verified end-to-end over loopback; 11 CLI tests. (commit `466e2ee`)
- **2026-09-18** — Implemented the full app via subagent-driven development from the plan: protocol, store, server, client session, command parser, Textual TUI, integration tests, README. Final review caught and fixed: multi-chunk messages sharing no id (never reassembled); Textual `call_from_thread`/pre-mount crash; then a per-chunk-ack regression. Merged `outlaw-impl` → `master` (merge `75d2925`).

## Maintaining this file

At the end of a work session, update: the "Last updated / Tests" line, the **Work log** (prepend a dated entry: what changed, why, key commit), any new **Known limitations**, and the **File map**/**Protocol** if modules or packet types changed. Keep entries terse and factual. This file is the first thing the next agent should read.
