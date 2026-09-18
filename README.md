# Outlaw

**Outlaw** is a Python UDP chat application that relays messages over an iodine DNS tunnel, featuring a VPS relay hub, user-chosen nicks, group chat, direct messaging, and offline message buffering. All communication is carried through a central server running on the VPS at `10.0.0.1:5005`.

## Architecture

Outlaw uses a **hub-and-spoke** topology via the VPS. All traffic flows through the server (`10.0.0.1`) because iodine DNS tunnel clients cannot reach each other directly—the server handles message routing for both group broadcasts and private messages, and buffers messages for offline users.

## Setup

### Server-side iodine (VPS)

Start the DNS tunnel with:
```bash
iodined -f -c -P <password> -m 250 10.0.0.1 tunnel.example.com
```

The `-m 250` flag sets the maximum fragment size; Outlaw is designed for datagrams ≤230 bytes.

### Client-side iodine (Termux)

Install iodine and connect to the tunnel:
```bash
pkg install iodine
iodine -f -P <password> tunnel.example.com
```

After connecting, confirm the tunnel interface exists and your device has a `10.0.0.x` address:
```bash
ip addr  # you should see a dns0 interface with 10.0.0.x
```

### Run the relay (VPS)

```bash
python3 server.py
```

### Run Outlaw (Termux)

Install dependencies and start the client:
```bash
pip install -r requirements.txt
OUTLAW_SERVER=10.0.0.1 python3 client.py
```

When prompted, enter your alias (nick).

#### Line-mode client (recommended on Termux)

The full-screen Textual UI (`client.py`) depends on the terminal correctly
handling Textual's capability handshake. Some Termux terminals don't, which
shows up as stray/gibberish characters in the input and a UI stuck at
`LINK DOWN` even when the network is fine. If you hit that, use the line-mode
client instead — same server, same protocol, same commands, plain scrolling
output, no Textual:

```bash
OUTLAW_SERVER=10.0.0.1 python3 client_cli.py
```

It needs no extra dependencies beyond the Python standard library.

## Environment Variables

- `OUTLAW_SERVER` — server address (default: `10.0.0.1`)
- `OUTLAW_PORT` — server port (default: `5005`)

## Commands

| Command | Description |
|---------|-------------|
| `/who` | Show online users in the peer panel |
| `/dm <nick>` | Switch to direct message mode with a user |
| `/group` | Switch to group chat mode (broadcast to all) |
| `/msg <nick> <text>` | Send a one-off direct message |
| `/nick <name>` | Change your alias |
| `/clear` | Clear the message stream |
| `/help` | Show command help |
| `/quit` | Leave and exit |

## Manual Test Checklist

1. **Start the server:** Run `iodined -f -c -P <password> -m 250 10.0.0.1 tunnel.example.com` on the VPS and `python3 server.py` in a separate terminal.

2. **Connect two devices:** Use two Termux sessions (or devices). Run `iodine -f -P <password> tunnel.example.com` on each and confirm both have `dns0` interface with `10.0.0.x` addresses.

3. **Launch two clients:** Run `OUTLAW_SERVER=10.0.0.1 python3 client.py` on each device, enter different nicks (e.g., "alice" and "bob").

4. **Verify peer discovery:** Both peers should appear in each other's peer panel. Send a group message from device 1 and confirm it appears on device 2 but is not echoed twice on device 1.

5. **Test direct messages:** On device 1, run `/dm bob`. Send a private message and confirm only device 2 (bob) receives it, not the group.

6. **Test message chunking:** Send a message longer than 140 bytes (e.g., 200 characters) from one device. Confirm it arrives intact on the other (the client reassembles chunks transparently).

7. **Test offline queue and replay:** Close the client on device 2 while keeping device 1 running. From device 1, send a direct message to bob (`/msg bob offline test`). Restart the client on device 2 and confirm the queued message is replayed automatically.

8. **Test server restart:** Kill and restart `server.py` on the VPS. Confirm that both clients re-register themselves within ~10 seconds. Send a message to an offline user (kill device 2's client first), restart device 2, and confirm the persisted message queue survives the server restart and is delivered.
