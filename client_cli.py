# client_cli.py — Outlaw line-mode client (no Textual).
# Same protocol and ClientSession as the full-screen client; a plain scrolling
# terminal instead of a TUI. Use this on terminals where the Textual UI misbehaves
# (e.g. Termux input-handshake gibberish). Run: python3 client_cli.py
import asyncio
import os
import time
from pathlib import Path

from outlaw.store import Config, append_line
from outlaw.client_net import ClientSession
from outlaw.commands import parse_input
from outlaw import media, store as _store
from outlaw.media import ImageError

DATA_DIR = Path(os.path.expanduser("~/.outlaw"))
SERVER = (os.environ.get("OUTLAW_SERVER", "10.0.0.1"),
          int(os.environ.get("OUTLAW_PORT", "5005")))

# ANSI colors (work in Termux); harmless if a terminal ignores them.
G = "\033[38;5;46m"    # matrix green — others' messages
A = "\033[38;5;208m"   # amber — your own / mode
D = "\033[38;5;244m"   # dim — system
RED = "\033[31m"       # errors / link down
X = "\033[0m"          # reset


def _stamp():
    return time.strftime("%H:%M")


class CliChat:
    """Terminal front-end. All UI is line-based print(); no curses/textual."""

    def __init__(self, nick, session_factory, out=print, history_path=None):
        self.nick = nick
        self.mode = ("group", None)   # ("group", None) or ("dm", nick)
        self.roster = []
        self.out = out
        self.history_path = history_path
        self.jsonl_path = Path(self.history_path).with_suffix(".jsonl") if self.history_path else None
        self.last_image = None
        self._running = True
        if self.jsonl_path:
            self._preload = _store.load_recent_history(self.jsonl_path)
        self.session = session_factory(self)

    # --- ClientSession callbacks (called on the asyncio loop) ---
    def on_message(self, frm, to, text, ts):
        tag = "" if to == "__all__" else " »you"
        self.out(f"{G}{_stamp()} {frm}{tag}{X}  {text}")
        self._log(f"{_stamp()} {frm}{tag}: {text}")
        self._log_entry({"ts": int(ts) if ts else int(time.time()), "frm": frm, "to": to,
                          "kind": "text", "text": text})

    def on_image(self, frm, to, path, meta):
        tag = "" if to == "__all__" else " »you"
        self.out(f"{G}{_stamp()} {frm}{tag}  [image saved: {path}] (/open){X}")
        self.last_image = path
        self._log_entry({"ts": int(time.time()), "frm": frm, "to": to,
                          "kind": "image", "path": path, "name": (meta or {}).get("name")})

    def on_roster(self, users):
        self.roster = users
        shown = ", ".join(users) if users else "(none)"
        self.out(f"{D}* online: {shown}{X}")

    def on_link(self, up):
        self.out(f"{G}* LINK UP{X}" if up else f"{RED}* LINK DOWN{X}")

    def on_error(self, msg):
        self.out(f"{RED}! {msg}{X}")

    # --- input handling ---
    def dispatch(self, line):
        act = parse_input(line)
        k = act["kind"]
        if k == "noop":
            return
        if k == "send":
            to = "__all__" if self.mode[0] == "group" else self.mode[1]
            self.session.send_text(to, act["text"])
            self._echo(to, act["text"])
        elif k == "oneoff_dm":
            self.session.send_text(act["nick"], act["text"])
            self._echo(act["nick"], act["text"])
        elif k == "dm_mode":
            self.mode = ("dm", act["nick"])
            self.out(f"{A}* now DMing {act['nick']} (type /group to return){X}")
        elif k == "group_mode":
            self.mode = ("group", None)
            self.out(f"{A}* group mode{X}")
        elif k == "nick":
            self.session.set_nick(act["name"])
            self.nick = act["name"]
            self.out(f"{A}* nick set to {act['name']}{X}")
        elif k == "who":
            shown = ", ".join(self.roster) if self.roster else "(none)"
            self.out(f"{A}* online: {shown}{X}")
        elif k == "clear":
            self.out("\033[2J\033[H")
        elif k == "help":
            self.out(f"{A}/dm <nick>  /group  /msg <nick> <text>  "
                     f"/nick <name>  /who  /clear  /quit{X}")
        elif k == "quit":
            self.session.leave()
            self._running = False
        elif k == "error":
            self.out(f"{RED}! {act['msg']}{X}")
        elif k == "img":
            try:
                to = "__all__" if self.mode[0] == "group" else self.mode[1]
                self.session.send_image(to, act["path"])
                self.out(f"{A}* sending image → {act['path']}{X}")
            except ImageError as e:
                self.out(f"{RED}! {e}{X}")
        elif k == "open":
            target = self.last_image if act["arg"] == "last" else act["arg"]
            if target and media.open_image(target):
                self.out(f"{D}* opening {target}{X}")
            else:
                self.out(f"{RED}! nothing to open{X}")
        elif k == "history":
            rows = _store.load_recent_history(self.jsonl_path, act["n"]) if self.jsonl_path else []
            for r in rows:
                if r.get("kind") == "image":
                    self.out(f"{D}{r.get('frm')}: [image {r.get('name')}]{X}")
                else:
                    self.out(f"{D}{r.get('frm')}: {r.get('text', '')}{X}")

    def _echo(self, to, text):
        tgt = "" if to == "__all__" else f" »{to}"
        self.out(f"{A}{_stamp()} {self.nick}{tgt}{X}  {text}")
        self._log(f"{_stamp()} {self.nick}{tgt}: {text}")
        self._log_entry({"ts": int(time.time()), "frm": self.nick, "to": to,
                          "kind": "text", "text": text})

    def _log(self, line):
        if self.history_path:
            append_line(self.history_path, line)

    def _log_entry(self, entry):
        if self.jsonl_path:
            _store.append_history(self.jsonl_path, entry)


def resolve_nick():
    cfg = Config(DATA_DIR / "config.json")
    nick = cfg.load()
    if not nick:
        nick = input("enter alias: ").strip() or "outlaw"
        cfg.save(nick)
    return nick


async def main():
    nick = resolve_nick()
    print(f"{G}== OUTLAW (line mode) =={X}")

    def factory(chat):
        return ClientSession(
            nick, SERVER,
            on_message=chat.on_message,
            on_roster=chat.on_roster,
            on_link=chat.on_link,
            on_error=chat.on_error,
            on_image=chat.on_image,
            media_dir=DATA_DIR / "media",
        )

    chat = CliChat(nick, factory, out=print, history_path=DATA_DIR / "history.log")
    await chat.session.start()
    print(f"{D}connecting to {SERVER[0]}:{SERVER[1]} as {nick} ... type /help{X}")

    while chat._running:
        try:
            line = await asyncio.to_thread(input, "")
        except (EOFError, KeyboardInterrupt):
            chat.session.leave()
            break
        chat.dispatch(line)

    chat.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
