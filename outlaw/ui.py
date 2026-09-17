# outlaw/ui.py
from datetime import datetime
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Header, Footer, Input, RichLog, Static
from outlaw.commands import parse_input
from outlaw import protocol as proto, store

BANNER = r"""
 ▄██████▄  ██    ██ ████████ ██       ▄███▄  ██     ██
██    ██  ██    ██    ██    ██      ██   ██  ██  █  ██
██    ██  ██    ██    ██    ██      ███████  ██ ███ ██
▀██████▀   ▀████▀     ██    ██████  ██   ██   ███ ███
        [ dns0 covert channel · udp relay · v1.0 ]
"""

class OutlawApp(App):
    CSS = """
    Screen { background: #000000; }
    #peers { width: 22; border: round #00ff41; color: #00ff41; }
    #stream { border: round #00ff41; }
    #status { height: 1; color: #ffb000; background: #0a0a0a; }
    Input { border: round #ffb000; color: #00ff41; background: #000000; }
    """
    BINDINGS = [("ctrl+c", "quit", "quit")]

    def __init__(self, session, nick, tunnel_ip, history_path):
        super().__init__()
        self.session = session
        self.nick = nick
        self.tunnel_ip = tunnel_ip
        self.history_path = history_path
        self.mode = ("group", None)   # ("group", None) or ("dm", nick)
        self.link_up = False
        self.tx = self.rx = 0

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal():
            yield Static("", id="peers")
            yield RichLog(id="stream", wrap=True, markup=True)
        yield Static(self._status_text(), id="status")
        yield Input(placeholder="type message or /command", id="entry")
        yield Footer()

    def on_mount(self):
        log = self.query_one("#stream", RichLog)
        for line in BANNER.splitlines():
            log.write(f"[#00ff41]{line}[/]")
        log.write("[#666666]> establishing tunnel ...[/]")
        self.query_one("#entry", Input).focus()

    def _status_text(self):
        mode = "group" if self.mode[0] == "group" else f"dm:{self.mode[1]}"
        link = "[#00ff41]● LINK UP[/]" if self.link_up else "[#ff3333]● LINK DOWN[/]"
        return f"[{mode}] {link}  tx:{self.tx} rx:{self.rx}  /who /dm /group /help /quit"

    def _refresh_status(self):
        self.query_one("#status", Static).update(self._status_text())

    def on_input_submitted(self, event: Input.Submitted):
        self.query_one("#entry", Input).value = ""
        action = parse_input(event.value)
        self._dispatch(action)

    def _dispatch(self, action):
        kind = action["kind"]
        log = self.query_one("#stream", RichLog)
        if kind == "noop":
            return
        if kind == "send":
            to = "__all__" if self.mode[0] == "group" else self.mode[1]
            self.session.send_text(to, action["text"])
            self.tx += 1
            self._echo_own(to, action["text"])
        elif kind == "oneoff_dm":
            self.session.send_text(action["nick"], action["text"])
            self.tx += 1
            self._echo_own(action["nick"], action["text"])
        elif kind == "dm_mode":
            self.mode = ("dm", action["nick"])
        elif kind == "group_mode":
            self.mode = ("group", None)
        elif kind == "nick":
            self.session.set_nick(action["name"])
            self.nick = action["name"]
            log.write(f"[#ffb000]* nick set to {action['name']}[/]")
        elif kind == "who":
            log.write("[#ffb000]* use the peers panel ←[/]")
        elif kind == "clear":
            log.clear()
        elif kind == "help":
            log.write("[#ffb000]/dm <nick> · /group · /msg <nick> <text> · /nick <name> · /who · /clear · /quit[/]")
        elif kind == "quit":
            self.session.leave()
            self.exit()
        elif kind == "error":
            log.write(f"[#ff3333]! {action['msg']}[/]")
        self._refresh_status()

    def _echo_own(self, to, text):
        ts = datetime.now().strftime("%H:%M")
        tgt = "" if to == "__all__" else f" » {to}"
        self.query_one("#stream", RichLog).write(f"[#ffb000]{ts} {self.nick}{tgt}  {text}[/]")
        store.append_line(self.history_path, f"{ts} {self.nick}{tgt}: {text}")

    # --- session callbacks (invoked from asyncio loop; same loop as Textual) ---
    def show_message(self, frm, to, text, ts):
        self.rx += 1
        stamp = datetime.now().strftime("%H:%M")
        tag = "" if to == "__all__" else " » you"
        self.query_one("#stream", RichLog).write(f"[#00ff41]{stamp} {frm}{tag}  {text}[/]")
        store.append_line(self.history_path, f"{stamp} {frm}{tag}: {text}")
        self._refresh_status()

    def set_roster(self, users):
        lines = "\n".join(f"[#00ff41]●[/] {u}" if u != self.nick else f"[#ffb000]●[/] {u} (you)" for u in users)
        self.query_one("#peers", Static).update(f"PEERS\n\n{lines}\n\n{len(users)} online")

    def set_link(self, up):
        self.link_up = up
        self._refresh_status()
