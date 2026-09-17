# client.py
import asyncio
import os
import sys
from pathlib import Path
from outlaw.store import Config
from outlaw.client_net import ClientSession
from outlaw.ui import OutlawApp

DATA_DIR = Path(os.path.expanduser("~/.outlaw"))
SERVER = (os.environ.get("OUTLAW_SERVER", "10.0.0.1"), int(os.environ.get("OUTLAW_PORT", "5005")))

def resolve_nick():
    cfg = Config(DATA_DIR / "config.json")
    nick = cfg.load()
    if not nick:
        nick = input("enter alias: ").strip() or "outlaw"
        cfg.save(nick)
    return nick

async def main():
    nick = resolve_nick()
    tunnel_ip = os.environ.get("OUTLAW_IP", "dns0")
    app = OutlawApp(session=None, nick=nick, tunnel_ip=tunnel_ip,
                    history_path=DATA_DIR / "history.log")
    session = ClientSession(
        nick, SERVER,
        on_message=lambda f, to, x, ts: app.call_from_thread(app.show_message, f, to, x, ts),
        on_roster=lambda users: app.call_from_thread(app.set_roster, users),
        on_link=lambda up: app.call_from_thread(app.set_link, up),
    )
    app.session = session
    await session.start()
    await app.run_async()
    session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
