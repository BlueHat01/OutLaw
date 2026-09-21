# client.py
import asyncio
import os
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
    # Callbacks fire on the same asyncio loop Textual runs on, so call the app
    # methods directly (no call_from_thread). The session is started from
    # OutlawApp.on_mount so widgets already exist when packets arrive.
    session = ClientSession(
        nick, SERVER,
        on_message=app.show_message,
        on_roster=app.set_roster,
        on_link=app.set_link,
        on_error=app.show_system,
        on_image=app.on_image,
        media_dir=DATA_DIR / "media",
    )
    app.session = session
    try:
        await app.run_async()
    finally:
        session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
