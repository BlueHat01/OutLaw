import asyncio
from outlaw.server import run_server

if __name__ == "__main__":
    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        print("\n[outlaw] relay stopped")
