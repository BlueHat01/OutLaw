def parse_input(text: str) -> dict:
    s = text.strip()
    if not s:
        return {"kind": "noop"}
    if not s.startswith("/"):
        return {"kind": "send", "text": s}
    parts = s[1:].split(" ", 1)
    cmd = parts[0].lower()
    rest = parts[1].strip() if len(parts) > 1 else ""
    if cmd == "who":
        return {"kind": "who"}
    if cmd == "group":
        return {"kind": "group_mode"}
    if cmd == "clear":
        return {"kind": "clear"}
    if cmd == "help":
        return {"kind": "help"}
    if cmd == "quit":
        return {"kind": "quit"}
    if cmd == "dm":
        if not rest:
            return {"kind": "error", "msg": "usage: /dm <nick>"}
        return {"kind": "dm_mode", "nick": rest.split(" ", 1)[0]}
    if cmd == "nick":
        if not rest:
            return {"kind": "error", "msg": "usage: /nick <name>"}
        return {"kind": "nick", "name": rest.split(" ", 1)[0]}
    if cmd == "msg":
        bits = rest.split(" ", 1)
        if len(bits) < 2 or not bits[1].strip():
            return {"kind": "error", "msg": "usage: /msg <nick> <text>"}
        return {"kind": "oneoff_dm", "nick": bits[0], "text": bits[1].strip()}
    if cmd == "img":
        if not rest:
            return {"kind": "error", "msg": "usage: /img <path>"}
        return {"kind": "img", "path": rest.strip()}
    if cmd == "open":
        return {"kind": "open", "arg": rest.split(" ", 1)[0] if rest else "last"}
    if cmd == "history":
        n = 20
        if rest.strip().isdigit():
            n = int(rest.strip())
        return {"kind": "history", "n": n}
    return {"kind": "error", "msg": f"unknown command: /{cmd}"}
