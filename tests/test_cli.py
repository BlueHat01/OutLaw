from client_cli import CliChat


class FakeSession:
    def __init__(self):
        self.sent = []
        self.nick_changes = []
        self.left = False

    def send_text(self, to, text):
        self.sent.append((to, text))

    def set_nick(self, name):
        self.nick_changes.append(name)

    def leave(self):
        self.left = True

    def close(self):
        pass


def make_chat():
    out = []
    fake = FakeSession()
    chat = CliChat("rahul", lambda c: fake, out=out.append, history_path=None)
    return chat, fake, out


def test_plain_text_sends_to_group():
    chat, fake, out = make_chat()
    chat.dispatch("hello all")
    assert fake.sent == [("__all__", "hello all")]


def test_dm_mode_then_send_targets_peer():
    chat, fake, out = make_chat()
    chat.dispatch("/dm alice")
    chat.dispatch("hi there")
    assert fake.sent == [("alice", "hi there")]
    assert chat.mode == ("dm", "alice")


def test_group_mode_switch_returns_to_broadcast():
    chat, fake, out = make_chat()
    chat.dispatch("/dm alice")
    chat.dispatch("/group")
    chat.dispatch("yo")
    assert fake.sent[-1] == ("__all__", "yo")
    assert chat.mode == ("group", None)


def test_oneoff_dm_keeps_group_mode():
    chat, fake, out = make_chat()
    chat.dispatch("/msg bob secret")
    assert ("bob", "secret") in fake.sent
    assert chat.mode == ("group", None)


def test_nick_change():
    chat, fake, out = make_chat()
    chat.dispatch("/nick ghost")
    assert fake.nick_changes == ["ghost"]
    assert chat.nick == "ghost"


def test_quit_leaves_and_stops():
    chat, fake, out = make_chat()
    chat.dispatch("/quit")
    assert chat._running is False
    assert fake.left is True


def test_noop_on_blank_line():
    chat, fake, out = make_chat()
    chat.dispatch("   ")
    assert fake.sent == []


def test_incoming_message_is_printed():
    chat, fake, out = make_chat()
    chat.on_message("alice", "__all__", "hey", 123)
    assert any("alice" in line and "hey" in line for line in out)


def test_dm_incoming_shows_you_tag():
    chat, fake, out = make_chat()
    chat.on_message("alice", "rahul", "psst", 123)
    assert any("»you" in line for line in out)


def test_roster_stored_for_who():
    chat, fake, out = make_chat()
    chat.on_roster(["rahul", "alice"])
    out.clear()
    chat.dispatch("/who")
    assert any("alice" in line for line in out)


def test_unknown_command_prints_error():
    chat, fake, out = make_chat()
    chat.dispatch("/bogus")
    assert any("!" in line for line in out)


class FakeSessionImg(FakeSession):
    def __init__(self):
        super().__init__()
        self.images = []
    def send_image(self, to, path):
        self.images.append((to, path)); return 3


def make_chat_img(tmp_path):
    out = []
    fake = FakeSessionImg()
    from client_cli import CliChat
    chat = CliChat("rahul", lambda c: fake, out=out.append,
                   history_path=tmp_path / "history.log")
    return chat, fake, out


def test_img_command_sends(tmp_path):
    chat, fake, out = make_chat_img(tmp_path)
    chat.dispatch("/img /sdcard/p.jpg")
    assert fake.images == [("__all__", "/sdcard/p.jpg")]


def test_img_error_is_reported(tmp_path):
    chat, fake, out = make_chat_img(tmp_path)
    def boom(to, path):
        from outlaw.media import ImageError
        raise ImageError("Pillow not installed")
    fake.send_image = boom
    chat.dispatch("/img /x.jpg")
    assert any("Pillow" in line for line in out)


def test_on_image_prints_and_records(tmp_path):
    chat, fake, out = make_chat_img(tmp_path)
    chat.on_image("alice", "__all__", "/data/media/alice-1.jpg", {"name": "p.jpg"})
    assert any("alice" in line and "image" in line.lower() for line in out)
    from outlaw import store
    rows = store.load_recent_history(tmp_path / "history.jsonl")
    assert any(r.get("kind") == "image" for r in rows)


def test_history_command_prints_recent(tmp_path):
    from outlaw import store
    store.append_history(tmp_path / "history.jsonl", {"ts": 1, "frm": "a", "to": "__all__", "kind": "text", "text": "old line"})
    chat, fake, out = make_chat_img(tmp_path)
    out.clear()
    chat.dispatch("/history 5")
    assert any("old line" in line for line in out)
