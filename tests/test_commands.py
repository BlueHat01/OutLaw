from outlaw.commands import parse_input

def test_plain_text_is_send():
    assert parse_input("hello world") == {"kind": "send", "text": "hello world"}

def test_empty_is_noop():
    assert parse_input("   ") == {"kind": "noop"}

def test_dm_mode():
    assert parse_input("/dm alice") == {"kind": "dm_mode", "nick": "alice"}

def test_group_mode():
    assert parse_input("/group") == {"kind": "group_mode"}

def test_oneoff_dm():
    assert parse_input("/msg bob hey there") == {"kind": "oneoff_dm", "nick": "bob", "text": "hey there"}

def test_nick_change():
    assert parse_input("/nick ghost") == {"kind": "nick", "name": "ghost"}

def test_simple_commands():
    assert parse_input("/who") == {"kind": "who"}
    assert parse_input("/clear") == {"kind": "clear"}
    assert parse_input("/help") == {"kind": "help"}
    assert parse_input("/quit") == {"kind": "quit"}

def test_unknown_command_is_error():
    assert parse_input("/frobnicate")["kind"] == "error"

def test_dm_without_arg_is_error():
    assert parse_input("/dm")["kind"] == "error"

def test_img_command():
    assert parse_input("/img /sdcard/pic.jpg") == {"kind": "img", "path": "/sdcard/pic.jpg"}

def test_img_requires_path():
    assert parse_input("/img")["kind"] == "error"

def test_open_defaults_to_last():
    assert parse_input("/open") == {"kind": "open", "arg": "last"}
    assert parse_input("/open 3f9") == {"kind": "open", "arg": "3f9"}

def test_history_default_and_arg():
    assert parse_input("/history") == {"kind": "history", "n": 20}
    assert parse_input("/history 50") == {"kind": "history", "n": 50}
