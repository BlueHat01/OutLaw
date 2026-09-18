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
