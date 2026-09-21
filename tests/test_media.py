import io
import os
from pathlib import Path
import pytest
from outlaw import media

# Task 1 tests
def test_constants():
    assert media.IMAGE_MAX_BYTES == 24576
    assert media.IMAGE_MAX_EDGE == 800
    assert media.IMAGE_MIN_EDGE == 240
    assert media.IMAGE_MAX_INPUT_BYTES == 26214400
    assert media.IMAGE_WINDOW == 6

def test_ext_for_mime():
    assert media.ext_for_mime("image/jpeg") == ".jpg"
    assert media.ext_for_mime("image/png") == ".png"
    assert media.ext_for_mime("application/x-weird") == ".img"

def test_safe_name_strips_path_and_truncates():
    assert media.safe_name("../../etc/passwd") == "passwd"
    assert media.safe_name("a/b/c/photo.jpg") == "photo.jpg"
    assert len(media.safe_name("x" * 100)) <= 24

# Task 2 tests
pytest.importorskip("PIL")
from PIL import Image

def _make_image(tmp_path, size=(2000, 1500), color=(120, 30, 200)):
    p = tmp_path / "src.png"
    Image.new("RGB", size, color).save(p)
    return str(p)

def test_compress_fits_budget(tmp_path):
    data, mime = media.compress_image(_make_image(tmp_path))
    assert mime == "image/jpeg"
    assert len(data) <= media.IMAGE_MAX_BYTES
    # decodes and longest edge within cap
    img = Image.open(io.BytesIO(data))
    assert max(img.size) <= media.IMAGE_MAX_EDGE

def test_compress_hard_noise_still_fits(tmp_path):
    # random noise is hard to JPEG-compress; forces the edge-downscale path
    import random
    p = tmp_path / "noise.png"
    img = Image.new("RGB", (1600, 1600))
    img.putdata([(random.randint(0,255), random.randint(0,255), random.randint(0,255))
                 for _ in range(1600*1600)])
    img.save(p)
    data, _ = media.compress_image(str(p))
    assert len(data) <= media.IMAGE_MAX_BYTES

def test_compress_rejects_oversize_input(tmp_path):
    p = tmp_path / "big.bin"
    p.write_bytes(b"\0" * (media.IMAGE_MAX_INPUT_BYTES + 1))
    with pytest.raises(media.ImageError):
        media.compress_image(str(p))

# Task 3 tests
def test_chunk_and_join_roundtrip():
    data = bytes(range(256)) * 4
    slices = media.chunk_b64(data, chunk_len=150)
    assert all(len(s) <= 150 for s in slices)
    assert media.join_b64(slices) == data

def test_chunk_b64_slice_fits_packet_budget():
    # 150 base64 chars keeps the im datagram under 230 bytes
    data = bytes(range(200))
    for s in media.chunk_b64(data, 150):
        assert len(s) <= 150

def test_save_image_writes_file(tmp_path):
    path = media.save_image(tmp_path, "alice", "3f9", "image/jpeg", b"\xff\xd8\xff")
    p = Path(path)
    assert p.exists()
    assert p.read_bytes() == b"\xff\xd8\xff"
    assert p.name == "alice-3f9.jpg"

# NEW-5: /open <id> resolution
def test_find_image_resolves_id(tmp_path):
    path = media.save_image(tmp_path, "alice", "abc", "image/jpeg", b"\xff\xd8\xff")
    assert media.find_image(tmp_path, "abc") == path          # <from>-<id>.<ext>
    assert media.find_image(tmp_path, "zzz") is None           # unknown id
    assert media.find_image(tmp_path / "nope", "abc") is None  # missing dir

# NEW-9: open_image must not report success for a missing file
def test_open_image_missing_file_returns_false(tmp_path):
    assert media.open_image(tmp_path / "does-not-exist.jpg") is False
