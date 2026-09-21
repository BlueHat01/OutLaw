import os
import io
import base64
import shutil
import subprocess
from pathlib import Path

# Task 1: Constants
IMAGE_MAX_BYTES = 24576
IMAGE_MAX_EDGE = 800
IMAGE_MIN_EDGE = 240
IMAGE_START_QUALITY = 85
IMAGE_MIN_QUALITY = 30
IMAGE_MAX_INPUT_BYTES = 26214400
IMAGE_WINDOW = 6
IMAGE_TRANSFER_TIMEOUT = 120

_MIME_EXT = {"image/jpeg": ".jpg", "image/png": ".png"}

# Task 1: Filename/MIME helpers
def ext_for_mime(mime: str) -> str:
    return _MIME_EXT.get(mime, ".img")

def safe_name(name: str, maxlen: int = 24) -> str:
    base = os.path.basename(str(name).replace("\\", "/"))
    base = base.strip() or "image"
    return base[:maxlen]

# Task 2: Image compression
class ImageError(Exception):
    pass

def _encode(img, quality) -> bytes:
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=quality, optimize=True)
    return buf.getvalue()

def compress_image(path: str):
    try:
        from PIL import Image
    except ImportError:
        raise ImageError("Pillow not installed — run: pip install Pillow")
    if os.path.getsize(path) > IMAGE_MAX_INPUT_BYTES:
        raise ImageError("image too large (>25 MB)")
    try:
        img = Image.open(path)
        img = img.convert("RGB")
    except Exception as e:
        raise ImageError(f"cannot read image: {e}")

    edge = IMAGE_MAX_EDGE
    while True:
        work = img.copy()
        work.thumbnail((edge, edge))  # preserves aspect, longest edge <= edge
        for q in range(IMAGE_START_QUALITY, IMAGE_MIN_QUALITY - 1, -5):
            data = _encode(work, q)
            if len(data) <= IMAGE_MAX_BYTES:
                return data, "image/jpeg"
        if edge <= IMAGE_MIN_EDGE:
            raise ImageError("cannot compress image under budget")
        edge = max(IMAGE_MIN_EDGE, int(edge * 0.8))

# Task 3: Base64 chunking and save/open helpers
def chunk_b64(data: bytes, chunk_len: int = 150) -> list:
    s = base64.b64encode(data).decode("ascii")
    return [s[i:i + chunk_len] for i in range(0, len(s), chunk_len)] or [""]

def join_b64(slices) -> bytes:
    return base64.b64decode("".join(slices).encode("ascii"))

def save_image(media_dir, frm, mid, mime, data) -> str:
    d = Path(media_dir)
    d.mkdir(parents=True, exist_ok=True)
    name = f"{safe_name(frm)}-{mid}{ext_for_mime(mime)}"
    path = d / name
    path.write_bytes(data)
    return str(path)

def find_image(media_dir, mid):
    """Resolve a transfer id to its saved file (<from>-<mid>.<ext>)."""
    d = Path(media_dir)
    if not d.exists():
        return None
    for p in sorted(d.glob(f"*-{mid}.*")):
        return str(p)
    return None

def open_image(path) -> bool:
    if not os.path.exists(str(path)):
        return False
    for launcher in ("termux-open", "xdg-open"):
        if shutil.which(launcher):
            try:
                subprocess.Popen([launcher, str(path)])
                return True
            except Exception:
                pass
    return False
