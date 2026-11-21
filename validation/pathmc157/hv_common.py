"""Small stdlib-only helpers for the PathMC #157 hosted packet."""
import hashlib
import json
import os
import tempfile

MAX_JSON = 32 * 1024 * 1024

def atomic_json(path, value):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp, path)
    finally:
        try: os.unlink(tmp)
        except FileNotFoundError: pass

def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()

def load_json(path, limit=MAX_JSON):
    if os.path.islink(path) or not os.path.isfile(path):
        raise ValueError("not a regular file: " + path)
    if os.path.getsize(path) > limit:
        raise ValueError("JSON exceeds byte limit: " + path)
    with open(path, "rb") as handle:
        raw = handle.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("JSON grew beyond byte limit: " + path)
    return json.loads(raw.decode("utf-8"))
