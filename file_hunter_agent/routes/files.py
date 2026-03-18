"""File operations — content serving, delete, move, write, stat, exists, hash."""

import asyncio
import logging
import mimetypes
import os
import shutil
import time

from starlette.requests import Request
from starlette.responses import FileResponse

from file_hunter_agent.config import is_path_allowed
from file_hunter_agent.response import json_ok, json_error

logger = logging.getLogger(__name__)

_FORBIDDEN = "Path is not within a configured location."

# Backfill hash request tracking
_hash_count = 0
_hash_start: float = 0.0
_hash_last: float = 0.0


def get_hash_status():
    """Return current backfill hash status, or None if idle."""
    if _hash_count == 0:
        return None
    if time.monotonic() - _hash_last > 30:
        return None  # previous batch finished
    return {"count": _hash_count}


async def file_write(request: Request):
    """Write text or base64-decoded bytes to a file.

    Optional 'append' flag (boolean) — if true, appends instead of overwriting.
    """
    body = await request.json()
    path = body.get("path", "")
    content = body.get("content", "")
    encoding = body.get("encoding", "text")
    append = body.get("append", False)
    if not path:
        return json_error("path is required.")
    if not is_path_allowed(path):
        return json_error(_FORBIDDEN, status=403)

    if encoding == "base64":
        import base64

        data = base64.b64decode(content)
        if append:
            await asyncio.to_thread(_append_bytes, path, data)
        else:
            await asyncio.to_thread(_write_bytes, path, data)
    else:
        if append:
            await asyncio.to_thread(_append_text, path, content)
        else:
            await asyncio.to_thread(_write_text, path, content)
    return json_ok({"written": path})


def _write_text(path: str, text: str):
    with open(path, "w") as f:
        f.write(text)


def _append_text(path: str, text: str):
    with open(path, "a") as f:
        f.write(text)


def _append_bytes(path: str, data: bytes):
    with open(path, "ab") as f:
        f.write(data)


def _write_bytes(path: str, data: bytes):
    with open(path, "wb") as f:
        f.write(data)


async def file_stat(request: Request):
    """Return stat info for a file."""
    body = await request.json()
    path = body.get("path", "")
    if not path:
        return json_error("path is required.")
    if not is_path_allowed(path):
        return json_error(_FORBIDDEN, status=403)

    exists = await asyncio.to_thread(os.path.exists, path)
    if not exists:
        return json_ok({"exists": False})

    st = await asyncio.to_thread(os.stat, path)
    return json_ok(
        {
            "exists": True,
            "size": st.st_size,
            "mtime": st.st_mtime,
            "ctime": st.st_ctime,
        }
    )


async def file_exists(request: Request):
    """Check if a path exists (file or directory)."""
    body = await request.json()
    path = body.get("path", "")
    if not path:
        return json_error("path is required.")
    if not is_path_allowed(path):
        return json_error(_FORBIDDEN, status=403)

    is_file = await asyncio.to_thread(os.path.isfile, path)
    is_dir = await asyncio.to_thread(os.path.isdir, path)
    return json_ok({"exists": is_file or is_dir, "is_file": is_file, "is_dir": is_dir})


async def file_hash(request: Request):
    """Hash a file. Default: xxHash64 only. With strong=true: xxHash64 + SHA-256."""
    global _hash_count, _hash_start, _hash_last

    body = await request.json()
    path = body.get("path", "")
    strong = body.get("strong", False)
    if not path:
        return json_error("path is required.")
    if not is_path_allowed(path):
        return json_error(_FORBIDDEN, status=403)

    exists = await asyncio.to_thread(os.path.isfile, path)
    if not exists:
        logger.warning("Backfill hash: file not found: %s", path)
        return json_error("File not found.", status=404)

    _hash_count += 1
    now = time.monotonic()
    if _hash_count == 1:
        _hash_start = now
        logger.info("Backfill hashing started")
    else:
        if now - _hash_last > 30:
            elapsed = _hash_last - _hash_start
            rate = (_hash_count - 1) / elapsed if elapsed > 0 else 0
            logger.info(
                "Backfill hashing complete: %d files in %.1fs (%.1f/sec)",
                _hash_count - 1,
                elapsed,
                rate,
            )
            _hash_count = 1
            _hash_start = now
            logger.info("Backfill hashing started")
    _hash_last = now

    logger.info("Backfill hash #%d: %s", _hash_count, path)

    try:
        if strong:
            from file_hunter_core.hasher import hash_file_sync

            hash_fast, hash_strong = await asyncio.to_thread(hash_file_sync, path)
            return json_ok({"hash_fast": hash_fast, "hash_strong": hash_strong})
        else:
            from file_hunter_core.hasher import hash_fast_only_sync

            hash_fast = await asyncio.to_thread(hash_fast_only_sync, path)
            return json_ok({"hash_fast": hash_fast})
    except OSError as e:
        logger.error("Backfill hash failed: %s: %r", path, e)
        return json_error(f"Hash failed: {e}", status=500)


def _inode_sorted_hash_batch(file_paths, hash_fn, result_key):
    """Sort paths by inode then hash sequentially — minimises disk seeks.

    Args:
        file_paths: list of absolute paths
        hash_fn: callable(path) -> str, the hash function to apply
        result_key: key name for the hash in result dicts (e.g. "hash_partial", "hash_fast")

    Returns (results, errors) tuple.
    """
    import os
    import time

    inode_paths = []
    errors = []
    for p in file_paths:
        if not is_path_allowed(p):
            errors.append({"path": p, "error": "Path not allowed"})
            continue
        try:
            ino = os.stat(p).st_ino
            inode_paths.append((ino, p))
        except FileNotFoundError:
            errors.append({"path": p, "error": "File not found"})
        except OSError as e:
            errors.append({"path": p, "error": str(e)})

    inode_paths.sort(key=lambda x: x[0])

    total = len(inode_paths)
    logger.info("Hash batch: %d files to %s (inode-sorted)", total, result_key)
    t0 = time.monotonic()
    last_log = t0

    results = []
    for _ino, p in inode_paths:
        file_t = time.monotonic()
        try:
            file_size = os.path.getsize(p)
            h = hash_fn(p)
            results.append({"path": p, result_key: h})
        except FileNotFoundError:
            errors.append({"path": p, "error": "File not found"})
        except OSError as e:
            errors.append({"path": p, "error": str(e)})

        elapsed_file = time.monotonic() - file_t
        now = time.monotonic()
        if elapsed_file >= 5.0 or now - last_log >= 5.0:
            done = len(results) + len(errors)
            rate = done / (now - t0) if (now - t0) > 0 else 0
            if elapsed_file >= 5.0:
                size_mb = file_size / (1024 * 1024) if file_size else 0
                logger.info(
                    "Hash batch: %d / %d %s (%.0f/sec) — slow file: %.0fMB %.1fs %s",
                    done,
                    total,
                    result_key,
                    rate,
                    size_mb,
                    elapsed_file,
                    os.path.basename(p),
                )
            else:
                logger.info(
                    "Hash batch: %d / %d %s (%.0f files/sec)",
                    done,
                    total,
                    result_key,
                    rate,
                )
            last_log = now

    elapsed = time.monotonic() - t0
    logger.info(
        "Hash batch complete: %d hashed, %d errors in %.1fs",
        len(results),
        len(errors),
        elapsed,
    )
    return results, errors


async def hash_partial_batch(request: Request):
    """Compute hash_partial for a batch of file paths, sorted by inode for disk locality."""
    body = await request.json()
    paths = body.get("paths", [])
    if not paths:
        return json_error("paths is required.")

    from file_hunter_core.hasher import hash_file_partial_sync

    results, errors = await asyncio.to_thread(
        _inode_sorted_hash_batch, paths, hash_file_partial_sync, "hash_partial"
    )
    return json_ok({"results": results, "errors": errors})


async def hash_fast_batch(request: Request):
    """Compute hash_fast (xxHash64 full file) for a batch of file paths, sorted by inode."""
    body = await request.json()
    paths = body.get("paths", [])
    if not paths:
        return json_error("paths is required.")

    from file_hunter_core.hasher import hash_fast_only_sync

    results, errors = await asyncio.to_thread(
        _inode_sorted_hash_batch, paths, hash_fast_only_sync, "hash_fast"
    )
    return json_ok({"results": results, "errors": errors})


async def file_content(request: Request):
    """Serve raw file bytes with correct MIME type."""
    path = request.query_params.get("path", "")
    if not path:
        return json_error("path is required.")
    if not is_path_allowed(path):
        return json_error(_FORBIDDEN, status=403)

    exists = await asyncio.to_thread(os.path.isfile, path)
    if not exists:
        return json_error("File not found.", status=404)

    content_type, _ = mimetypes.guess_type(path)
    return FileResponse(path, media_type=content_type or "application/octet-stream")


async def stream_write(request: Request):
    """Accept raw binary body and stream it to a file on disk.

    Path is provided as a query parameter: POST /files/stream-write?path=...
    """
    path = request.query_params.get("path", "")
    if not path:
        return json_error("path is required.")
    if not is_path_allowed(path):
        return json_error(_FORBIDDEN, status=403)

    parent = os.path.dirname(path)
    if not await asyncio.to_thread(os.path.isdir, parent):
        return json_error("Parent directory not found.", status=404)

    total = 0
    with open(path, "wb") as f:
        async for chunk in request.stream():
            f.write(chunk)
            total += len(chunk)

    return json_ok({"written": path, "size": total})


async def file_delete(request: Request):
    """Delete a file from disk."""
    body = await request.json()
    path = body.get("path", "")
    if not path:
        return json_error("path is required.")
    if not is_path_allowed(path):
        return json_error(_FORBIDDEN, status=403)

    exists = await asyncio.to_thread(os.path.isfile, path)
    if not exists:
        return json_error("File not found.", status=404)

    await asyncio.to_thread(os.remove, path)
    return json_ok({"deleted": path})


async def file_move(request: Request):
    """Move/rename a file."""
    body = await request.json()
    src = body.get("path", "")
    dest = body.get("destination", "")
    if not src or not dest:
        return json_error("path and destination are required.")
    if not is_path_allowed(src):
        return json_error(_FORBIDDEN, status=403)
    if not is_path_allowed(dest):
        return json_error(_FORBIDDEN, status=403)

    exists = await asyncio.to_thread(os.path.isfile, src)
    if not exists:
        return json_error("Source file not found.", status=404)

    await asyncio.to_thread(shutil.move, src, dest)
    return json_ok({"moved": src, "destination": dest})
