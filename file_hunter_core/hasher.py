"""Dual-hash file computation (xxHash64 + SHA-256) in a single pass.

All functions are synchronous. The server wraps these in asyncio.to_thread().
"""

import hashlib
import os

import xxhash

CHUNK_SIZE = 1024 * 1024  # 1 MB
PARTIAL_SIZE = 64 * 1024  # 64 KB


def hash_file_sync(path: str) -> tuple[str, str]:
    """Read file once, feed both hashers, return (xxhash64_hex, sha256_hex)."""
    xx = xxhash.xxh64()
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            xx.update(chunk)
            sha.update(chunk)
    return xx.hexdigest(), sha.hexdigest()


def hash_fast_only_sync(path: str) -> str:
    """Read file once, return xxHash64 hex only. ~10x faster than dual hash."""
    xx = xxhash.xxh64()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            xx.update(chunk)
    return xx.hexdigest()


def hash_partial_and_fast_sync(path: str) -> tuple[str, str]:
    """Read file once, return (hash_partial, hash_fast).

    hash_partial: xxHash64 of first 64KB + last 64KB
    hash_fast: xxHash64 of the full file

    For files <= 128KB both hashes are identical (full file is read either way).
    Single sequential read — no extra I/O.
    """
    file_size = os.path.getsize(path)
    xx_fast = xxhash.xxh64()

    if file_size <= PARTIAL_SIZE * 2:
        # Small file — full read, both hashes are the same
        with open(path, "rb") as f:
            while chunk := f.read(CHUNK_SIZE):
                xx_fast.update(chunk)
        h = xx_fast.hexdigest()
        return h, h

    # Large file — read full file for hash_fast, track first/last for hash_partial
    first_buf = bytearray()
    last_buf = bytearray()

    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            xx_fast.update(chunk)
            if len(first_buf) < PARTIAL_SIZE:
                need = PARTIAL_SIZE - len(first_buf)
                first_buf.extend(chunk[:need])
            # Maintain a rolling buffer of the last PARTIAL_SIZE bytes
            last_buf.extend(chunk)
            if len(last_buf) > PARTIAL_SIZE:
                last_buf = last_buf[-PARTIAL_SIZE:]

    xx_partial = xxhash.xxh64()
    xx_partial.update(bytes(first_buf))
    xx_partial.update(bytes(last_buf))

    return xx_partial.hexdigest(), xx_fast.hexdigest()


def hash_file_partial_sync(path: str) -> str:
    """xxHash64 of first 64KB + last 64KB. For files <= 128KB, reads everything."""
    xx = xxhash.xxh64()
    file_size = os.path.getsize(path)
    with open(path, "rb") as f:
        if file_size <= PARTIAL_SIZE * 2:
            while chunk := f.read(CHUNK_SIZE):
                xx.update(chunk)
        else:
            # First 64KB
            read = 0
            while read < PARTIAL_SIZE:
                chunk = f.read(min(CHUNK_SIZE, PARTIAL_SIZE - read))
                if not chunk:
                    break
                xx.update(chunk)
                read += len(chunk)
            # Last 64KB
            f.seek(-PARTIAL_SIZE, 2)
            while chunk := f.read(CHUNK_SIZE):
                xx.update(chunk)
    return xx.hexdigest()
