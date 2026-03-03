"""Dual-hash file computation (xxHash64 + SHA-256) in a single pass.

All functions are synchronous. The server wraps these in asyncio.to_thread().
"""

import hashlib
import os

import xxhash

CHUNK_SIZE = 1024 * 1024  # 1 MB
PARTIAL_SIZE = 4 * 1024 * 1024  # 4 MB


def hash_file_sync(path: str) -> tuple[str, str]:
    """Read file once, feed both hashers, return (xxhash64_hex, sha256_hex)."""
    xx = xxhash.xxh64()
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            xx.update(chunk)
            sha.update(chunk)
    return xx.hexdigest(), sha.hexdigest()


def hash_file_partial_sync(path: str) -> str:
    """xxHash64 of first 4MB + last 4MB. For files <= 8MB, reads everything."""
    xx = xxhash.xxh64()
    file_size = os.path.getsize(path)
    with open(path, "rb") as f:
        if file_size <= PARTIAL_SIZE * 2:
            while chunk := f.read(CHUNK_SIZE):
                xx.update(chunk)
        else:
            # First 4MB
            read = 0
            while read < PARTIAL_SIZE:
                chunk = f.read(min(CHUNK_SIZE, PARTIAL_SIZE - read))
                if not chunk:
                    break
                xx.update(chunk)
                read += len(chunk)
            # Last 4MB
            f.seek(-PARTIAL_SIZE, 2)
            while chunk := f.read(CHUNK_SIZE):
                xx.update(chunk)
    return xx.hexdigest()
