"""Scan cache — tracks file state between scans to enable incremental mode.

Stores (rel_path, file_size, modified_date) per location in a SQLite database.
On rescan, the agent compares the current filesystem state against the cache
to identify new, changed, and deleted files. Only new/changed files are hashed.
"""

import hashlib
import logging
import os
import sqlite3

logger = logging.getLogger("file_hunter_agent")

_CACHE_DIR = ".cache"


def _cache_path(location_path: str) -> str:
    """Return the cache DB path for a location."""
    key = hashlib.sha256(location_path.encode()).hexdigest()[:16]
    os.makedirs(_CACHE_DIR, exist_ok=True)
    return os.path.join(_CACHE_DIR, f"{key}.db")


def _open(location_path: str) -> sqlite3.Connection:
    db = sqlite3.connect(_cache_path(location_path))
    db.execute("PRAGMA journal_mode=WAL")
    db.execute(
        """CREATE TABLE IF NOT EXISTS files (
            rel_path TEXT PRIMARY KEY,
            file_size INTEGER NOT NULL,
            modified_date TEXT NOT NULL
        )"""
    )
    return db


def load_cache(location_path: str) -> dict[str, tuple[int, str]]:
    """Load the scan cache for a location.

    Returns {rel_path: (file_size, modified_date)} or empty dict if no cache.
    """
    path = _cache_path(location_path)
    if not os.path.exists(path):
        return {}

    db = _open(location_path)
    try:
        rows = db.execute("SELECT rel_path, file_size, modified_date FROM files").fetchall()
        return {row[0]: (row[1], row[2]) for row in rows}
    finally:
        db.close()


def save_cache(location_path: str, files: dict[str, tuple[int, str]]):
    """Replace the scan cache for a location.

    files: {rel_path: (file_size, modified_date)}
    """
    db = _open(location_path)
    try:
        db.execute("DELETE FROM files")
        batch = [(rp, size, mtime) for rp, (size, mtime) in files.items()]
        db.executemany(
            "INSERT INTO files (rel_path, file_size, modified_date) VALUES (?, ?, ?)",
            batch,
        )
        db.commit()
        logger.info("Cache saved: %d files for %s", len(batch), location_path)
    finally:
        db.close()
