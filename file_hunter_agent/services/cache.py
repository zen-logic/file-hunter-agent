"""Scan cache — tracks file state between scans to enable incremental mode.

Stores (rel_path, file_size, modified_date) per location in a SQLite database.
On rescan, the agent compares the current filesystem state against the cache
to identify new, changed, and deleted files. Only new/changed files are hashed.

The cache is updated incrementally during scanning so that a cancelled scan
can resume where it left off.
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


class ScanCache:
    """Persistent scan cache backed by a per-location SQLite DB."""

    def __init__(self, location_path: str):
        self.location_path = location_path
        self._db = None

    def open(self):
        self._db = sqlite3.connect(
            _cache_path(self.location_path), check_same_thread=False
        )
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute(
            """CREATE TABLE IF NOT EXISTS files (
                rel_path TEXT PRIMARY KEY,
                file_size INTEGER NOT NULL,
                modified_date TEXT NOT NULL
            )"""
        )

    def close(self):
        if self._db:
            self._db.close()
            self._db = None

    def load(self) -> dict[str, tuple[int, str]]:
        """Load all cached entries. Returns {rel_path: (file_size, modified_date)}."""
        rows = self._db.execute(
            "SELECT rel_path, file_size, modified_date FROM files"
        ).fetchall()
        return {row[0]: (row[1], row[2]) for row in rows}

    def has_entries(self) -> bool:
        """Check if the cache has any entries (without loading all)."""
        row = self._db.execute("SELECT 1 FROM files LIMIT 1").fetchone()
        return row is not None

    def update_batch(self, files: list[dict]):
        """Upsert a batch of scanned files into the cache."""
        self._db.executemany(
            "INSERT OR REPLACE INTO files (rel_path, file_size, modified_date) VALUES (?, ?, ?)",
            [(f["rel_path"], f["file_size"], f["modified_date"]) for f in files],
        )
        self._db.commit()

    def remove_deleted(self, rel_paths: list[str]):
        """Remove deleted file entries from the cache."""
        for i in range(0, len(rel_paths), 500):
            batch = rel_paths[i : i + 500]
            placeholders = ",".join("?" * len(batch))
            self._db.execute(
                f"DELETE FROM files WHERE rel_path IN ({placeholders})", batch
            )
        self._db.commit()
