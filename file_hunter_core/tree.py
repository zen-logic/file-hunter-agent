"""Full metadata tree walk — streams directory-at-a-time over HTTP.

Synchronous BFS generator. Each yield is one complete directory:
  D record, then all F records (inode-sorted), ready for server ingest.

TSV format (tabs in filenames converted to spaces):
    D\trel_dir\n
    F\trel_path\tsize\tmtime\tctime\tinode\thash_partial\n
    E\ttotal_dirs\ttotal_files\n

Memory: O(files in one directory) — each directory is yielded then discarded.
"""

import logging
import os
import stat
import time
from collections import deque
from datetime import datetime, timezone

from file_hunter_core.hasher import hash_file_partial_sync

logger = logging.getLogger("file_hunter_agent")


def walk_tree(root: str, prefix: str | None = None, fmt: str = "tsv"):
    """BFS generator yielding one chunk per directory.

    Each chunk contains the D record followed by all F records for that
    directory, sorted by inode for disk locality when hashing.

    Args:
        root: absolute path to location root
        prefix: optional subdirectory prefix (relative to root) to scope the walk
        fmt: "tsv" (default). Legacy "json" is no longer supported.
    """
    if fmt != "tsv":
        raise ValueError(f"Unsupported format: {fmt!r} (only 'tsv' is supported)")

    scope = prefix or root
    logger.info("Tree walk starting: %s", scope)
    t0 = time.monotonic()
    last_log = t0
    start = os.path.join(root, prefix) if prefix else root
    queue = deque([start])
    total_dirs = 0
    total_files = 0

    while queue:
        dirpath = queue.popleft()

        rel_dir = os.path.relpath(dirpath, root)
        if rel_dir == ".":
            rel_dir = ""

        now = time.monotonic()
        if now - last_log >= 5.0:
            logger.info(
                "Tree walk progress: %s — %d dirs, %d files so far (%.1fs)",
                scope,
                total_dirs,
                total_files,
                now - t0,
            )
            last_log = now

        try:
            entries = list(os.scandir(dirpath))
        except (PermissionError, OSError):
            continue

        subdirs = []
        file_entries = []

        for entry in entries:
            try:
                if entry.is_symlink():
                    continue
                if entry.is_dir(follow_symlinks=False):
                    subdirs.append(entry.path)
                    continue
                st = entry.stat(follow_symlinks=False)
            except OSError:
                continue

            if not stat.S_ISREG(st.st_mode):
                continue

            rel_path = os.path.join(rel_dir, entry.name) if rel_dir else entry.name
            mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(
                timespec="seconds"
            )
            ctime = datetime.fromtimestamp(
                st.st_birthtime if hasattr(st, "st_birthtime") else st.st_ctime,
                tz=timezone.utc,
            ).isoformat(timespec="seconds")

            file_entries.append(
                (st.st_ino, entry.path, rel_path, st.st_size, mtime, ctime)
            )

        # Sort by inode for disk locality when hashing
        file_entries.sort(key=lambda e: e[0])

        # Build the chunk: D record + F records for this directory
        lines = [f"D\t{rel_dir.replace(chr(9), ' ')}\n"]

        for ino, full_path, rel_path, size, mtime, ctime in file_entries:
            # Hash partial: skip zero-byte files
            hp = ""
            if size > 0:
                try:
                    hp = hash_file_partial_sync(full_path)
                except (OSError, PermissionError):
                    pass

            safe_rel = rel_path.replace(chr(9), " ")
            lines.append(f"F\t{safe_rel}\t{size}\t{mtime}\t{ctime}\t{ino}\t{hp}\n")

        total_dirs += 1
        total_files += len(file_entries)

        yield "".join(lines)

        for sd in sorted(subdirs):
            queue.append(sd)

    elapsed = time.monotonic() - t0
    logger.info(
        "Tree walk complete: %s — %d dirs, %d files in %.1fs",
        scope,
        total_dirs,
        total_files,
        elapsed,
    )
    yield f"E\t{total_dirs}\t{total_files}\n"
