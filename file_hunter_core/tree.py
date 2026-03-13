"""Full metadata tree walk — streams NDJSON for server-side diffing.

Synchronous BFS generator. Yields one JSON line per record:
  - {"type":"dir","rel_dir":"..."}
  - {"type":"file","rel_path":"...","size":...,"mtime":"..."}
  - {"type":"end","dirs":...,"files":...}

Memory: O(files in one directory) — each directory is yielded then discarded.
"""

import json
import logging
import os
import stat
import time
from collections import deque
from datetime import datetime, timezone

logger = logging.getLogger("file_hunter_agent")


def walk_tree(root: str, prefix: str | None = None):
    """BFS generator yielding NDJSON lines for every dir and file.

    Args:
        root: absolute path to location root
        prefix: optional subdirectory prefix (relative to root) to scope the walk
    """
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

        yield json.dumps({"type": "dir", "rel_dir": rel_dir}) + "\n"
        total_dirs += 1

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

            yield (
                json.dumps(
                    {
                        "type": "file",
                        "rel_path": rel_path,
                        "size": st.st_size,
                        "mtime": mtime,
                    }
                )
                + "\n"
            )
            total_files += 1

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
    yield json.dumps({"type": "end", "dirs": total_dirs, "files": total_files}) + "\n"
