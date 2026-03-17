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


def walk_tree(root: str, prefix: str | None = None, fmt: str = "json"):
    """BFS generator yielding lines for every dir and file.

    Args:
        root: absolute path to location root
        prefix: optional subdirectory prefix (relative to root) to scope the walk
        fmt: "json" for NDJSON (default), "tsv" for tab-separated values

    TSV format (tabs in filenames converted to spaces):
        D\\trel_dir\\n
        F\\trel_path\\tsize\\tmtime\\tctime\\n
        E\\tdirs\\tfiles\\n
    """
    use_tsv = fmt == "tsv"
    scope = prefix or root
    logger.info("Tree walk starting: %s (format=%s)", scope, fmt)
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

        if use_tsv:
            yield f"D\t{rel_dir.replace(chr(9), ' ')}\n"
        else:
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
            ctime = datetime.fromtimestamp(
                st.st_birthtime if hasattr(st, "st_birthtime") else st.st_ctime,
                tz=timezone.utc,
            ).isoformat(timespec="seconds")

            if use_tsv:
                yield f"F\t{rel_path.replace(chr(9), ' ')}\t{st.st_size}\t{mtime}\t{ctime}\n"
            else:
                yield (
                    json.dumps(
                        {
                            "type": "file",
                            "rel_path": rel_path,
                            "size": st.st_size,
                            "mtime": mtime,
                            "ctime": ctime,
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
    if use_tsv:
        yield f"E\t{total_dirs}\t{total_files}\n"
    else:
        yield (
            json.dumps({"type": "end", "dirs": total_dirs, "files": total_files}) + "\n"
        )
