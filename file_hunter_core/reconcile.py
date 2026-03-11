"""Directory reconciliation — compare expected catalog contents against disk reality.

Synchronous. The agent endpoint wraps this in asyncio.to_thread().
"""

import os
import stat
from datetime import datetime, timezone

from file_hunter_core.classify import classify_file
from file_hunter_core.hasher import hash_file_partial_sync


def reconcile_directory(dirpath: str, root_path: str, expected: list[dict]) -> dict:
    """Compare expected catalog entries against what's on disk.

    Args:
        dirpath: absolute path to the directory to iterate
        root_path: location root for rel_path computation
        expected: list of dicts from the server's catalog, each with:
            rel_path, file_size, modified_date, hash_fast

    Returns dict with:
        unchanged: list of rel_paths confirmed on disk matching expected
        changed: list of full file metadata dicts (size/mtime/hash differs)
        gone: list of rel_paths in expected but not on disk
        new: list of full file metadata dicts (on disk but not in expected)
        subdirs: list of subdirectory names found on disk
    """
    rel_dir = os.path.relpath(dirpath, root_path)
    if rel_dir == ".":
        rel_dir = ""

    # Build lookup of expected files by rel_path
    expected_by_rel = {e["rel_path"]: e for e in expected}

    # Iterate disk
    disk_files = {}  # rel_path -> stat result
    subdirs = []

    try:
        entries = os.listdir(dirpath)
    except (PermissionError, OSError):
        # Can't read directory — everything expected is gone
        return {
            "unchanged": [],
            "changed": [],
            "gone": [e["rel_path"] for e in expected],
            "new": [],
            "subdirs": [],
        }

    for name in entries:
        full_path = os.path.join(dirpath, name)

        try:
            is_link = os.path.islink(full_path)
        except OSError:
            continue

        if is_link:
            continue

        try:
            is_dir = os.path.isdir(full_path)
        except OSError:
            continue

        if is_dir:
            subdirs.append(name)
            continue

        # Regular file
        try:
            st = os.stat(full_path)
        except OSError:
            continue

        if not stat.S_ISREG(st.st_mode):
            continue

        # Skip .moved and .sources stubs
        if name.endswith(".moved") or name.endswith(".sources"):
            continue

        rel_path = os.path.join(rel_dir, name) if rel_dir else name
        disk_files[rel_path] = (full_path, name, st)

    # Compare
    unchanged = []
    changed = []
    gone = []
    new_files = []

    # Check expected against disk
    for rel_path, exp in expected_by_rel.items():
        if rel_path not in disk_files:
            gone.append(rel_path)
            continue

        full_path, name, st = disk_files[rel_path]
        mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(
            timespec="seconds"
        )

        # Compare size and mtime first
        if st.st_size == exp.get("file_size") and mtime == exp.get("modified_date"):
            unchanged.append(rel_path)
        else:
            # Changed — build full metadata
            changed.append(_build_file_info(full_path, name, rel_path, rel_dir, st))

    # Find new files (on disk but not expected)
    for rel_path, (full_path, name, st) in disk_files.items():
        if rel_path not in expected_by_rel:
            new_files.append(_build_file_info(full_path, name, rel_path, rel_dir, st))

    return {
        "unchanged": unchanged,
        "changed": changed,
        "gone": gone,
        "new": new_files,
        "subdirs": subdirs,
    }


def _build_file_info(
    full_path: str, name: str, rel_path: str, rel_dir: str, st: os.stat_result
) -> dict:
    """Build file metadata dict with hash_fast computed."""
    hidden = name.startswith(".") or (rel_dir and any(
        part.startswith(".") for part in rel_dir.split(os.sep)
    ))

    type_high, type_low = classify_file(name)

    # Compute hash_partial for non-hidden, non-zero files
    hash_partial = None
    if not hidden and st.st_size > 0:
        try:
            hash_partial = hash_file_partial_sync(full_path)
        except OSError:
            pass

    return {
        "filename": name,
        "full_path": full_path,
        "rel_path": rel_path,
        "rel_dir": rel_dir,
        "file_size": st.st_size,
        "created_date": datetime.fromtimestamp(
            st.st_birthtime if hasattr(st, "st_birthtime") else st.st_ctime,
            tz=timezone.utc,
        ).isoformat(timespec="seconds"),
        "modified_date": datetime.fromtimestamp(
            st.st_mtime, tz=timezone.utc
        ).isoformat(timespec="seconds"),
        "file_type_high": type_high,
        "file_type_low": type_low,
        "hidden": 1 if hidden else 0,
        "hash_partial": hash_partial,
    }
