"""Directory walker — synchronous scan of a single directory.

Generator yielding subdirectory paths and file metadata one at a time.
No database, no async. The caller handles DB upsert separately.
"""

import os
import stat
from datetime import datetime, timezone

from file_hunter_core.classify import classify_file


def scan_directory(dirpath: str, root_path: str, parent_hidden: bool = False):
    """Scan a single directory. Yields ("dir", path) or ("file", info_dict).

    Subdirectories yield as ("dir", full_path).
    Files yield as ("file", metadata_dict).

    Dotfiles/dotfolders are included with hidden=1. Files inside a hidden
    parent directory inherit hidden status.
    """
    try:
        scandir_it = os.scandir(dirpath)
    except (PermissionError, OSError):
        return

    rel_dir = os.path.relpath(dirpath, root_path).replace(os.sep, "/")
    if rel_dir == ".":
        rel_dir = ""

    for entry in scandir_it:
        try:
            if entry.is_symlink():
                continue
            if entry.is_dir(follow_symlinks=False):
                yield ("dir", entry.path)
                continue
            st = entry.stat(follow_symlinks=False)
        except OSError:
            continue

        if not stat.S_ISREG(st.st_mode):
            continue

        hidden = parent_hidden or entry.name.startswith(".")
        rel_path = f"{rel_dir}/{entry.name}" if rel_dir else entry.name
        type_high, type_low = classify_file(entry.name)

        yield (
            "file",
            {
                "filename": entry.name,
                "full_path": entry.path,
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
            },
        )
