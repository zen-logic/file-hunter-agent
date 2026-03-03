"""Scan orchestrator — BFS walk + hash, result batching via WebSocket.

Uses file_hunter_core for synchronous filesystem operations, wrapped in
asyncio.to_thread() to keep the event loop responsive.
"""

import asyncio
import logging
import time
from collections import deque

from file_hunter_core.walker import scan_directory
from file_hunter_core.hasher import hash_file_partial_sync

logger = logging.getLogger("file_hunter_agent")

# Scan state
_scan_task: asyncio.Task | None = None
_cancel_flag = False
_scanning = False
_current_path = ""

# Message send function — set by client.py after WS connects
_send_fn = None

BATCH_SIZE = 50
PROGRESS_INTERVAL = 0.5  # seconds


def set_send_fn(fn):
    """Register the WebSocket send function for streaming results."""
    global _send_fn
    _send_fn = fn


def is_scanning() -> bool:
    return _scanning


def get_current_path() -> str:
    return _current_path


def cancel_scan():
    global _cancel_flag
    _cancel_flag = True


async def start_scan(path: str, root_path: str | None = None) -> bool:
    """Start a scan. Returns False if already scanning.

    path: directory to walk (scan target — may be a subfolder)
    root_path: location root for rel_path computation (defaults to path)
    """
    global _scan_task, _cancel_flag, _scanning, _current_path

    if _scanning:
        return False

    _cancel_flag = False
    _scanning = True
    _current_path = path
    _scan_task = asyncio.create_task(_run_scan(path, root_path or path))
    return True


async def _send(msg: dict):
    """Send a message via the WebSocket client if connected."""
    if _send_fn:
        logger.info("Scanner sending: %s", msg.get("type", "?"))
        await _send_fn(msg)
    else:
        logger.warning(
            "Scanner: no send function registered, message dropped: %s",
            msg.get("type", "?"),
        )


async def _run_scan(path: str, root_path: str):
    global _scanning, _current_path

    files_found = 0
    files_hashed = 0
    last_progress = 0.0
    file_batch = []
    all_files = []

    try:
        # --- Discovery phase: BFS walk ---
        logger.info("Discovery phase starting for: %s (root: %s)", path, root_path)
        import os as _os

        queue = deque([(path, False)])  # (dirpath, is_hidden)

        while queue:
            if _cancel_flag:
                await _send(
                    {
                        "type": "scan_cancelled",
                        "path": path,
                        "filesFound": files_found,
                        "filesHashed": files_hashed,
                    }
                )
                return

            dirpath, dir_hidden = queue.popleft()
            logger.info("Scanning directory: %s", dirpath)
            subdirs, file_infos = await asyncio.to_thread(
                scan_directory, dirpath, root_path, dir_hidden
            )
            logger.info("  Found %d files, %d subdirs", len(file_infos), len(subdirs))
            for subdir in subdirs:
                sub_hidden = dir_hidden or _os.path.basename(subdir).startswith(".")
                queue.append((subdir, sub_hidden))

            for fi in file_infos:
                # Skip .moved and .sources stubs
                name = fi["filename"]
                if name.endswith(".moved") or name.endswith(".sources"):
                    continue

                files_found += 1
                all_files.append(fi)

            # Throttled progress broadcast
            now = time.monotonic()
            if now - last_progress >= PROGRESS_INTERVAL:
                last_progress = now
                await _send(
                    {
                        "type": "scan_progress",
                        "path": path,
                        "phase": "discovery",
                        "filesFound": files_found,
                        "filesHashed": files_hashed,
                    }
                )

        # --- Hashing phase ---
        # Partial hash every file (fast, fixed-size read).
        # Full hash only when size duplicates exist within the same
        # BATCH_SIZE send-batch — keeps the first scan fast.
        # Cross-location dups are resolved by the server-side backfill.
        logger.info("Hashing phase starting: %d files to process", len(all_files))

        for fi in all_files:
            if _cancel_flag:
                await _send(
                    {
                        "type": "scan_cancelled",
                        "path": path,
                        "filesFound": files_found,
                        "filesHashed": files_hashed,
                    }
                )
                return

            fp = fi["full_path"]

            # Gate 0: hidden files — never hash
            if fi.get("hidden"):
                fi["hash_partial"] = None
                fi["hash_fast"] = None
                fi["hash_strong"] = None
            else:
                try:
                    logger.info("Hashing: %s (%d bytes)", fp, fi["file_size"])
                    partial = await asyncio.to_thread(hash_file_partial_sync, fp)
                    fi["hash_partial"] = partial
                    fi["hash_fast"] = None
                    fi["hash_strong"] = None
                except OSError as e:
                    logger.warning("Hash failed: %s: %s", fp, e)
                    fi["hash_partial"] = None
                    fi["hash_fast"] = None
                    fi["hash_strong"] = None

            files_hashed += 1
            file_batch.append(fi)

            if len(file_batch) >= BATCH_SIZE:
                await _send({"type": "scan_files", "path": path, "files": file_batch})
                file_batch = []

            # Throttled progress
            now = time.monotonic()
            if now - last_progress >= PROGRESS_INTERVAL:
                last_progress = now
                await _send(
                    {
                        "type": "scan_progress",
                        "path": path,
                        "phase": "hashing",
                        "filesFound": files_found,
                        "filesHashed": files_hashed,
                    }
                )

        # Flush remaining batch
        if file_batch:
            await _send({"type": "scan_files", "path": path, "files": file_batch})

        await _send(
            {
                "type": "scan_completed",
                "path": path,
                "filesFound": files_found,
                "filesHashed": files_hashed,
            }
        )

    except Exception as e:
        logger.error("Scan failed with exception: %s", e, exc_info=True)
        await _send(
            {
                "type": "scan_error",
                "path": path,
                "error": str(e),
            }
        )

    finally:
        logger.info(
            "Scan finished for: %s (found=%d, hashed=%d)",
            path,
            files_found,
            files_hashed,
        )
        _scanning = False
        _current_path = ""
