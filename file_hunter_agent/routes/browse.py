"""Filesystem browsing endpoint."""

import asyncio
import os

from starlette.requests import Request

from file_hunter_agent.config import get_locations, is_path_allowed
from file_hunter_agent.response import json_ok, json_error
from file_hunter_core.browse import get_children, get_root_entries


async def browse(request: Request):
    path = request.query_params.get("path", "")
    if path:
        if not is_path_allowed(path):
            return json_error("Path is not within a configured location.", status=403)
        entries = await asyncio.to_thread(get_children, path)
        return json_ok(entries)

    # No path — return configured location roots
    locations = get_locations()
    entries = []
    for loc in locations:
        has_children = await asyncio.to_thread(_has_children, loc["path"])
        entries.append(
            {
                "name": loc["name"],
                "path": loc["path"],
                "hasChildren": has_children,
            }
        )
    return json_ok(entries)


async def browse_system(request: Request):
    """Browse the full filesystem — no location restrictions."""
    path = request.query_params.get("path", "")
    if path:
        entries = await asyncio.to_thread(get_children, path)
        return json_ok({"path": path, "entries": entries})

    # No path — return system root entries (volumes/mounts)
    entries = await asyncio.to_thread(get_root_entries)
    return json_ok({"path": None, "entries": entries})


def _has_children(path: str) -> bool:
    try:
        for name in os.listdir(path):
            if name.startswith("."):
                continue
            child = os.path.join(path, name)
            if os.path.isdir(child) and not os.path.islink(child):
                return True
    except (PermissionError, OSError):
        pass
    return False
