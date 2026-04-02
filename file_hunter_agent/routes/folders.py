"""Folder operations — create, delete, move, exists."""

import asyncio
import logging
import os
import shutil

from starlette.requests import Request

from file_hunter_agent.config import is_path_allowed
from file_hunter_agent.response import json_ok, json_error

logger = logging.getLogger(__name__)

_FORBIDDEN = "Path is not within a configured location."


async def folder_move(request: Request):
    """Move/rename a directory."""
    body = await request.json()
    src = body.get("path", "")
    dest = body.get("destination", "")
    if not src or not dest:
        return json_error("path and destination are required.")
    if not is_path_allowed(src):
        return json_error(_FORBIDDEN, status=403)
    if not is_path_allowed(dest):
        return json_error(_FORBIDDEN, status=403)

    exists = await asyncio.to_thread(os.path.isdir, src)
    if not exists:
        return json_error("Source directory not found.", status=404)

    await asyncio.to_thread(shutil.move, src, dest)
    logger.info("Folder move: %s → %s", os.path.basename(src), os.path.basename(dest))
    return json_ok({"moved": src, "destination": dest})


async def folder_exists(request: Request):
    """Check if a directory exists."""
    body = await request.json()
    path = body.get("path", "")
    if not path:
        return json_error("path is required.")
    if not is_path_allowed(path):
        return json_error(_FORBIDDEN, status=403)

    exists = await asyncio.to_thread(os.path.isdir, path)
    return json_ok({"exists": exists})


async def folder_create(request: Request):
    """Create a directory (and parents)."""
    body = await request.json()
    path = body.get("path", "")
    if not path:
        return json_error("path is required.")
    if not is_path_allowed(path):
        return json_error(_FORBIDDEN, status=403)

    await asyncio.to_thread(os.makedirs, path, exist_ok=True)
    logger.info("Folder create: %s", os.path.basename(path))
    return json_ok({"created": path})


async def folder_delete(request: Request):
    """Recursively delete a directory."""
    body = await request.json()
    path = body.get("path", "")
    if not path:
        return json_error("path is required.")
    if not is_path_allowed(path):
        return json_error(_FORBIDDEN, status=403)

    exists = await asyncio.to_thread(os.path.isdir, path)
    if not exists:
        return json_error("Directory not found.", status=404)

    await asyncio.to_thread(shutil.rmtree, path)
    logger.info("Folder delete: %s", os.path.basename(path))
    return json_ok({"deleted": path})
