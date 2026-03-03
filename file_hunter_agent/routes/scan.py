"""Scan control endpoints — start and cancel."""

import asyncio
import os

from starlette.requests import Request

from file_hunter_agent.config import is_path_allowed
from file_hunter_agent.response import json_ok, json_error
from file_hunter_agent.services.scanner import start_scan, cancel_scan, is_scanning


async def scan_start(request: Request):
    """Start a filesystem scan at the given path."""
    body = await request.json()
    path = body.get("path", "")
    if not path:
        return json_error("path is required.")
    if not is_path_allowed(path):
        return json_error("Path is not within a configured location.", status=403)

    exists = await asyncio.to_thread(os.path.isdir, path)
    if not exists:
        return json_error("Path not found.", status=404)

    started = await start_scan(path)
    if not started:
        return json_error("A scan is already running.")

    return json_ok({"scanning": path})


async def scan_cancel(request: Request):
    """Cancel the running scan."""
    if not is_scanning():
        return json_error("No scan is running.")

    cancel_scan()
    return json_ok({"cancelled": True})
