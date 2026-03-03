"""Disk stats endpoint — returns capacity info for a location path."""

import os
import shutil

from starlette.requests import Request

from file_hunter_agent.config import is_path_allowed
from file_hunter_agent.response import json_ok, json_error


async def disk_stats(request: Request):
    """Return disk stats for a location path: total, free, readonly, mount."""
    try:
        body = await request.json()
    except Exception:
        return json_error("Invalid JSON body.")

    path = body.get("path", "").strip()
    if not path:
        return json_error("Missing path.")

    if not is_path_allowed(path):
        return json_error("Path is not within a configured location.", status=403)

    if not os.path.ismount(path):
        return json_ok({"mount": False})

    try:
        usage = shutil.disk_usage(path)
    except OSError as e:
        return json_error(f"Cannot read disk usage: {e}")

    readonly = False
    try:
        st = os.statvfs(path)
        readonly = bool(st.f_flag & os.ST_RDONLY)
    except (OSError, AttributeError):
        pass

    return json_ok(
        {
            "mount": True,
            "total": usage.total,
            "free": usage.free,
            "readonly": readonly,
        }
    )
