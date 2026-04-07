"""File upload endpoint."""

import asyncio
import os

from starlette.requests import Request

from file_hunter_agent.config import is_path_allowed
from file_hunter_agent.response import json_ok, json_error


async def upload(request: Request):
    """Accept multipart file upload. Writes files to dest_dir."""
    form = await request.form()
    dest_dir = form.get("dest_dir", "")
    if not dest_dir:
        return json_error("dest_dir is required.")
    if not is_path_allowed(dest_dir):
        return json_error("Path is not within a configured location.", status=403)

    exists = await asyncio.to_thread(os.path.isdir, dest_dir)
    if not exists:
        return json_error("Destination directory not found.", status=404)

    mtime = form.get("mtime")

    saved = []
    for key in form:
        if key in ("dest_dir", "mtime"):
            continue
        upload_file = form[key]
        if not hasattr(upload_file, "read"):
            continue

        filename = upload_file.filename
        dest_path = os.path.join(dest_dir, filename)
        upload_file.file.seek(0)
        size = await asyncio.to_thread(_stream_to_file, upload_file.file, dest_path)

        # Restore original modified time if provided
        if mtime:
            t = float(mtime)
            await asyncio.to_thread(os.utime, dest_path, (t, t))

        saved.append({"filename": filename, "path": dest_path, "size": size})

    return json_ok({"files": saved})


def _stream_to_file(src_file, dest_path: str) -> int:
    """Stream from file-like to disk, return bytes written."""
    total = 0
    with open(dest_path, "wb") as f:
        while True:
            chunk = src_file.read(1024 * 1024)  # 1MB
            if not chunk:
                break
            f.write(chunk)
            total += len(chunk)
    return total
