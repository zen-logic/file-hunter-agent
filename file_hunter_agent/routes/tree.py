"""Tree walk endpoint — stream metadata + partial hashes as TSV."""

import asyncio
import logging

from starlette.concurrency import iterate_in_threadpool
from starlette.requests import Request
from starlette.responses import StreamingResponse

from file_hunter_agent.config import is_path_allowed
from file_hunter_agent.response import json_error
from file_hunter_core.tree import walk_tree

logger = logging.getLogger("file_hunter_agent")


async def _safe_tree_stream(path, prefix, metadata_only=False):
    """Wrap tree walk so shutdown cancellation doesn't produce tracebacks."""
    try:
        async for chunk in iterate_in_threadpool(
            walk_tree(path, prefix, metadata_only=metadata_only)
        ):
            yield chunk
    except asyncio.CancelledError:
        logger.info("Tree walk cancelled (shutdown): %s", path)


async def tree(request: Request):
    """Stream metadata for every file under a location root.

    POST body:
        path: absolute path to location root
        prefix: optional relative subdirectory to scope the walk
        metadata_only: if true, skip hash phase (for rescan diff)

    Each yielded chunk is one complete directory: D record followed by
    all F records (inode-sorted with partial hashes).
    """
    body = await request.json()
    path = body.get("path", "")
    prefix = body.get("prefix") or None
    metadata_only = bool(body.get("metadata_only", False))

    if not path:
        return json_error("path is required.")
    if not is_path_allowed(path):
        return json_error("Path is not within a configured location.", status=403)
    if prefix:
        import os

        full = os.path.join(path, prefix)
        if not is_path_allowed(full):
            return json_error(
                "Prefix path is not within a configured location.", status=403
            )

    return StreamingResponse(
        _safe_tree_stream(path, prefix, metadata_only=metadata_only),
        media_type="text/tab-separated-values",
    )
