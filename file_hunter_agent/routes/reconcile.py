"""Directory reconciliation endpoint — compare catalog expectations against disk."""

import asyncio

from starlette.requests import Request

from file_hunter_agent.config import is_path_allowed
from file_hunter_agent.response import json_ok, json_error
from file_hunter_core.reconcile import reconcile_directory


async def reconcile(request: Request):
    """Compare expected catalog contents against disk reality for a single directory.

    POST body:
        path: absolute path to directory
        root_path: location root for rel_path computation
        expected: list of {rel_path, file_size, modified_date, hash_fast}
    """
    body = await request.json()
    path = body.get("path", "")
    root_path = body.get("root_path", "")
    expected = body.get("expected", [])
    cursor = body.get("cursor")  # None if old server, int if paginating

    if not path or not root_path:
        return json_error("path and root_path are required.")
    if not is_path_allowed(path):
        return json_error("Path is not within a configured location.", status=403)

    result = await asyncio.to_thread(
        reconcile_directory, path, root_path, expected, cursor=cursor
    )
    return json_ok(result)
