"""Starlette HTTP app assembly for the agent."""

import asyncio

from starlette.applications import Starlette
from starlette.routing import Route

from file_hunter_agent import config
from file_hunter_agent.auth import AgentAuthMiddleware
from file_hunter_agent.routes.browse import browse, browse_system
from file_hunter_agent.routes.locations import (
    add_location,
    rename_location,
    delete_location,
)
from file_hunter_agent.routes.files import (
    file_content,
    file_delete,
    file_move,
    file_write,
    file_stat,
    file_exists,
    file_hash,
    hash_partial_batch,
    stream_write,
)
from file_hunter_agent.routes.folders import (
    folder_create,
    folder_delete,
    folder_move,
    folder_exists,
)
from file_hunter_agent.routes.upload import upload
from file_hunter_agent.routes.disk_stats import disk_stats
from file_hunter_agent.routes.scan import scan_start, scan_cancel
from file_hunter_agent.routes.reconcile import reconcile
from file_hunter_agent.routes.tree import tree
from file_hunter_agent.routes.status import status


_ws_task = None


async def on_startup():
    global _ws_task
    from file_hunter_agent.client import run_client

    server_url = config.get("server_url")
    if server_url:
        _ws_task = asyncio.create_task(run_client())


async def on_shutdown():
    from file_hunter_agent import client

    client._shutting_down = True
    if _ws_task and not _ws_task.done():
        _ws_task.cancel()
        try:
            await _ws_task
        except asyncio.CancelledError:
            pass


def create_app():
    """Create and return the ASGI app with auth middleware."""
    app = Starlette(
        on_startup=[on_startup],
        on_shutdown=[on_shutdown],
        routes=[
            Route("/browse", browse, methods=["GET"]),
            Route("/browse-system", browse_system, methods=["GET"]),
            Route("/locations/add", add_location, methods=["POST"]),
            Route("/locations/rename", rename_location, methods=["POST"]),
            Route("/locations/delete", delete_location, methods=["POST"]),
            Route("/files/content", file_content, methods=["GET"]),
            Route("/files/delete", file_delete, methods=["POST"]),
            Route("/files/move", file_move, methods=["POST"]),
            Route("/files/write", file_write, methods=["POST"]),
            Route("/files/stat", file_stat, methods=["POST"]),
            Route("/files/exists", file_exists, methods=["POST"]),
            Route("/files/hash", file_hash, methods=["POST"]),
            Route("/files/hash-partial-batch", hash_partial_batch, methods=["POST"]),
            Route("/files/stream-write", stream_write, methods=["POST"]),
            Route("/folders/create", folder_create, methods=["POST"]),
            Route("/folders/delete", folder_delete, methods=["POST"]),
            Route("/folders/move", folder_move, methods=["POST"]),
            Route("/folders/exists", folder_exists, methods=["POST"]),
            Route("/upload", upload, methods=["POST"]),
            Route("/scan", scan_start, methods=["POST"]),
            Route("/scan/cancel", scan_cancel, methods=["POST"]),
            Route("/reconcile", reconcile, methods=["POST"]),
            Route("/tree", tree, methods=["POST"]),
            Route("/disk-stats", disk_stats, methods=["POST"]),
            Route("/status", status, methods=["GET"]),
        ],
    )

    return AgentAuthMiddleware(app, lambda: config.get("token"))
