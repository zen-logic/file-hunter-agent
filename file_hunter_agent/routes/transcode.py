"""Transcode route — start a video transcode via ffmpeg."""

from starlette.requests import Request

from file_hunter_agent.config import is_path_allowed
from file_hunter_agent.response import json_ok, json_error
from file_hunter_agent.services.transcode import is_transcoding, start_transcode, cancel_transcode


async def transcode_start(request: Request):
    """POST /transcode — start transcoding a video file."""
    body = await request.json()
    path = body.get("path", "")
    if not path:
        return json_error("path is required.")
    if not is_path_allowed(path):
        return json_error("Path is not within a configured location.", status=403)
    if is_transcoding():
        return json_error("A transcode is already running.")
    started = await start_transcode(path)
    if not started:
        return json_error("Failed to start transcode.")
    return json_ok({"started": True})


async def transcode_cancel(request: Request):
    """POST /transcode/cancel — cancel the running transcode."""
    cancel_transcode()
    return json_ok({"cancelled": True})
