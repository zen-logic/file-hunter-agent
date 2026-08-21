"""Agent status endpoint."""

from file_hunter_agent.response import json_ok
from file_hunter_agent.routes.files import get_hash_status
from file_hunter_agent.services.scanner import is_scanning, get_current_path
from file_hunter_agent.services.transcode import is_transcoding, get_status as get_transcode_status


async def status(request):
    """Return the agent's current activity status."""
    result = {"status": "idle"}

    if is_scanning():
        result = {"status": "scanning", "path": get_current_path()}

    hs = get_hash_status()
    if hs is not None:
        result = {"status": "hashing", "count": hs["count"]}

    result["transcoding"] = is_transcoding()
    ts = get_transcode_status()
    if ts:
        result["transcodePath"] = ts["path"]

    return json_ok(result)
