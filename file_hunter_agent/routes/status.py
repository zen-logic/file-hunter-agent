"""Agent status endpoint."""

from file_hunter_agent.response import json_ok
from file_hunter_agent.routes.files import get_hash_status
from file_hunter_agent.services.scanner import is_scanning, get_current_path


async def status(request):
    """Return the agent's current activity status."""
    if is_scanning():
        return json_ok({"status": "scanning", "path": get_current_path()})

    hs = get_hash_status()
    if hs is not None:
        return json_ok({"status": "hashing", "count": hs["count"]})

    return json_ok({"status": "idle"})
