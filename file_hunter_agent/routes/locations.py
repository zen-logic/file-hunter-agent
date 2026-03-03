"""Location management endpoints."""

import os

from starlette.requests import Request

from file_hunter_agent import config
from file_hunter_agent.client import send_message
from file_hunter_agent.response import json_ok, json_error


async def add_location(request: Request):
    """Add a new location to the agent's config."""
    body = await request.json()
    name = (body.get("name") or "").strip()
    path = (body.get("path") or "").strip()

    if not name:
        return json_error("Location name is required.")
    if not path:
        return json_error("Location path is required.")
    if not os.path.isdir(path):
        return json_error("Path does not exist or is not a directory.")

    # Check for duplicates
    existing = config.get_locations()
    for loc in existing:
        if os.path.realpath(loc["path"]) == os.path.realpath(path):
            return json_error(f"Path is already configured as '{loc['name']}'.")

    # Add to config and persist
    locations = list(existing)
    locations.append({"name": name, "path": path})
    config.save_config({"locations": locations})
    config.load_config()

    # Notify server via WebSocket
    await send_message(
        {
            "type": "locations_updated",
            "locations": config.get_locations_with_status(),
        }
    )

    return json_ok({"name": name, "path": path})


async def rename_location(request: Request):
    """Rename an existing location in the agent's config."""
    body = await request.json()
    path = (body.get("path") or "").strip()
    name = (body.get("name") or "").strip()

    if not path:
        return json_error("Location path is required.")
    if not name:
        return json_error("New name is required.")

    locations = list(config.get_locations())
    for loc in locations:
        if loc["path"] == path:
            loc["name"] = name
            config.save_config({"locations": locations})
            config.load_config()
            await send_message(
                {
                    "type": "locations_updated",
                    "locations": config.get_locations_with_status(),
                }
            )
            return json_ok({"name": name, "path": path})

    return json_error("Location not found.")


async def delete_location(request: Request):
    """Remove a location from the agent's config."""
    body = await request.json()
    path = (body.get("path") or "").strip()
    if not path:
        return json_error("Location path is required.")

    locations = list(config.get_locations())
    original_len = len(locations)
    locations = [loc for loc in locations if loc["path"] != path]

    if len(locations) == original_len:
        return json_error("Location not found.")
    config.save_config({"locations": locations})
    config.load_config()
    await send_message(
        {
            "type": "locations_updated",
            "locations": config.get_locations_with_status(),
        }
    )
    return json_ok({"deleted": path})
