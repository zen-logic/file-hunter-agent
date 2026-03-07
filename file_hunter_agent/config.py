"""Persistent agent config at ./config.json in the working directory.

Config format:
{
  "server_url": "http://fileserver:8000",
  "token": "...",
  "http_host": "0.0.0.0",
  "http_port": 8001,
  "locations": [
    {"name": "Photos", "path": "/mnt/photos"},
    {"name": "Music", "path": "/mnt/music"}
  ]
}
"""

import json
import os
from pathlib import Path

_CONFIG_FILE = Path("config.json")
_config: dict = {}


def load_config(path: str | None = None) -> dict:
    """Load config from disk. Returns empty dict if file doesn't exist."""
    global _config, _CONFIG_FILE
    if path:
        _CONFIG_FILE = Path(path)
    if _CONFIG_FILE.exists():
        _config = json.loads(_CONFIG_FILE.read_text())
    else:
        _config = {}
    return _config


def save_config(data: dict):
    """Merge data into config and write to disk."""
    global _config
    _config.update(data)
    _CONFIG_FILE.write_text(json.dumps(_config, indent=2) + "\n")


def get(key: str, default=None):
    """Get a config value."""
    return _config.get(key, default)


def get_locations() -> list[dict]:
    """Return configured locations: [{"name": ..., "path": ...}, ...]."""
    return _config.get("locations", [])


def get_locations_with_status() -> list[dict]:
    """Return locations with 'online' field from os.path.isdir()."""
    return [
        {"name": loc["name"], "path": loc["path"], "online": os.path.isdir(loc["path"])}
        for loc in get_locations()
    ]


def is_path_allowed(path: str) -> bool:
    """Check if a path falls within any configured location root."""
    if not path:
        return False
    try:
        resolved = os.path.realpath(path)
    except (OSError, ValueError):
        return False
    for loc in get_locations():
        root = os.path.realpath(loc["path"])
        if resolved == root or resolved.startswith(root + os.sep):
            return True
    return False
