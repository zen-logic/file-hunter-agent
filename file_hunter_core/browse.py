"""Filesystem browsing — volumes, home directory, subdirectories.

Pure filesystem operations, no server dependencies.
"""

import os
import platform


def get_root_entries():
    """Return mount points / volumes as root-level entries."""
    system = platform.system()

    if system == "Darwin":
        return _macos_volumes()
    elif system == "Linux":
        return _linux_mounts()
    else:
        return [_make_entry("/")]


def get_children(path):
    """Return immediate subdirectories of the given path, sorted, skipping hidden."""
    entries = []
    try:
        for name in sorted(os.listdir(path)):
            if name.startswith("."):
                continue
            full = os.path.join(path, name)
            if os.path.isdir(full) and not os.path.islink(full):
                entries.append(_make_entry(full, name))
    except PermissionError:
        pass
    return entries


def _make_entry(path, name=None):
    if name is None:
        name = os.path.basename(path) or path
    return {
        "name": name,
        "path": path,
        "hasChildren": _has_subdirs(path),
    }


def _has_subdirs(path):
    try:
        for name in os.listdir(path):
            if name.startswith("."):
                continue
            child = os.path.join(path, name)
            if os.path.isdir(child) and not os.path.islink(child):
                return True
    except (PermissionError, OSError):
        pass
    return False


def _macos_volumes():
    """List entries under /Volumes/ plus the user's home directory."""
    entries = []

    # User's home folder (the boot volume isn't in /Volumes — it's a symlink)
    home = os.path.expanduser("~")
    if os.path.isdir(home):
        entries.append(_make_entry(home, f"Home ({os.path.basename(home)})"))

    # External / mounted volumes
    volumes_path = "/Volumes"
    if os.path.isdir(volumes_path):
        for name in sorted(os.listdir(volumes_path)):
            full = os.path.join(volumes_path, name)
            if os.path.isdir(full) and not os.path.islink(full):
                entries.append(_make_entry(full, name))

    if not entries:
        entries.append(_make_entry("/"))
    return entries


def _linux_mounts():
    """Parse /proc/mounts for real device mounts, plus /mnt/* and /media/* children."""
    entries = []
    seen = set()

    # User's home folder
    home = os.path.expanduser("~")
    if os.path.isdir(home):
        entries.append(_make_entry(home, f"Home ({os.path.basename(home)})"))
        seen.add(home)

    # System paths to exclude
    skip = {"/", "/boot", "/boot/efi", "/snap", "/proc", "/sys", "/dev", "/run", "/tmp"}

    if os.path.exists("/proc/mounts"):
        try:
            with open("/proc/mounts") as f:
                for line in f:
                    parts = line.split()
                    if len(parts) < 2:
                        continue
                    dev, mount = parts[0], parts[1]
                    if not dev.startswith("/dev/"):
                        continue
                    if mount in skip or mount.startswith("/snap/"):
                        continue
                    if mount not in seen and os.path.isdir(mount):
                        seen.add(mount)
                        entries.append(_make_entry(mount))
        except (PermissionError, OSError):
            pass

    # Also include children of /mnt and /media
    for parent in ("/mnt", "/media"):
        if os.path.isdir(parent):
            try:
                for name in sorted(os.listdir(parent)):
                    full = os.path.join(parent, name)
                    if os.path.isdir(full) and full not in seen:
                        seen.add(full)
                        entries.append(_make_entry(full, name))
            except (PermissionError, OSError):
                pass

    if not entries:
        entries.append(_make_entry("/"))
    return entries
