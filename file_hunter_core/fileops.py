"""Filesystem operations — stubs, sources files, path helpers.

Pure synchronous functions. No database, no async, no server dependencies.
"""

import os


def write_moved_stub(original_path: str, filename: str, destination: str, date: str):
    """Delete original file and write a .moved stub in its place."""
    stub_path = original_path + ".moved"
    os.remove(original_path)
    with open(stub_path, "w") as f:
        f.write("Consolidated by File Hunter\n")
        f.write(f"Original: {filename}\n")
        f.write(f"Moved to: {destination}\n")
        f.write(f"Date: {date}\n")


def write_sources_file(canonical_path: str, all_copies: list[dict], date: str):
    """Write or append to a .sources metadata file next to the canonical file.

    If the file already exists (e.g. from a previous merge), new entries are
    appended to preserve the full audit trail.

    all_copies: list of dicts with 'location_name' and 'rel_path' keys.
    """
    sources_path = canonical_path + ".sources"
    entries = [f"- {c['location_name']}: {c['rel_path']}\n" for c in all_copies]

    if os.path.exists(sources_path):
        with open(sources_path, "a") as f:
            for entry in entries:
                f.write(entry)
    else:
        with open(sources_path, "w") as f:
            f.write("Consolidated by File Hunter\n")
            f.write(f"Date: {date}\n")
            f.write("\nSources:\n")
            for entry in entries:
                f.write(entry)


def write_or_append_sources(
    canonical_path: str, source_location: str, source_rel: str, date: str
):
    """Append a single source entry to the .sources file next to the canonical file.

    Creates the file with header if it doesn't exist, otherwise appends.
    """
    sources_path = canonical_path + ".sources"
    entry = f"- {source_location}: {source_rel}\n"

    if os.path.exists(sources_path):
        with open(sources_path, "a") as f:
            f.write(entry)
    else:
        with open(sources_path, "w") as f:
            f.write("Consolidated by File Hunter\n")
            f.write(f"Date: {date}\n")
            f.write("\nSources:\n")
            f.write(entry)


def unique_dest_path(dest_path: str) -> str:
    """Handle filename collision — append (2), (3), etc."""
    if not os.path.exists(dest_path):
        return dest_path

    base, ext = os.path.splitext(dest_path)
    counter = 2
    while True:
        candidate = f"{base} ({counter}){ext}"
        if not os.path.exists(candidate):
            return candidate
        counter += 1
