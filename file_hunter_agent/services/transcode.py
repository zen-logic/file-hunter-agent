"""Video transcode service — H.264/AAC MP4 via ffmpeg subprocess."""

import asyncio
import json
import logging
import os
import shutil
import tempfile
import time

logger = logging.getLogger("file_hunter_agent")

# State
_task: asyncio.Task | None = None
_cancel_flag = False
_send_fn = None
_current_path: str | None = None


def set_send_fn(fn):
    """Register the WebSocket send function for progress updates."""
    global _send_fn
    _send_fn = fn


def is_available() -> bool:
    """Check whether ffmpeg and ffprobe are on PATH."""
    return shutil.which("ffmpeg") is not None and shutil.which("ffprobe") is not None


def is_transcoding() -> bool:
    return _task is not None and not _task.done()


def get_status() -> dict | None:
    """Return current transcode status, or None if idle."""
    if not is_transcoding():
        return None
    return {"path": _current_path}


def cancel_transcode():
    global _cancel_flag
    _cancel_flag = True


async def _send(msg: dict):
    if _send_fn:
        await _send_fn(msg)


async def _get_duration(path: str) -> float | None:
    """Get video duration in seconds via ffprobe."""
    proc = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await proc.communicate()
    if proc.returncode != 0:
        return None
    try:
        data = json.loads(stdout)
        return float(data["format"]["duration"])
    except (KeyError, ValueError, json.JSONDecodeError):
        return None


async def start_transcode(path: str) -> bool:
    """Start a transcode. Returns False if already running."""
    global _task, _cancel_flag, _current_path

    if is_transcoding():
        return False

    _cancel_flag = False
    _current_path = path
    _task = asyncio.create_task(_run_transcode(path))
    return True


def _output_path(path: str) -> str:
    """Build the final output path, avoiding collisions with existing files."""
    name, _ = os.path.splitext(path)
    candidate = f"{name}-transcode.mp4"
    if not os.path.exists(candidate):
        return candidate
    n = 2
    while True:
        candidate = f"{name}-transcode-{n}.mp4"
        if not os.path.exists(candidate):
            return candidate
        n += 1


async def _drain_stderr(proc):
    """Read stderr to prevent pipe buffer deadlock. Returns collected output."""
    chunks = []
    try:
        async for line in proc.stderr:
            chunks.append(line)
    except asyncio.CancelledError:
        pass
    return b"".join(chunks)


async def _run_transcode(path: str):
    """Run ffmpeg, writing to a temp dir, then move to the final location.

    The output is written outside the source folder so in-progress files
    are invisible to scans. Only the final move puts the file where it
    belongs, and that's atomic on the same filesystem.
    """
    global _cancel_flag, _current_path

    tmp_dir = None
    try:
        final_path = _output_path(path)
        filename = os.path.basename(final_path)

        # Check disk space on the destination filesystem
        source_size = os.path.getsize(path)
        stat_fs = os.statvfs(os.path.dirname(path))
        free_bytes = stat_fs.f_bavail * stat_fs.f_frsize
        if free_bytes < source_size:
            await _send({
                "type": "transcode_error",
                "path": path,
                "error": "Insufficient disk space for transcode.",
            })
            return

        # Create temp dir for the encode — outside configured locations
        # so scans never see the partial file
        tmp_dir = tempfile.mkdtemp(prefix="fh-transcode-")
        tmp_output = os.path.join(tmp_dir, filename)

        await _send({
            "type": "transcode_progress",
            "path": path,
            "output": final_path,
            "percent": 0,
            "status": "probing",
        })

        duration = await _get_duration(path)

        await _send({
            "type": "transcode_progress",
            "path": path,
            "output": final_path,
            "percent": 0,
            "status": "transcoding",
            "duration": duration,
        })

        proc = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-y",
            "-i", path,
            "-c:v", "libx264", "-preset", "medium", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-movflags", "+faststart",
            "-progress", "pipe:1",
            "-nostats",
            tmp_output,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # Drain stderr concurrently to prevent pipe buffer deadlock
        stderr_task = asyncio.create_task(_drain_stderr(proc))

        last_update = 0.0
        try:
            async for line in proc.stdout:
                if _cancel_flag:
                    proc.kill()
                    await proc.wait()
                    stderr_task.cancel()
                    await _send({
                        "type": "transcode_cancelled",
                        "path": path,
                    })
                    return

                text = line.decode("utf-8", errors="replace").strip()
                if text.startswith("out_time_us=") and duration:
                    try:
                        us = int(text.split("=", 1)[1])
                        percent = min(100.0, (us / 1_000_000) / duration * 100)
                        now = time.monotonic()
                        if now - last_update >= 0.5:
                            last_update = now
                            await _send({
                                "type": "transcode_progress",
                                "path": path,
                                "output": final_path,
                                "percent": round(percent, 1),
                                "status": "transcoding",
                            })
                    except ValueError:
                        pass

            await proc.wait()
            stderr_output = await stderr_task
        except asyncio.CancelledError:
            proc.kill()
            await proc.wait()
            stderr_task.cancel()
            raise

        if proc.returncode != 0:
            error_msg = stderr_output.decode("utf-8", errors="replace")[-500:]
            logger.error("ffmpeg failed (rc=%d): %s", proc.returncode, error_msg)
            await _send({
                "type": "transcode_error",
                "path": path,
                "error": f"ffmpeg exited with code {proc.returncode}",
            })
            return

        # Move completed file to its final location
        await asyncio.to_thread(shutil.move, tmp_output, final_path)

        # Success — stat the file at its final location
        st = os.stat(final_path)
        await _send({
            "type": "transcode_complete",
            "path": path,
            "output": final_path,
            "filename": filename,
            "size": st.st_size,
            "mtime": st.st_mtime,
            "ctime": st.st_ctime,
            "inode": st.st_ino,
        })
        logger.info("Transcode complete: %s -> %s", path, final_path)

    except Exception as exc:
        logger.exception("Transcode failed: %s", exc)
        await _send({
            "type": "transcode_error",
            "path": path,
            "error": str(exc),
        })
    finally:
        _current_path = None
        # Clean up temp dir (may contain partial output on error/cancel)
        if tmp_dir and os.path.isdir(tmp_dir):
            shutil.rmtree(tmp_dir, ignore_errors=True)
