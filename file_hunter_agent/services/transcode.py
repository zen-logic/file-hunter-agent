"""Video transcode service — H.264/AAC MP4 via ffmpeg subprocess."""

import asyncio
import logging
import os
import shutil
import time

logger = logging.getLogger("file_hunter_agent")

# State
_task: asyncio.Task | None = None
_cancel_flag = False
_send_fn = None


def set_send_fn(fn):
    """Register the WebSocket send function for progress updates."""
    global _send_fn
    _send_fn = fn


def is_available() -> bool:
    """Check whether ffmpeg and ffprobe are on PATH."""
    return shutil.which("ffmpeg") is not None and shutil.which("ffprobe") is not None


def is_transcoding() -> bool:
    return _task is not None and not _task.done()


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
    import json
    try:
        data = json.loads(stdout)
        return float(data["format"]["duration"])
    except (KeyError, ValueError, json.JSONDecodeError):
        return None


async def start_transcode(path: str) -> bool:
    """Start a transcode. Returns False if already running."""
    global _task, _cancel_flag

    if is_transcoding():
        return False

    _cancel_flag = False
    _task = asyncio.create_task(_run_transcode(path))
    return True


async def _run_transcode(path: str):
    """Run ffmpeg to produce <name>-transcode.mp4 alongside the original."""
    global _cancel_flag

    name, _ = os.path.splitext(path)
    output_path = f"{name}-transcode.mp4"
    filename = os.path.basename(output_path)

    await _send({
        "type": "transcode_progress",
        "path": path,
        "output": output_path,
        "percent": 0,
        "status": "probing",
    })

    duration = await _get_duration(path)

    await _send({
        "type": "transcode_progress",
        "path": path,
        "output": output_path,
        "percent": 0,
        "status": "transcoding",
        "duration": duration,
    })

    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y",
        "-i", path,
        "-c:v", "libx264", "-preset", "slow", "-crf", "18",
        "-c:a", "aac", "-b:a", "192k",
        "-movflags", "+faststart",
        "-progress", "pipe:1",
        "-nostats",
        output_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    last_update = 0.0
    try:
        async for line in proc.stdout:
            if _cancel_flag:
                proc.kill()
                await proc.wait()
                # Clean up partial file
                if os.path.exists(output_path):
                    os.unlink(output_path)
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
                            "output": output_path,
                            "percent": round(percent, 1),
                            "status": "transcoding",
                        })
                except ValueError:
                    pass

        await proc.wait()
    except asyncio.CancelledError:
        proc.kill()
        await proc.wait()
        if os.path.exists(output_path):
            os.unlink(output_path)
        raise

    if proc.returncode != 0:
        stderr = await proc.stderr.read()
        error_msg = stderr.decode("utf-8", errors="replace")[-500:]
        logger.error("ffmpeg failed (rc=%d): %s", proc.returncode, error_msg)
        if os.path.exists(output_path):
            os.unlink(output_path)
        await _send({
            "type": "transcode_error",
            "path": path,
            "error": f"ffmpeg exited with code {proc.returncode}",
        })
        return

    # Success — stat the output file
    st = os.stat(output_path)
    await _send({
        "type": "transcode_complete",
        "path": path,
        "output": output_path,
        "filename": filename,
        "size": st.st_size,
        "mtime": st.st_mtime,
        "ctime": st.st_ctime,
        "inode": st.st_ino,
    })
    logger.info("Transcode complete: %s -> %s", path, output_path)
