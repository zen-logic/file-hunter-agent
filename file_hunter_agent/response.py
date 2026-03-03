"""JSON response helpers."""

from starlette.responses import JSONResponse


def json_ok(data, status=200) -> JSONResponse:
    return JSONResponse({"ok": True, "data": data}, status_code=status)


def json_error(message: str, status=400) -> JSONResponse:
    return JSONResponse({"ok": False, "error": message}, status_code=status)
