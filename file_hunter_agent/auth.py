"""HTTP bearer token middleware for the agent's local HTTP server.

Simple shared-secret check — the token is configured at startup and
the central server uses it when making HTTP calls to this agent.
"""

import hmac

from starlette.responses import JSONResponse


class AgentAuthMiddleware:
    def __init__(self, app, get_token_fn):
        self.app = app
        self.get_token_fn = get_token_fn

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        token = self.get_token_fn()
        if not token:
            # No token configured — pass through (shouldn't happen in practice)
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        auth = headers.get(b"authorization", b"").decode()
        provided = auth.replace("Bearer ", "") if auth.startswith("Bearer ") else ""

        if not provided or not hmac.compare_digest(provided, token):
            response = JSONResponse(
                {"ok": False, "error": "Authentication required."},
                status_code=401,
            )
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)
