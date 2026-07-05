"""Optional HTTP Basic Auth gate.

Babel has no built-in user system; every page (including Settings, which
renders the Sonarr API key, Plex token, and Discord webhook URL) is otherwise
open to anyone who can reach the port. Setting AUTH_USERNAME + AUTH_PASSWORD
enables a Basic Auth challenge for the dashboard. The Sonarr webhook and the
Docker healthcheck are exempted since they can't complete an interactive
auth challenge and have their own guards (webhook secret / no sensitive data).
"""

import hmac

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from src.config import get_settings

EXEMPT_PATHS = {"/api/health", "/api/webhook/sonarr", "/favicon.ico"}
EXEMPT_PREFIXES = ("/static/",)


def _is_exempt(path: str) -> bool:
    return path in EXEMPT_PATHS or path.startswith(EXEMPT_PREFIXES)


class BasicAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        settings = get_settings()
        if not settings.AUTH_USERNAME or not settings.AUTH_PASSWORD:
            return await call_next(request)

        if _is_exempt(request.url.path):
            return await call_next(request)

        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Basic "):
            import base64

            try:
                decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
                username, _, password = decoded.partition(":")
            except Exception:
                username, password = "", ""

            user_ok = hmac.compare_digest(username, settings.AUTH_USERNAME)
            pass_ok = hmac.compare_digest(password, settings.AUTH_PASSWORD)
            if user_ok and pass_ok:
                return await call_next(request)

        return Response(
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="Babel"'},
            content="Authentication required.",
        )
