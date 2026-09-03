"""Optional HTTP Basic Auth gate, plus same-origin enforcement for writes.

Babel has no user system; every page (including Settings, which can reach the
Sonarr API key, the Plex token and the Discord webhook URL) is otherwise open
to anyone who can reach the port. Setting a username and password — either via
``AUTH_USERNAME``/``AUTH_PASSWORD`` in the environment or on the Settings page —
enables a Basic Auth challenge for the dashboard.

The Sonarr webhook and the Docker healthcheck are exempt: neither can complete
an interactive challenge, and both have their own guards (a shared secret and
no sensitive data respectively).

Separately, every state-changing request is required to be same-origin.
Without that, any page in another browser tab could POST to Babel and rewrite
its settings, because the dashboard has no per-request token and, by default,
no authentication either.
"""

import base64
import hashlib
import hmac
import logging
import os
import secrets

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from src.config import get_settings

logger = logging.getLogger(__name__)

EXEMPT_PATHS = {"/api/health", "/api/webhook/sonarr", "/favicon.ico"}
EXEMPT_PREFIXES = ("/static/",)

# Same-origin is not required for these: the webhook is called by Sonarr, which
# sends no Origin header and authenticates with its own shared secret.
CSRF_EXEMPT_PATHS = {"/api/webhook/sonarr"}
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

_PBKDF2_ROUNDS = 240_000


def hash_password(password: str) -> str:
    """Hash a password for storage in the settings table."""
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt, _PBKDF2_ROUNDS
    )
    return f"pbkdf2_sha256${_PBKDF2_ROUNDS}${salt.hex()}${digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    """Check *password* against a hash produced by hash_password()."""
    try:
        algorithm, rounds, salt_hex, digest_hex = stored.split("$")
        if algorithm != "pbkdf2_sha256":
            return False
        expected = bytes.fromhex(digest_hex)
        actual = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), bytes.fromhex(salt_hex), int(rounds)
        )
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(actual, expected)


def _constant_time_equals(a: str, b: str) -> bool:
    """Compare two strings without leaking their length through timing.

    Encoded first: hmac.compare_digest raises TypeError on a str containing
    non-ASCII, which turned a wrong username into a 500 that skipped the auth
    denial entirely.
    """
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def _is_exempt(path: str) -> bool:
    return path in EXEMPT_PATHS or path.startswith(EXEMPT_PREFIXES)


async def _credentials() -> tuple[str, str, str]:
    """Return (username, plaintext password, password hash) in force.

    Read through get_effective_settings() so a username and password set on
    the Settings page take effect without a restart — previously auth could
    only be turned on by editing the container's environment, which is why it
    tended to stay off. That call is cached for a few seconds, so this does
    not add a database round trip per request.
    """
    try:
        from src.config import get_effective_settings

        cfg = await get_effective_settings()
    except Exception:
        logger.warning("Could not read auth settings; falling back to env", exc_info=True)
        settings = get_settings()
        return settings.AUTH_USERNAME, settings.AUTH_PASSWORD, settings.AUTH_PASSWORD_HASH

    return (
        cfg.get("AUTH_USERNAME", "") or "",
        cfg.get("AUTH_PASSWORD", "") or "",
        cfg.get("AUTH_PASSWORD_HASH", "") or "",
    )


class BasicAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        username, password, password_hash = await _credentials()
        if not username or not (password or password_hash):
            return await call_next(request)

        if _is_exempt(request.url.path):
            return await call_next(request)

        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
                given_user, _, given_pass = decoded.partition(":")
            except Exception:
                given_user, given_pass = "", ""

            user_ok = _constant_time_equals(given_user, username)
            if password:
                pass_ok = _constant_time_equals(given_pass, password)
            else:
                pass_ok = verify_password(given_pass, password_hash)
            if user_ok and pass_ok:
                return await call_next(request)

        return Response(
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="Babel"'},
            content="Authentication required.",
        )


class SameOriginMiddleware(BaseHTTPMiddleware):
    """Reject cross-origin state changes.

    Babel's forms carry no CSRF token, so without this any page the user has
    open elsewhere can POST to the dashboard — repointing Sonarr, starting
    scans, or rewriting ignore patterns — as long as it can reach the port.

    Set ``ALLOW_CROSS_ORIGIN_WRITES=true`` to disable, for the unusual setup
    that drives Babel's endpoints from another origin on purpose.
    """

    def __init__(self, app):
        super().__init__(app)
        self.enabled = os.environ.get(
            "ALLOW_CROSS_ORIGIN_WRITES", ""
        ).strip().lower() not in ("1", "true", "yes")

    async def dispatch(self, request: Request, call_next):
        if not self.enabled or request.method in SAFE_METHODS:
            return await call_next(request)
        if request.url.path in CSRF_EXEMPT_PATHS:
            return await call_next(request)

        # Modern browsers send Sec-Fetch-Site on every request; it is the
        # cleanest signal. Origin is the fallback, and a request with neither
        # is not coming from a browser form post at all.
        site = request.headers.get("sec-fetch-site")
        if site is not None:
            if site in ("same-origin", "none"):
                return await call_next(request)
            logger.warning(
                "Blocked cross-site %s %s (Sec-Fetch-Site: %s)",
                request.method, request.url.path, site,
            )
            return JSONResponse({"error": "Cross-origin request blocked"}, status_code=403)

        origin = request.headers.get("origin")
        if origin:
            expected = f"{request.url.scheme}://{request.headers.get('host', '')}"
            if not _constant_time_equals(origin, expected):
                logger.warning(
                    "Blocked cross-origin %s %s (Origin: %s)",
                    request.method, request.url.path, origin,
                )
                return JSONResponse(
                    {"error": "Cross-origin request blocked"}, status_code=403
                )

        return await call_next(request)
