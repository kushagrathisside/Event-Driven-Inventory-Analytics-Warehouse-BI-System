from __future__ import annotations

import hmac

from flask import Flask, jsonify, request

from medwarehouse.logging import get_logger


logger = get_logger(__name__)

_SKIP_AUTH_PATHS = {"/health"}
_SKIP_AUTH_PREFIXES = ("/static/",)


def _extract_key(req) -> str | None:
    key = req.headers.get("X-API-Key")
    if key:
        return key
    auth = req.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return None


def register_api_key_auth(app: Flask, api_key: str | None, *, service: str) -> None:
    """
    Attach a before_request hook that enforces X-API-Key / Bearer token auth.
    If api_key is None (not configured), auth is skipped and a startup warning is logged.
    Health and static paths are always exempt.
    """
    if not api_key:
        logger.warning(
            "SECURITY: %s is running without authentication. "
            "Set MW_%s_API_KEY to enable API key protection.",
            service.upper(),
            service.upper(),
        )
        return

    @app.before_request
    def _check_key():
        if request.path in _SKIP_AUTH_PATHS:
            return None
        if any(request.path.startswith(p) for p in _SKIP_AUTH_PREFIXES):
            return None
        provided = _extract_key(request)
        if not provided or not hmac.compare_digest(provided, api_key):
            logger.warning("Rejected unauthenticated request path=%s ip=%s",
                           request.path, request.remote_addr)
            return jsonify({"error": "Unauthorized"}), 401
        return None
