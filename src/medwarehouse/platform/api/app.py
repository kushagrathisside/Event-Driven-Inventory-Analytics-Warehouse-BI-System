from __future__ import annotations

import os
import secrets
from pathlib import Path

from flask import Flask

from medwarehouse.logging import configure_logging
from medwarehouse.platform.auth import register_api_key_auth
from medwarehouse.platform.control import ControlPlaneService, get_control_plane_service
from medwarehouse.platform.services import StatusService, get_status_service

from medwarehouse.platform.api.routes import create_api_blueprint, create_pages_blueprint


def create_platform_app(
    *,
    control_plane: ControlPlaneService | None = None,
    status_service: StatusService | None = None,
) -> Flask:
    from medwarehouse.config import get_settings

    control_plane = get_control_plane_service() if control_plane is None else control_plane
    status_service = get_status_service() if status_service is None else status_service

    platform_root = Path(__file__).resolve().parents[1]
    app = Flask(
        __name__,
        template_folder=str(platform_root / "ui" / "templates"),
        static_folder=str(platform_root / "ui" / "static"),
    )
    # Secret key required for Flask session (used by flash messages in page routes).
    # MW_PLATFORM_SECRET_KEY should be set in production; falls back to a per-process random
    # token in dev (flash messages are lost on restart, which is acceptable for a local tool).
    app.secret_key = os.environ.get("MW_PLATFORM_SECRET_KEY") or secrets.token_hex(32)

    register_api_key_auth(app, get_settings().platform_api_key, service="platform")
    app.register_blueprint(
        create_pages_blueprint(control_plane=control_plane, status_service=status_service)
    )
    app.register_blueprint(
        create_api_blueprint(control_plane=control_plane, status_service=status_service)
    )
    return app


def run_platform_server(*, host: str = "127.0.0.1", port: int = 8787, debug: bool = False) -> None:
    configure_logging("INFO")
    app = create_platform_app()
    app.run(host=host, port=port, debug=debug)

