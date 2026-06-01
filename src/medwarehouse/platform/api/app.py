from __future__ import annotations

from pathlib import Path

from flask import Flask

from medwarehouse.logging import configure_logging
from medwarehouse.platform.control import ControlPlaneService, get_control_plane_service
from medwarehouse.platform.services import StatusService, get_status_service

from medwarehouse.platform.api.routes import create_api_blueprint, create_pages_blueprint


def create_platform_app(
    *,
    control_plane: ControlPlaneService | None = None,
    status_service: StatusService | None = None,
) -> Flask:
    control_plane = get_control_plane_service() if control_plane is None else control_plane
    status_service = get_status_service() if status_service is None else status_service

    platform_root = Path(__file__).resolve().parents[1]
    app = Flask(
        __name__,
        template_folder=str(platform_root / "ui" / "templates"),
        static_folder=str(platform_root / "ui" / "static"),
    )
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

