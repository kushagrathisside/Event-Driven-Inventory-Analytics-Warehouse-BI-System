from __future__ import annotations

from flask import Flask, jsonify

from medwarehouse.config import get_settings
from medwarehouse.logging import configure_logging
from medwarehouse.platform.auth import register_api_key_auth
from medwarehouse_webapp.api.inventory import blueprint as inventory_bp
from medwarehouse_webapp.api.orders import blueprint as orders_bp
from medwarehouse_webapp.api.reorder import blueprint as reorder_bp
from medwarehouse_webapp.api.stock import blueprint as stock_bp


def create_webapp() -> Flask:
    app = Flask(__name__)
    register_api_key_auth(app, get_settings().webapp_api_key, service="webapp")
    app.register_blueprint(stock_bp)
    app.register_blueprint(inventory_bp)
    app.register_blueprint(reorder_bp)
    app.register_blueprint(orders_bp)

    @app.get("/health")
    def health():
        return jsonify({"status": "ok", "service": "medwarehouse-webapp"})

    return app


def run_webapp(*, host: str = "127.0.0.1", port: int = 8080, debug: bool = False) -> None:
    configure_logging("INFO")
    app = create_webapp()
    app.run(host=host, port=port, debug=debug)
