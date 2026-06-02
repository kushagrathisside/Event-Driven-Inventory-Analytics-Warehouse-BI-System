from __future__ import annotations

import argparse

from medwarehouse_webapp.app import run_webapp


def main() -> None:
    parser = argparse.ArgumentParser(prog="medwarehouse-webapp")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    run_webapp(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
