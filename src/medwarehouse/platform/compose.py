from __future__ import annotations

import subprocess
from dataclasses import dataclass


@dataclass(frozen=True)
class ComposeResolution:
    available: bool
    command: tuple[str, ...] | None
    message: str


def _run_version_check(base_command: tuple[str, ...]) -> tuple[bool, str]:
    try:
        # No cwd needed — `docker compose version` is not project-specific.
        result = subprocess.run(
            [*base_command, "version"],
            check=True,
            capture_output=True,
            text=True,
        )
        output = (result.stdout or result.stderr or "").strip()
        return True, output
    except FileNotFoundError as exc:
        return False, str(exc)
    except subprocess.CalledProcessError as exc:
        combined = "\n".join(
            part.strip()
            for part in ((exc.stdout or ""), (exc.stderr or ""))
            if part.strip()
        )
        return False, combined or str(exc)


def resolve_compose_command() -> ComposeResolution:
    attempts: list[str] = []
    for command in (("docker", "compose"), ("docker-compose",)):
        ok, output = _run_version_check(command)
        if ok:
            return ComposeResolution(
                available=True,
                command=command,
                message=output or "compose available",
            )
        attempts.append(f"{' '.join(command)}: {output}")

    return ComposeResolution(
        available=False,
        command=None,
        message="No usable Docker Compose command found.\n" + "\n".join(attempts),
    )
