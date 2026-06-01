from __future__ import annotations

from medwarehouse.platform.control import get_control_plane_service


class JobController:
    def start(self, job_id: str) -> tuple[bool, str]:
        return get_control_plane_service().start_job(job_id)

    def stop(self, job_id: str) -> tuple[bool, str]:
        return get_control_plane_service().stop_job(job_id)

    def snapshots(self) -> list[dict[str, object]]:
        return get_control_plane_service().job_snapshots()


job_controller = JobController()
