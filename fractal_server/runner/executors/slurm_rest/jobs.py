from typing import Any

from fractal_server.runner.executors.slurm_common.fractal_slurm_jobs import (
    FractalSlurmJob,
)
from fractal_server.runner.executors.slurm_common.fractal_slurm_jobs import (
    parse_stdout_path,
)

from .client import SlurmRestClient


def _first_state(value: Any) -> str:
    # v0.0.44 returns a list, e.g. ["COMPLETED", "COMPLETING"]
    if isinstance(value, list):
        return value[0] if value else "UNKNOWN"
    return value or "UNKNOWN"


def _timestamp(value: Any) -> int | None:
    if isinstance(value, dict):
        return value.get("number") if value.get("set") else None
    return value or None


def collect_fractal_slurm_jobs(
    *,
    client: SlurmRestClient,
    job_root: str,
    start_time: int,
) -> list[FractalSlurmJob]:
    """
    Find the SLURM jobs of a fractal job, both finished and active, from the
    stdout path that SLURM stores for each job.

    Args:
        client:
        job_root: Remote folder of the fractal job (`job.working_dir_user`).
        start_time: Unix timestamp, lower bound for the accounting query.
    """
    jobs: dict[str, FractalSlurmJob] = {}
    for item in client.list_accounting_jobs(start_time=start_time):
        parsed = parse_stdout_path(item.get("stdout"), job_root)
        if parsed is None:
            continue
        slurm_job_id = str(item["job_id"])
        jobs[slurm_job_id] = FractalSlurmJob(
            slurm_job_id=slurm_job_id,
            state=_first_state((item.get("state") or {}).get("current")),
            subfolder=parsed[0],
            prefix=parsed[1],
            source="slurmdb",
            end_time=_timestamp((item.get("time") or {}).get("end")),
        )
    # Live data is more up to date
    for item in client.list_jobs():
        parsed = parse_stdout_path(item.get("standard_output"), job_root)
        if parsed is None:
            continue
        slurm_job_id = str(item["job_id"])
        job = FractalSlurmJob(
            slurm_job_id=slurm_job_id,
            state=_first_state(item.get("job_state")),
            subfolder=parsed[0],
            prefix=parsed[1],
            source="slurm",
        )
        # For active jobs, `end_time` is the expected end
        if not job.is_active:
            job.end_time = _timestamp(item.get("end_time"))
        jobs[slurm_job_id] = job
    return sorted(jobs.values(), key=lambda job: int(job.slurm_job_id))
