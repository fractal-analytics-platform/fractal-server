from typing import Any
from typing import Literal

from pydantic import BaseModel

from fractal_server.runner.executors.slurm_rest.client import SlurmRestClient

# See `SlurmJob.slurm_stdout_remote_path`; SLURM keeps the `%j` placeholder
STDOUT_SUFFIX = "-slurm-%j.out"
# Any other state (also unknown ones) counts as active, so that the job is
# cancelled rather than possibly run twice
FINISHED_STATES = {
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "OUT_OF_MEMORY",
    "NODE_FAIL",
    "PREEMPTED",
    "BOOT_FAIL",
    "DEADLINE",
}


def is_active_state(state: str) -> bool:
    return state not in FINISHED_STATES


class FractalSlurmJob(BaseModel):
    slurm_job_id: str
    state: str
    subfolder: str
    prefix: str
    source: Literal["slurm", "slurmdb"]
    end_time: int | None = None

    @property
    def is_active(self) -> bool:
        return is_active_state(self.state)


def parse_stdout_path(
    stdout: str | None, job_root: str
) -> tuple[str, str] | None:
    """
    Split `{job_root}/{subfolder}/{prefix}-slurm-%j.out` into
    `(subfolder, prefix)`, for a SLURM job of the fractal job with remote
    folder `job_root`.
    """
    head = f"{job_root.rstrip('/')}/"
    if (
        not stdout
        or not stdout.startswith(head)
        or not stdout.endswith(STDOUT_SUFFIX)
    ):
        return None
    subfolder, _, prefix = stdout[len(head) : -len(STDOUT_SUFFIX)].partition(
        "/"
    )
    if not subfolder or not prefix or "/" in prefix:
        return None
    return subfolder, prefix


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
