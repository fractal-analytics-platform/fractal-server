from typing import Any
from typing import Literal

from pydantic import BaseModel

from fractal_server.runner.executors.slurm_rest.client import SlurmRestClient
from fractal_server.ssh._fabric import FractalSSH

TAG_PREFIX = "fractal:job"
ACTIVE_STATES = {
    "PENDING",
    "RUNNING",
    "CONFIGURING",
    "COMPLETING",
    "SUSPENDED",
    "REQUEUED",
    "RESIZING",
}


class TaggedSlurmJob(BaseModel):
    slurm_job_id: str
    state: str
    subfolder: str | None
    prefix: str
    source: Literal["slurm", "slurmdb"]
    end_time: int | None = None

    @property
    def is_active(self) -> bool:
        return self.state in ACTIVE_STATES


def parse_tag(
    comment: str | None, fractal_job_id: int
) -> tuple[str | None, str] | None:
    """
    Parse `fractal:job{id}:{subfolder}:{prefix}` into `(subfolder, prefix)`.
    """
    head = f"{TAG_PREFIX}{fractal_job_id}:"
    if not comment or not comment.startswith(head):
        return None
    parts = comment[len(head) :].split(":")
    if len(parts) == 1:
        # Tags written before the subfolder was added
        return None, parts[0]
    return parts[0], parts[1]


def _first_state(value: Any) -> str:
    # v0.0.44 returns a list, e.g. ["COMPLETED", "COMPLETING"]
    if isinstance(value, list):
        return value[0] if value else "UNKNOWN"
    return value or "UNKNOWN"


def _timestamp(value: Any) -> int | None:
    if isinstance(value, dict):
        return value.get("number") if value.get("set") else None
    return value or None


def collect_tagged_jobs(
    *,
    client: SlurmRestClient,
    fractal_job_id: int,
    start_time: int,
) -> list[TaggedSlurmJob]:
    """
    Find the SLURM jobs of a fractal job, both finished and active.

    Args:
        client:
        fractal_job_id:
        start_time: Unix timestamp, lower bound for the accounting query.
    """
    jobs: dict[str, TaggedSlurmJob] = {}
    for item in client.list_accounting_jobs(start_time=start_time):
        tag = parse_tag((item.get("comment") or {}).get("job"), fractal_job_id)
        if tag is None:
            continue
        slurm_job_id = str(item["job_id"])
        jobs[slurm_job_id] = TaggedSlurmJob(
            slurm_job_id=slurm_job_id,
            state=_first_state((item.get("state") or {}).get("current")),
            subfolder=tag[0],
            prefix=tag[1],
            source="slurmdb",
            end_time=_timestamp((item.get("time") or {}).get("end")),
        )
    # slurmdbd only stores the comment once a job ended, so live data wins
    for item in client.list_jobs():
        tag = parse_tag(item.get("comment"), fractal_job_id)
        if tag is None:
            continue
        slurm_job_id = str(item["job_id"])
        job = TaggedSlurmJob(
            slurm_job_id=slurm_job_id,
            state=_first_state(item.get("job_state")),
            subfolder=tag[0],
            prefix=tag[1],
            source="slurm",
        )
        # For active jobs, `end_time` is the expected end
        if not job.is_active:
            job.end_time = _timestamp(item.get("end_time"))
        jobs[slurm_job_id] = job
    return sorted(jobs.values(), key=lambda job: int(job.slurm_job_id))


def read_task_outputs(
    *, fractal_ssh: FractalSSH, folder: str
) -> dict[str, Any]:
    """
    Read all `*-output.json` files of a remote task subfolder.
    """
    names = fractal_ssh.run_command(cmd=f"ls -1 {folder}").split()
    return {
        name: fractal_ssh.read_remote_json_file(f"{folder}/{name}")
        for name in names
        if name.endswith("-output.json")
    }
