"""
Job queue table for `slurm_rest` resources (`GET /api/v2/job/squeue/`).

For the other SLURM resources, the endpoint returns the output of a `squeue`
command. For `slurm_rest` resources, it calls `GET /jobs` of `slurmrestd`
with the user's token (no SSH), which returns JSON: this module turns it
into a text table with the same columns, so that it is shown in the same way
in fractal-web.
"""

import time
from typing import Any
from typing import Literal

from fractal_server.runner.config import SlurmRestConfig
from fractal_server.runner.executors.slurm_common._job_states import (
    STATES_FINISHED,
)

from .client import SlurmRestClient
from .client import squeue_state
from .token import get_token_provider

# Header and width of the columns before `NODELIST(REASON)` (as in the
# `squeue --format` of the endpoint)
# NOTE: we can reduce the custom code here, as no strict column enforcement
# from fractal-web side, for now keeping the same format as other runners
# for consistency.
COLUMNS = (
    ("JOBID", 12),
    ("PARTITION", 9),
    ("NAME", 24),
    ("USER", 14),
    ("ACCOUNT", 14),
    ("STATE", 11),
    ("TIME", 12),
    ("NODES", 6),
    ("CPUS", 4),
    ("MIN_MEMORY", 10),
)
LAST_HEADER = "NODELIST(REASON)"


def _number(value: Any) -> int:
    """
    Value of a number field: either a number, or a `{"set", "infinite",
    "number"}` object (`0` if unset or infinite).
    """
    if isinstance(value, dict):
        if value.get("set") and not value.get("infinite"):
            return int(value.get("number", 0))
        return 0
    return int(value or 0)


def format_seconds(seconds: int) -> str:
    """
    Time as printed by `squeue`, e.g. `0:05`, `1:02:03`, `2-01:02:03`.
    """
    days, rest = divmod(max(seconds, 0), 86400)
    hours, rest = divmod(rest, 3600)
    minutes, secs = divmod(rest, 60)
    if days:
        return f"{days}-{hours:02d}:{minutes:02d}:{secs:02d}"
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def _line(values: list[str], last: str) -> str:
    """
    Build one line of the job queue table.
    """
    cells = [
        value[:width].rjust(width)
        for (_, width), value in zip(COLUMNS, values, strict=True)
    ]
    return " ".join(cells + [last])


def _job_line(job: dict[str, Any], now: int) -> str:
    state = squeue_state(job.get("job_state") or [])
    # Run time: until now, or until the end time if already passed
    start = _number(job.get("start_time"))
    end = _number(job.get("end_time"))
    elapsed = 0
    if start:
        elapsed = (min(end, now) if end else now) - start
    memory = _number(job.get("memory_per_cpu")) or _number(
        job.get("memory_per_node")
    )
    if state == "PENDING":
        last = f"({job.get('state_reason') or 'None'})"
    else:
        last = job.get("nodes") or ""
    values = [
        str(job.get("job_id", "")),
        job.get("partition") or "",
        job.get("name") or "",
        job.get("user_name") or "",
        job.get("account") or "",
        state,
        format_seconds(elapsed),
        str(_number(job.get("node_count"))),
        str(_number(job.get("cpus"))),
        f"{memory}M",
    ]
    return _line(values, last)


def format_job_queue_table(jobs: list[dict[str, Any]], *, now: int) -> str:
    """
    `squeue`-like text for `slurmrestd` jobs: not finished jobs, by job ID.

    Args:
        jobs: Jobs of a `GET /jobs` response.
        now: Current time (seconds since the epoch), for run times.
    """
    # `GET /jobs` also lists recently finished jobs, which `squeue` hides
    shown = [
        job
        for job in jobs
        if squeue_state(job.get("job_state") or []) not in STATES_FINISHED
    ]
    shown.sort(key=lambda job: job.get("job_id", 0))
    lines = [_line([header for header, _ in COLUMNS], LAST_HEADER)]
    lines.extend(_job_line(job, now) for job in shown)
    return "".join(f"{line}\n" for line in lines)


def get_job_queue_table(
    *,
    rest_config: SlurmRestConfig,
    user_id: int,
    profile_username: str,
    scope: Literal["all", "user", "accounts"],
    slurm_accounts: list[str],
) -> str:
    """
    `squeue`-like text of the jobs that the SLURM token of a Fractal user
    can see (blocking: SSO token and `slurmrestd` requests).

    Args:
        rest_config: `slurmrestd` connection of the resource.
        user_id: Fractal user.
        profile_username: SSH user of the profile.
        scope:
            `user`: jobs of the SLURM user of the token; `accounts`: jobs of
            the `slurm_accounts` (all jobs if there are none); `all`: all
            jobs.
        slurm_accounts: SLURM accounts of the Fractal user.
    """
    token_provider = get_token_provider(
        user_id=user_id, profile_username=profile_username
    )
    client = SlurmRestClient(
        base_url=rest_config.url,
        username=token_provider.slurm_username,
        get_token=token_provider.get_token,
        ca_bundle=rest_config.ca_bundle,
        api_version=rest_config.api_version,
    )
    jobs = client.list_jobs()
    if scope == "user":
        jobs = [
            job
            for job in jobs
            if job.get("user_name") == token_provider.slurm_username
        ]
    elif scope == "accounts" and slurm_accounts:
        jobs = [job for job in jobs if job.get("account") in slurm_accounts]
    return format_job_queue_table(jobs, now=int(time.time()))
