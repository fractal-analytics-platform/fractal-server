"""
SLURM jobs of a Fractal job, identified by their stdout path.
"""

from typing import Literal

from pydantic import BaseModel

from ._job_states import STATES_FINISHED

# See `SlurmJob.slurm_stdout_remote_path`; SLURM keeps the `%j` placeholder
STDOUT_SUFFIX = "-slurm-%j.out"


def is_active_state(state: str) -> bool:
    # Same finished states as the polling of `BaseSlurmRunner`. Any other
    # state (also unknown ones) counts as active, so that the job is cancelled
    # rather than possibly run twice
    return state not in STATES_FINISHED


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
