from pathlib import Path
from typing import Union

from ...models.v1 import ApplyWorkflow
from ...models.v2 import JobV2
from ...runner.filenames import SHUTDOWN_FILENAME


def _write_shutdown_file(*, job: Union[ApplyWorkflow, JobV2]):
    """
    Write job's shutdown file.

    Args:
        job:

    Note: we are **not** marking the job as failed (by setting its `status`
    attribute) here, since this will be done by the runner backend as soon as
    it detects the shutdown-trigerring file and performs the actual shutdown.
    """
    shutdown_file = Path(job.working_dir) / SHUTDOWN_FILENAME
    with shutdown_file.open("w") as f:
        f.write(f"Trigger executor shutdown for {job.id=}.")
