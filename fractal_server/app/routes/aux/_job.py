from pathlib import Path

from fastapi import HTTPException
from fastapi import status

from fractal_server.app.models.v2 import JobV2
from fractal_server.logger import set_logger
from fractal_server.runner.filenames import SHUTDOWN_FILENAME

logger = set_logger(__name__)


def _write_shutdown_file(*, job: JobV2) -> None:
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


def _write_shutdown_file_or_422(*, job: JobV2) -> None:
    try:
        _write_shutdown_file(job=job)
    except Exception as e:
        logger.error(
            "An error was raised by `_write_shutdown_file` during  "
            f"Job {job.id} shutdown. Original error: '{str(e)}'."
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Could not shutdown Job {job.id}, please try again.",
        )
