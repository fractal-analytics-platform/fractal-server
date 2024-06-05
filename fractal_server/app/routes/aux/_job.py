from io import BytesIO
from pathlib import Path
from typing import Union
from zipfile import ZIP_DEFLATED
from zipfile import ZipFile

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


def _zip_folder_to_byte_stream(*, folder: str, zip_filename: str) -> BytesIO:
    """
    Get byte stream with the zipped log folder of a job.

    Args:
        folder: the folder to zip
        zip_filename: name of the zipped archive
    """
    working_dir_path = Path(folder)

    byte_stream = BytesIO()
    with ZipFile(byte_stream, mode="w", compression=ZIP_DEFLATED) as zipfile:
        for fpath in working_dir_path.glob("*"):
            zipfile.write(filename=str(fpath), arcname=str(fpath.name))

    return byte_stream
