from io import BytesIO
from pathlib import Path
from zipfile import ZIP_DEFLATED
from zipfile import ZipFile

from fastapi.responses import StreamingResponse

from ...db import AsyncSession
from ...models import ApplyWorkflow
from ...runner._common import SHUTDOWN_FILENAME


def _write_shutdown_file(*, job: ApplyWorkflow):
    """
    Note: we are **not** marking the job as failed (by setting its `status`
    attribute) here, since this will be done by the runner backend as soon as
    it detects the shutdown-trigerring file and performs the actual shutdown.
    """
    shutdown_file = Path(job.working_dir) / SHUTDOWN_FILENAME
    with shutdown_file.open("w") as f:
        f.write(f"Trigger executor shutdown for {job.id=}.")


async def _get_streaming_response(
    *, job: ApplyWorkflow, db: AsyncSession
) -> StreamingResponse:
    working_dir_str = job.dict()["working_dir"]
    working_dir_path = Path(working_dir_str)

    PREFIX_ZIP = working_dir_path.name
    zip_filename = f"{PREFIX_ZIP}_archive.zip"
    byte_stream = BytesIO()
    with ZipFile(byte_stream, mode="w", compression=ZIP_DEFLATED) as zipfile:
        for fpath in working_dir_path.glob("*"):
            zipfile.write(filename=str(fpath), arcname=str(fpath.name))

    await db.close()

    return StreamingResponse(
        iter([byte_stream.getvalue()]),
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment;filename={zip_filename}"},
    )
