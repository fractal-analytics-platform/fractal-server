import os
import shutil
from io import BytesIO
from pathlib import Path
from zipfile import ZIP_DEFLATED
from zipfile import ZipFile


def _zip_folder_to_byte_stream(*, folder: str):
    """
    Get byte stream with the zipped log folder of a job.

    Args:
        folder: the folder to zip
    """
    zip_file = Path(f"{folder}.zip")

    if os.path.exists(zip_file):

        def iterfile():
            """
            https://fastapi.tiangolo.com/advanced/
            custom-response/#using-streamingresponse-with-file-like-objects
            """
            with open(zip_file, mode="rb") as file_like:
                yield from file_like

        return iterfile()

    else:

        byte_stream = BytesIO()
        with ZipFile(
            byte_stream, mode="w", compression=ZIP_DEFLATED
        ) as zipfile:
            for root, dirs, files in os.walk(folder):
                for file in files:
                    file_path = os.path.join(root, file)
                    archive_path = os.path.relpath(file_path, folder)
                    zipfile.write(file_path, archive_path)
        return iter([byte_stream.getvalue()])


def _zip_folder_to_file_and_remove(*, folder: str) -> None:
    shutil.make_archive(
        base_name=f"{folder}_tmp", format="zip", root_dir=Path(folder)
    )
    shutil.move(f"{folder}_tmp.zip", f"{folder}.zip")
    shutil.rmtree(folder)
