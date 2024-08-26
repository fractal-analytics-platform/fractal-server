import os
import shutil
from io import BytesIO
from pathlib import Path
from typing import Iterator
from typing import TypeVar
from zipfile import ZIP_DEFLATED
from zipfile import ZipFile

T = TypeVar("T", str, BytesIO)


def _create_zip(folder: str, output: T) -> T:
    if isinstance(output, str) and os.path.exists(output):
        raise FileExistsError
    with ZipFile(output, mode="w", compression=ZIP_DEFLATED) as zipfile:
        for root, dirs, files in os.walk(folder):
            for file in files:
                file_path = os.path.join(root, file)
                archive_path = os.path.relpath(file_path, folder)
                zipfile.write(file_path, archive_path)
    return output


def _zip_folder_to_byte_stream(*, folder: str) -> Iterator:
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

        byte_stream = _create_zip(folder=folder, output=BytesIO())
        return iter([byte_stream.getvalue()])


def _zipping_worked(*, folder: str) -> bool:

    zip_file = f"{folder}_tmp.zip"
    folder = Path(folder)

    # CHECK 1: zip file exists
    if not os.path.exists(zip_file):
        return False

    folder_files = [f for f in folder.glob("**/*") if f.is_file()]
    folder_size = sum(f.stat().st_size for f in folder_files)

    with ZipFile(zip_file, "r") as zip_ref:
        zip_files = set(
            name for name in zip_ref.namelist() if not name.endswith("/")
        )
    zip_size = os.path.getsize(zip_file)

    # CHECK 2: all files are there
    if folder_files != zip_files:
        return False

    # CHECK 3: zip file is at least 10% of the original folder
    if zip_size * 10 < folder_size:
        return False

    return True


def _zip_folder_to_file_and_remove(*, folder: str) -> None:
    _create_zip(folder, f"{folder}_tmp.zip")
    if _zipping_worked(folder=folder):
        shutil.move(f"{folder}_tmp.zip", f"{folder}.zip")
        shutil.rmtree(folder)
    else:
        if os.path.exists(f"{folder}_tmp.zip"):
            os.remove(f"{folder}_tmp.zip")
