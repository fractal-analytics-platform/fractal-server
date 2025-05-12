import os
import shutil
from collections.abc import Iterator
from io import BytesIO
from pathlib import Path
from typing import TypeVar
from zipfile import ZIP_DEFLATED
from zipfile import ZipFile

from fractal_server.logger import set_logger

logger = set_logger("fractal_server.zip_tools")

T = TypeVar("T", str, BytesIO)
THRESHOLD_ZIP_FILE_SIZE_MB = 1.0


def _create_zip(folder: str, output: T) -> T:
    """
    Zip a folder into a zip-file or into a BytesIO.

    Args:
        folder: Folder to be zipped.
        output: Either a string with the path of the zip file, or a BytesIO
            object.

    Returns:
        Either the zip-file path string, or the modified BytesIO object.
    """
    if isinstance(output, str) and os.path.exists(output):
        raise FileExistsError(f"Zip file '{output}' already exists")
    if isinstance(output, BytesIO) and output.getbuffer().nbytes > 0:
        raise ValueError("BytesIO is not empty")

    with ZipFile(output, mode="w", compression=ZIP_DEFLATED) as zipfile:
        for root, dirs, files in os.walk(folder):
            for file in files:
                file_path = os.path.join(root, file)
                archive_path = os.path.relpath(file_path, folder)
                zipfile.write(file_path, archive_path)
    return output


def _zip_folder_to_byte_stream_iterator(folder: str) -> Iterator:
    """
    Returns byte stream with the zipped log folder of a job.

    Args:
        folder: the folder to zip
    """
    zip_file = Path(f"{folder}.zip")

    if os.path.exists(zip_file):

        def iterfile():
            """
            https://fastapi.tiangolo.com/advanced/custom-response/#using-streamingresponse-with-file-like-objects
            """
            with open(zip_file, mode="rb") as file_like:
                yield from file_like

        return iterfile()

    else:

        byte_stream = _create_zip(folder, output=BytesIO())
        return iter([byte_stream.getvalue()])


def _folder_can_be_deleted(folder: str) -> bool:
    """
    Given the path of a folder as string, returns `False` if either:
    - the related zip file `{folder}.zip` does already exists,
    - the folder and the zip file have a different number of internal files,
    - the zip file has a very small size.
    Otherwise returns `True`.
    """
    # CHECK 1: zip file exists
    zip_file = f"{folder}.zip"
    if not os.path.exists(zip_file):
        logger.info(
            f"Folder '{folder}' won't be deleted because file '{zip_file}' "
            "does not exist."
        )
        return False

    # CHECK 2: folder and zip file have the same number of files
    folder_files_count = sum(1 for f in Path(folder).rglob("*") if f.is_file())
    with ZipFile(zip_file, "r") as zip_ref:
        zip_files_count = len(zip_ref.namelist())
    if folder_files_count != zip_files_count:
        logger.info(
            f"Folder '{folder}' won't be deleted because it contains "
            f"{folder_files_count} files while '{zip_file}' contains "
            f"{zip_files_count}."
        )
        return False

    # CHECK 3: zip file size is >= than `THRESHOLD_ZIP_FILE_SIZE_MB`
    zip_size = os.path.getsize(zip_file)
    if zip_size < THRESHOLD_ZIP_FILE_SIZE_MB * (1024**2):
        logger.info(
            f"Folder '{folder}' won't be deleted because '{zip_file}' is too "
            f"small ({zip_size / (1024**2):.5f} MB, whereas the minimum limit "
            f"for deletion is {THRESHOLD_ZIP_FILE_SIZE_MB})."
        )
        return False

    return True


def _zip_folder_to_file_and_remove(folder: str) -> None:
    """
    Creates a ZIP archive of the specified folder and removes the original
    folder (if it can be deleted).

    This function performs the following steps:
    1. Creates a ZIP archive of the `folder` and names it with a temporary
       suffix `_tmp.zip`.
    2. Renames the ZIP removing the suffix (this would possibly overwrite a
        file with the same name already present).
    3. Checks if the folder can be safely deleted using the
        `_folder_can_be_deleted` function. If so, deletes the original folder.
    """

    tmp_zipfile = f"{folder}_tmp.zip"
    zipfile = f"{folder}.zip"

    try:
        logger.info(f"Start creating temporary zip file at '{tmp_zipfile}'.")
        _create_zip(folder, tmp_zipfile)
        logger.info("Zip file created.")
    except Exception as e:
        logger.error(
            f"Error while creating temporary zip file. Original error: '{e}'."
        )
        Path(tmp_zipfile).unlink(missing_ok=True)
        return

    logger.info(f"Moving temporary zip file to {zipfile}.")
    shutil.move(tmp_zipfile, zipfile)
    logger.info("Zip file moved.")

    if _folder_can_be_deleted(folder):
        logger.info(f"Removing folder '{folder}'.")
        shutil.rmtree(folder)
        logger.info("Folder removed.")
