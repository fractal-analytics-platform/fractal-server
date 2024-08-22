import os
from io import BytesIO
from zipfile import ZIP_DEFLATED
from zipfile import ZipFile


def _zip_folder_to_byte_stream(*, folder: str) -> BytesIO:
    """
    Get byte stream with the zipped log folder of a job.

    Args:
        folder: the folder to zip
    """

    byte_stream = BytesIO()
    with ZipFile(byte_stream, mode="w", compression=ZIP_DEFLATED) as zipfile:
        for root, dirs, files in os.walk(folder):
            for file in files:
                file_path = os.path.join(root, file)
                archive_path = os.path.relpath(file_path, folder)
                zipfile.write(file_path, archive_path)

    return byte_stream
