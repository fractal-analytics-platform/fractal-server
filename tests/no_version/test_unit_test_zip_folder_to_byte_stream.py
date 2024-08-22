import os
import shutil
from pathlib import Path
from zipfile import ZipFile

from devtools import debug

from fractal_server.zip_tools import _zip_folder_to_byte_stream
from fractal_server.zip_tools import _zip_folder_to_file_and_remove


def test_zip_folder_to_byte_stream(tmp_path: Path):

    debug(os.listdir(tmp_path))

    # Prepare file/folder structure
    (tmp_path / "test").mkdir()
    (tmp_path / "test/file1").touch()
    (tmp_path / "test/file2").touch()
    (tmp_path / "test/subfolder").mkdir()
    (tmp_path / "test/subfolder/file3").touch()
    (tmp_path / "test/subfolder/file4").touch()

    output = _zip_folder_to_byte_stream(folder=tmp_path.as_posix())

    # Write BytesIO to file
    zip_file = tmp_path / "zipped_folder.zip"
    with zip_file.open("wb") as f:
        for byte in output:
            f.write(byte)

    # Unzip the log archive
    unzipped_archived_path = tmp_path / "unzipped_folder"
    unzipped_archived_path.mkdir()
    with ZipFile(zip_file.as_posix(), mode="r") as zipfile:
        zipfile.extractall(path=unzipped_archived_path.as_posix())

    # Verify that all expected items are present
    glob_list = [file.name for file in unzipped_archived_path.rglob("*")]
    assert "file1" in glob_list
    assert "file2" in glob_list
    assert "subfolder" in glob_list
    assert "file3" in glob_list
    assert "file4" in glob_list

    zip_file.unlink()
    shutil.rmtree(unzipped_archived_path)
    assert os.listdir(tmp_path) == ["test"]

    _zip_folder_to_file_and_remove(folder=tmp_path / "test")
    # assert that original `test` folder has been deleted
    assert os.listdir(tmp_path) == ["test.zip"]

    unzipped_archived_path = tmp_path / "unzipped_folder"
    unzipped_archived_path.mkdir()
    with ZipFile(tmp_path / "test.zip", mode="r") as zipfile:
        zipfile.extractall(path=unzipped_archived_path.as_posix())

    glob_list = [file.name for file in unzipped_archived_path.rglob("*")]
    assert "file1" in glob_list
    assert "file2" in glob_list
    assert "subfolder" in glob_list
    assert "file3" in glob_list
    assert "file4" in glob_list
