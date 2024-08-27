import os
from pathlib import Path
from zipfile import ZipFile

import fractal_server.zip_tools
from fractal_server.zip_tools import _zip_folder_to_byte_stream_iterator
from fractal_server.zip_tools import _zip_folder_to_file_and_remove


def test_zip_folder_to_byte_stream_iterator(tmp_path: Path):

    # Prepare file/folder structure
    test_folder = tmp_path / "test"
    test_folder.mkdir()
    (test_folder / "subfolder1").mkdir()
    (test_folder / "subfolder1/subsubfolder1").mkdir()
    (test_folder / "subfolder1/subsubfolder2").mkdir()
    (test_folder / "subfolder2").mkdir()
    (test_folder / "file1").touch()
    (test_folder / "subfolder1/file2").touch()
    (test_folder / "subfolder1/subsubfolder1/file3").touch()
    (test_folder / "subfolder1/subsubfolder2/file4").touch()
    (test_folder / "subfolder2/file5").touch()

    output = _zip_folder_to_byte_stream_iterator(folder=tmp_path.as_posix())

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
    assert "file3" in glob_list
    assert "file4" in glob_list
    assert "file5" in glob_list
    assert "subfolder1" in glob_list
    assert "subfolder2" in glob_list
    assert "subsubfolder1" in glob_list
    assert "subsubfolder2" in glob_list


def test_zip_folder_to_file_and_remove(tmp_path: Path, monkeypatch):

    monkeypatch.setattr(
        fractal_server.zip_tools, "THRESHOLD_ZIP_FILE_SIZE_MB", 0.0005
    )

    # Prepare file/folder structure
    test_folder = tmp_path / "test"
    test_folder.mkdir()
    (test_folder / "subfolder1").mkdir()
    (test_folder / "subfolder1/subsubfolder1").mkdir()
    (test_folder / "subfolder1/subsubfolder2").mkdir()
    (test_folder / "subfolder2").mkdir()
    (test_folder / "file1").touch()
    (test_folder / "subfolder1/file2").touch()
    (test_folder / "subfolder1/subsubfolder1/file3").touch()
    (test_folder / "subfolder1/subsubfolder2/file4").touch()
    (test_folder / "subfolder2/file5").touch()

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
    assert "file3" in glob_list
    assert "file4" in glob_list
    assert "file5" in glob_list
    assert "subfolder1" in glob_list
    assert "subfolder2" in glob_list
    assert "subsubfolder1" in glob_list
    assert "subsubfolder2" in glob_list
