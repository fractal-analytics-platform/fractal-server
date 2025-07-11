import os
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import pytest

import fractal_server.zip_tools
from fractal_server.zip_tools import _create_zip
from fractal_server.zip_tools import _folder_can_be_deleted
from fractal_server.zip_tools import _zip_folder_to_byte_stream_iterator
from fractal_server.zip_tools import _zip_folder_to_file_and_remove


def make_folder(base: Path) -> Path:
    """
    Creates the following repo inside `base`
    - test:
        - file1
        - subfolder1:
            - file2
            - subsubfolder1:
                - file3
            - subsubfolder2:
                - file4
        - subfolder2:
            - file5
    """
    test_folder = base / "test"
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
    return test_folder


def test_create_zip(tmp_path):
    test_folder = make_folder(tmp_path)

    ret = _create_zip(test_folder, f"{test_folder}.zip")
    assert ret == f"{test_folder}.zip"

    with pytest.raises(FileExistsError):
        _create_zip(test_folder, f"{test_folder}.zip")
    os.unlink(f"{test_folder}.zip")

    with pytest.raises(ValueError):
        _create_zip(test_folder, BytesIO(b"foo"))
    ret = _create_zip(test_folder, BytesIO())
    assert isinstance(ret, BytesIO)
    assert ret.getbuffer().nbytes > 0


def test_create_zip_fail(tmp_path, monkeypatch):
    def _fake_create_zip(*args, **kwargs):
        raise RuntimeError("foo")

    test_folder = make_folder(tmp_path)

    corrupted_file = Path(f"{test_folder}_tmp.zip")
    corrupted_file.touch()

    assert corrupted_file.exists()
    monkeypatch.setattr(
        fractal_server.zip_tools, "_create_zip", _fake_create_zip
    )
    _zip_folder_to_file_and_remove(test_folder.as_posix())
    assert not corrupted_file.exists()


def test_zip_folder_to_byte_stream_iterator(tmp_path: Path):
    test_folder = make_folder(tmp_path)

    # Case 1: zip file does not exist yet
    output = _zip_folder_to_byte_stream_iterator(folder=test_folder)

    zip_file_1 = Path(tmp_path / "foo.zip")
    with zip_file_1.open("wb") as f:
        for byte in output:
            f.write(byte)

    unzipped_archived_path = tmp_path / "unzipped_folder"
    unzipped_archived_path.mkdir()
    with ZipFile(zip_file_1.as_posix(), mode="r") as zipfile:
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

    # Case 2: zip file already exists
    _create_zip(test_folder, output=f"{test_folder}.zip")
    output = _zip_folder_to_byte_stream_iterator(folder=test_folder)

    zip_file_2 = Path(tmp_path / "bar.zip")
    with zip_file_2.open("wb") as f:
        for byte in output:
            f.write(byte)

    unzipped_archived_path_2 = tmp_path / "unzipped_folder2"
    unzipped_archived_path_2.mkdir()
    with ZipFile(zip_file_2.as_posix(), mode="r") as zipfile:
        zipfile.extractall(path=unzipped_archived_path_2.as_posix())
    glob_list = [file.name for file in unzipped_archived_path_2.rglob("*")]
    assert "file1" in glob_list
    assert "file2" in glob_list
    assert "file3" in glob_list
    assert "file4" in glob_list
    assert "file5" in glob_list
    assert "subfolder1" in glob_list
    assert "subfolder2" in glob_list
    assert "subsubfolder1" in glob_list
    assert "subsubfolder2" in glob_list


def test_folder_can_be_deleted(tmp_path: Path, monkeypatch):
    test_folder = make_folder(tmp_path)
    assert _folder_can_be_deleted(test_folder) is False

    _create_zip(test_folder, output=f"{test_folder}.zip")
    assert _folder_can_be_deleted(test_folder) is False
    # monkeypatch THRESHOLD_ZIP_FILE_SIZE_MB to make folder deletable
    monkeypatch.setattr(
        fractal_server.zip_tools, "THRESHOLD_ZIP_FILE_SIZE_MB", 0.0001
    )
    assert _folder_can_be_deleted(test_folder) is True

    os.unlink(test_folder / "file1")
    assert _folder_can_be_deleted(test_folder) is False


def test_zip_folder_to_file_and_remove(tmp_path: Path, monkeypatch):
    assert os.listdir(tmp_path) == []

    test_folder = make_folder(tmp_path)
    assert os.listdir(tmp_path) == ["test"]

    _zip_folder_to_file_and_remove(test_folder)
    assert set(os.listdir(tmp_path)) == {"test", "test.zip"}

    with ZipFile(tmp_path / "test.zip", mode="r") as zipfile:
        assert "file1" in zipfile.namelist()

    os.unlink(test_folder / "file1")
    # monkeypatch THRESHOLD_ZIP_FILE_SIZE_MB to make folder deletable
    monkeypatch.setattr(
        fractal_server.zip_tools, "THRESHOLD_ZIP_FILE_SIZE_MB", 0.0001
    )
    _zip_folder_to_file_and_remove(test_folder)
    assert os.listdir(tmp_path) == ["test.zip"]
    with ZipFile(tmp_path / "test.zip", mode="r") as zipfile:
        # `test.zip`` has been overriden by `shutil.move`
        assert "file1" not in zipfile.namelist()
