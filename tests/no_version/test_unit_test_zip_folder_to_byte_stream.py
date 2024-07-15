from pathlib import Path
from zipfile import ZipFile

from devtools import debug

from fractal_server.app.routes.aux._job import _zip_folder_to_byte_stream


def test_zip_folder_to_byte_stream(tmp_path: Path):
    debug(tmp_path)

    # Prepare file/folder structure
    (tmp_path / "file1").touch()
    (tmp_path / "file2").touch()
    (tmp_path / "folder").mkdir()
    (tmp_path / "folder/file3").touch()
    (tmp_path / "folder/file4").touch()

    output = _zip_folder_to_byte_stream(folder=tmp_path.as_posix())

    # Write BytesIO to file
    archive_path = tmp_path / "zipped_folder.zip"
    with archive_path.open("wb") as f:
        f.write(output.getbuffer())

    # Unzip the log archive
    unzipped_archived_path = tmp_path / "unzipped_folder"
    unzipped_archived_path.mkdir()
    with ZipFile(archive_path.as_posix(), mode="r") as zipfile:
        zipfile.extractall(path=unzipped_archived_path.as_posix())

    # Verify that all expected items are present
    glob_list = [file.name for file in unzipped_archived_path.rglob("*")]
    debug(glob_list)
    assert "file1" in glob_list
    assert "file2" in glob_list
    assert "folder" in glob_list
    assert "file3" in glob_list
    assert "file4" in glob_list
