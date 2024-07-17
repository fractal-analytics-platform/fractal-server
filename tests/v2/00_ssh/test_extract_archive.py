from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.runner.compress_folder import compress_folder
from fractal_server.app.runner.extract_archive import extract_archive
from fractal_server.app.runner.extract_archive import main


def test_main_failures():
    """
    Test failures in `extract_archive.main` function.
    """
    # Too many arguments
    with pytest.raises(SystemExit, match="Invalid argument"):
        main(["dummy", "/arg1.tar.gz", "/arg2.tar.gz"])
    # Too few arguments
    with pytest.raises(SystemExit, match="Invalid argument"):
        main(["dummy"])
    # Argument does not end with ".tar.gz"
    with pytest.raises(SystemExit, match="Invalid argument"):
        main(["dummy", "/tmp"])


def test_extract_archive(tmp_path: Path):

    folder1 = tmp_path / "folder1"
    folder1.mkdir()
    with (folder1 / "file1").open("w") as f:
        f.write("First version\n")
    compress_folder(folder1)

    archive_path = tmp_path / "archive.tar.gz"
    Path(tmp_path / "folder1.tar.gz").rename(archive_path)
    extract_archive(archive_path)
    debug(list((tmp_path / "archive").glob("*")))

    assert (tmp_path / "archive").is_dir()
    assert (tmp_path / "archive/file1").exists()
    assert (tmp_path / "archive/file1").is_file()

    archive_path.unlink()

    folder2 = tmp_path / "folder2"
    folder2.mkdir()
    with (folder2 / "file1").open("w") as f:
        f.write("Second version\n")
    (folder2 / "file2").touch()
    compress_folder(folder2)

    archive_path = tmp_path / "archive.tar.gz"
    Path(tmp_path / "folder2.tar.gz").rename(archive_path)
    extract_archive(archive_path)
    debug(list((tmp_path / "archive").glob("*")))

    assert (tmp_path / "archive").is_dir()
    assert (tmp_path / "archive/file1").is_file()
    assert (tmp_path / "archive/file2").is_file()
    with (tmp_path / "archive/file1").open("r") as f:
        content = f.read()
    assert content == "Second version\n"
