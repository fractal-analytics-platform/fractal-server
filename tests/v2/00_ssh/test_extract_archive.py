from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.runner.compress_folder import compress_folder
from fractal_server.app.runner.extract_archive import extract_archive
from fractal_server.app.runner.extract_archive import main


def create_test_files(path: Path):
    path.mkdir()
    (path / "file1.txt").write_text("File 1")
    (path / "file2.txt").write_text("File 2")
    (path / "job.sbatch").write_text("Exclude this file")
    (path / "file_in_name.pickle").write_text("Exclude this pickle")


def test_main_success(tmp_path):
    tarfile_path = Path(f"{tmp_path}/subfolder.tar.gz")
    subfolder_path = tmp_path / "subfolder"
    create_test_files(subfolder_path)
    compress_folder(subfolder_path)
    test_argv = ["extract_archive", str(tarfile_path)]
    main(test_argv)


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


def test_extract_archive_failure(tmp_path: Path):
    with pytest.raises(SystemExit, match="Missing file"):
        extract_archive(tmp_path / "missing.tar.gz")


def test_extract_archive(tmp_path: Path):
    # Create two archives
    folder_A = tmp_path / "folder_A"
    folder_B = tmp_path / "folder_B"
    folder_A.mkdir()
    folder_B.mkdir()
    with (folder_A / "file1").open("w") as f:
        f.write("Version A of file1\n")
    with (folder_B / "file1").open("w") as f:
        f.write("Version B of file1\n")
    (folder_B / "file2").touch()
    compress_folder(folder_A)
    compress_folder(folder_B)

    archive_path = tmp_path / "archive.tar.gz"

    # Extract archive of folder_A into `archive` folder
    Path(tmp_path / "folder_A.tar.gz").rename(archive_path)
    extract_archive(archive_path)
    debug(list((tmp_path / "archive").glob("*")))

    # Verify output of first archive extraction
    assert (tmp_path / "archive").is_dir()
    assert (tmp_path / "archive/file1").exists()
    file1_content = (tmp_path / "archive/file1").open("r").read()
    assert file1_content == "Version A of file1\n"

    archive_path.unlink()

    # Extract archive of folder_B into existing `archive` folder
    Path(tmp_path / "folder_B.tar.gz").rename(archive_path)
    extract_archive(archive_path)
    debug(list((tmp_path / "archive").glob("*")))

    # Verify output of secomd archive extraction
    assert (tmp_path / "archive").is_dir()
    assert (tmp_path / "archive/file1").exists()
    assert (tmp_path / "archive/file2").exists()
    file1_content = (tmp_path / "archive/file1").open("r").read()
    assert file1_content == "Version B of file1\n"
