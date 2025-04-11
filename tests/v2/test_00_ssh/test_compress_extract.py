import shutil
from pathlib import Path

import pytest

from fractal_server.app.runner.compress_folder import compress_folder
from fractal_server.app.runner.compress_folder import main as main_compress
from fractal_server.app.runner.extract_archive import extract_archive
from fractal_server.app.runner.extract_archive import main as main_extract


def create_test_files(path: Path):
    path.mkdir()
    (path / "file1.txt").write_text("File 1")
    (path / "file2.txt").write_text("File 2")
    (path / "job.sbatch").write_text("Exclude this file")
    (path / "file_in_name.pickle").write_text("Exclude this pickle")


def test_compress_and_extract_without_filelist(tmp_path: Path):
    subfolder_path = Path(f"{tmp_path}/subfolder")
    create_test_files(subfolder_path)
    extracted_path = Path(f"{tmp_path}/extracted")
    (extracted_path / "subfolder").mkdir(parents=True)
    tarfile_path = Path(f"{tmp_path}/subfolder.tar.gz")
    new_tarfile_path = extracted_path / "subfolder.tar.gz"

    # First run (without overwrite)
    compress_folder(subfolder_path, filelist_path=None)
    shutil.copy(tarfile_path, new_tarfile_path)
    extract_archive(new_tarfile_path)

    assert tarfile_path.exists()
    assert new_tarfile_path.exists()
    assert not Path(f"{subfolder_path.name}_copy").exists()
    assert (extracted_path / "subfolder/file1.txt").exists()
    assert (extracted_path / "subfolder/file2.txt").exists()
    assert (extracted_path / "subfolder/job.sbatch").exists()
    assert (extracted_path / "subfolder/file_in_name.pickle").exists()

    # Create new file
    (subfolder_path / "file3.txt").write_text("File 2")

    # Second run (with overwrite)
    compress_folder(subfolder_path, filelist_path=None)
    shutil.copy(tarfile_path, new_tarfile_path)
    extract_archive(new_tarfile_path)

    assert tarfile_path.exists()
    assert new_tarfile_path.exists()
    assert not Path(f"{subfolder_path.name}_copy").exists()
    assert (extracted_path / "subfolder/file1.txt").exists()
    assert (extracted_path / "subfolder/file2.txt").exists()
    assert (extracted_path / "subfolder/file3.txt").exists()
    assert (extracted_path / "subfolder/job.sbatch").exists()
    assert (extracted_path / "subfolder/file_in_name.pickle").exists()


def test_compress_and_extract_with_filelist(tmp_path: Path):

    subfolder_path = Path(f"{tmp_path}/subfolder")
    create_test_files(subfolder_path)
    extracted_path = Path(f"{tmp_path}/extracted")
    new_tarfile_path = extracted_path / "subfolder.tar.gz"
    (extracted_path / "subfolder").mkdir(parents=True)
    tarfile_path = Path(f"{tmp_path}/subfolder.tar.gz")

    filelist_path = (subfolder_path / "filelist.txt").as_posix()
    with open(filelist_path, "w") as f:
        f.write("file1.txt\n")
        f.write("file2.txt\n")
        f.write("missing.txt\n")

    # First run (without overwrite)
    compress_folder(subfolder_path, filelist_path=filelist_path)
    shutil.copy(tarfile_path, new_tarfile_path)
    extract_archive(new_tarfile_path)

    assert tarfile_path.exists()
    assert new_tarfile_path.exists()
    assert not Path(f"{subfolder_path.name}_copy").exists()
    assert (extracted_path / "subfolder/file1.txt").exists()
    assert (extracted_path / "subfolder/file2.txt").exists()
    assert not (extracted_path / "subfolder/job.sbatch").exists()
    assert not (extracted_path / "subfolder/file_in_name.pickle").exists()

    # Create new file and update filelist
    (subfolder_path / "file3.txt").write_text("File 2")
    with open(filelist_path, "a") as f:
        f.write("file3.txt\n")

    # Second run (with overwrite)
    compress_folder(subfolder_path, filelist_path=filelist_path)
    shutil.copy(tarfile_path, new_tarfile_path)
    extract_archive(new_tarfile_path)

    assert tarfile_path.exists()
    assert new_tarfile_path.exists()
    assert not Path(f"{subfolder_path.name}_copy").exists()
    assert (extracted_path / "subfolder/file1.txt").exists()
    assert (extracted_path / "subfolder/file2.txt").exists()
    assert (extracted_path / "subfolder/file3.txt").exists()
    assert not (extracted_path / "subfolder/job.sbatch").exists()
    assert not (extracted_path / "subfolder/file_in_name.pickle").exists()


def test_compress_folder_failure(tmp_path: Path):

    invalid_subfolder_path = tmp_path / "non_existent_subfolder"
    tarfile_path = tmp_path / "non_existent_subfolder.tar.gz"

    with pytest.raises(SystemExit):
        compress_folder(invalid_subfolder_path, filelist_path=None)
    assert not tarfile_path.exists()


def test_extract_archive_failure(tmp_path: Path):
    with pytest.raises(SystemExit, match="Missing file"):
        extract_archive(tmp_path / "missing.tar.gz")


def test_main_compress_success_without_filelist(tmp_path: Path):
    subfolder_path = tmp_path / "subfolder"
    create_test_files(subfolder_path)
    test_argv = ["compress_folder", subfolder_path.as_posix()]
    main_compress(test_argv)


def test_main_compress_success_with_filelist(tmp_path: Path):
    subfolder_path = tmp_path / "subfolder"
    create_test_files(subfolder_path)
    filelist_path = (subfolder_path / "filelist.txt").as_posix()
    with open(filelist_path, "w") as f:
        f.write("file1.txt\n")
        f.write("file2.txt\n")
        f.write("missing.txt\n")
    test_argv = [
        "compress_folder",
        subfolder_path.as_posix(),
        "--filelist",
        filelist_path,
    ]
    main_compress(test_argv)


def test_main_extract_success(tmp_path: Path):
    tarfile_path = Path(f"{tmp_path}/subfolder.tar.gz")
    subfolder_path = tmp_path / "subfolder"
    create_test_files(subfolder_path)
    compress_folder(subfolder_path, filelist_path=None)
    test_argv = ["extract_archive", tarfile_path.as_posix()]
    main_extract(test_argv)


def test_main_compress_invalid_arguments():

    with pytest.raises(SystemExit):
        main_compress([])

    with pytest.raises(SystemExit):
        main_compress(["compress_folder"])

    with pytest.raises(SystemExit):
        main_compress(["compress_folder", "/path", "arg2"])

    with pytest.raises(SystemExit):
        main_compress(["compress_folder", "/path", "arg2", "arg3"])


def test_main_extract_invalid_arguments():
    with pytest.raises(SystemExit):
        main_extract([])
    with pytest.raises(SystemExit, match="Invalid argument"):
        main_extract(["extract_archive"])
    with pytest.raises(SystemExit, match="Invalid argument"):
        main_extract(["extract_archive", "/arg1.tar.gz", "/arg2.tar.gz"])
    with pytest.raises(SystemExit, match="Invalid argument"):
        main_extract(["extract_archive", "/tmp.txt"])
