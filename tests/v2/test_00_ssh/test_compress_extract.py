import shutil
import subprocess
from pathlib import Path

import pytest

from fractal_server.app.runner.executors.slurm_ssh.run_subprocess import (
    run_subprocess,
)
from fractal_server.app.runner.executors.slurm_ssh.tar_commands import (
    get_tar_compression_cmd,
)
from fractal_server.app.runner.executors.slurm_ssh.tar_commands import (
    get_tar_extraction_cmd,
)


def _compress_folder(subfolder_path: Path, filelist_path: Path | None):
    """
    This function simulates the typical usage of `get_tar_compression_cmd`.
    """
    tar_cmd = get_tar_compression_cmd(
        subfolder_path=subfolder_path,
        filelist_path=filelist_path,
    )
    run_subprocess(tar_cmd)


def _extract_archive(tarfile_path_local: Path):
    """
    This function simulates the typical usage of `get_tar_extraction_cmd`.
    """
    target_dir, cmd_tar = get_tar_extraction_cmd(Path(tarfile_path_local))
    Path(target_dir).mkdir(exist_ok=True)
    run_subprocess(cmd=cmd_tar)


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
    _compress_folder(subfolder_path, filelist_path=None)
    shutil.copy(tarfile_path, new_tarfile_path)
    _extract_archive(new_tarfile_path)

    assert tarfile_path.exists()
    assert new_tarfile_path.exists()
    assert (extracted_path / "subfolder/file1.txt").exists()
    assert (extracted_path / "subfolder/file2.txt").exists()
    assert (extracted_path / "subfolder/job.sbatch").exists()
    assert (extracted_path / "subfolder/file_in_name.pickle").exists()

    # Create new file
    (subfolder_path / "file3.txt").write_text("File 2")

    # Second run (with overwrite)
    assert tarfile_path.exists()
    _compress_folder(subfolder_path, filelist_path=None)
    shutil.copy(tarfile_path, new_tarfile_path)
    _extract_archive(new_tarfile_path)

    assert tarfile_path.exists()
    assert new_tarfile_path.exists()
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
    _compress_folder(subfolder_path, filelist_path=Path(filelist_path))
    shutil.copy(tarfile_path, new_tarfile_path)
    _extract_archive(new_tarfile_path)

    assert tarfile_path.exists()
    assert new_tarfile_path.exists()
    assert (extracted_path / "subfolder/file1.txt").exists()
    assert (extracted_path / "subfolder/file2.txt").exists()
    assert not (extracted_path / "subfolder/job.sbatch").exists()
    assert not (extracted_path / "subfolder/file_in_name.pickle").exists()

    # Create new file and update filelist
    (subfolder_path / "file3.txt").write_text("File 2")
    with open(filelist_path, "a") as f:
        f.write("file3.txt\n")

    # Second run (with overwrite)
    _compress_folder(subfolder_path, filelist_path=Path(filelist_path))
    shutil.copy(tarfile_path, new_tarfile_path)
    _extract_archive(new_tarfile_path)

    assert tarfile_path.exists()
    assert new_tarfile_path.exists()
    assert (extracted_path / "subfolder/file1.txt").exists()
    assert (extracted_path / "subfolder/file2.txt").exists()
    assert (extracted_path / "subfolder/file3.txt").exists()
    assert not (extracted_path / "subfolder/job.sbatch").exists()
    assert not (extracted_path / "subfolder/file_in_name.pickle").exists()


def test_compress_folder_failure(tmp_path: Path):
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        _compress_folder(tmp_path / "something", filelist_path=None)
    print(exc_info.value)


def test_extract_archive_failure(tmp_path: Path):
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        _extract_archive(tmp_path / "missing.tar.gz")
    print(exc_info.value)

    with pytest.raises(ValueError, match="must end with"):
        _extract_archive(tmp_path / "wrong.suffix")
