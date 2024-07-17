import logging
import subprocess
from pathlib import Path

import pytest

from fractal_server.app.runner.compress_folder import compress_folder
from fractal_server.app.runner.compress_folder import run_subprocess

logger = logging.getLogger(__name__)


def create_test_files(path: Path):
    path.mkdir()
    Path(path / "file1.txt").write_text("File 1")
    Path(path / "file2.txt").write_text("File 2")
    Path(path / "job.sbatch").write_text("Exclude this file")
    Path(path / "file_in_name.pickle").write_text("Exclude this pickle")


def test_compress_folder_success(tmp_path):
    subfolder_path = Path(f"{tmp_path}/subfolder")
    tarfile_path = Path(f"{tmp_path}/archive.tar.gz")
    create_test_files(subfolder_path)
    compress_folder(subfolder_path, tarfile_path, logger_name=logger.name)

    assert tarfile_path.exists()
    assert not Path(f"{subfolder_path.name}_copy").exists()
    assert tarfile_path.exists()

    extracted_path = Path(f"{tmp_path}/extracted")
    extracted_path.mkdir()

    subprocess.run(["tar", "xzf", tarfile_path, "-C", extracted_path])
    assert (extracted_path / "file1.txt").exists()
    assert (extracted_path / "file2.txt").exists()
    assert not (extracted_path / "job.sbatch").exists()
    assert not (extracted_path / "file.pickle").exists()


def test_compress_folder_tar_failure(tmp_path):
    tarfile_path = Path(f"{tmp_path}/archive.tar.gz")

    invalid_subfolder_path = Path(f"{tmp_path} / non_existent_subfolder")

    with pytest.raises(FileNotFoundError):
        compress_folder(
            invalid_subfolder_path, tarfile_path, logger_name=logger.name
        )

    assert not tarfile_path.exists()


def test_run_subprocess_other_exception(caplog):

    caplog.set_level(logging.DEBUG)

    with pytest.raises(Exception):
        run_subprocess("/bin/test_no_cmd", logger_name=logger.name)

    assert any(
        record.message
        == ("An error occurred while running command: " "/bin/test_no_cmd")
        for record in caplog.records
    )
