import logging
import subprocess
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from fractal_server.app.runner.compress_folder import compress_folder
from fractal_server.app.runner.compress_folder import copy_subfolder
from fractal_server.app.runner.compress_folder import remove_temp_subfolder
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
    create_test_files(subfolder_path)
    compress_folder(subfolder_path)

    tarfile_path = Path(f"{tmp_path}/subfolder.tar.gz")
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


def test_run_subprocess_other_exception(caplog):

    caplog.set_level(logging.DEBUG)

    with pytest.raises(Exception):
        run_subprocess("/bin/test_no_cmd", logger_name=logger.name)

    assert any(
        record.message
        == ("An error occurred while running command: " "/bin/test_no_cmd")
        for record in caplog.records
    )


def test_compress_folder_tar_failure(tmp_path, caplog):
    subfolder_path = tmp_path / "subfolder"
    create_test_files(subfolder_path)
    module = "fractal_server.app.runner.compress_folder."
    with patch(f"{module}copy_subfolder") as mock_copy_subfolder, patch(
        f"{module}create_tar_archive"
    ) as mock_create_tar_archive, patch(
        f"{module}remove_temp_subfolder"
    ) as mock_remove_temp_subfolder:

        mock_copy_subfolder.side_effect = copy_subfolder
        mock_create_tar_archive.return_value = MagicMock(
            returncode=1, stderr="Mocked error"
        )
        mock_remove_temp_subfolder.side_effect = remove_temp_subfolder
        caplog.set_level(logging.DEBUG)

        with pytest.raises(SystemExit):
            compress_folder(subfolder_path)

            assert "START" in caplog.text
            assert f"{subfolder_path=}" in caplog.text
            assert "tarfile_path=" in caplog.text
            assert "Copying from" in caplog.text
            assert "Creating tar archive at" in caplog.text
            assert "ERROR: Error in tar command: Mocked error" in caplog.text
            assert "Removing temporary subfolder" in caplog.text
            assert "shutil.rmtree END" in caplog.text
            assert any(
                record.levelname == "ERROR" for record in caplog.records
            )

    invalid_subfolder_path = Path(f"{tmp_path} / non_existent_subfolder")

    with pytest.raises(FileNotFoundError):
        compress_folder(invalid_subfolder_path)
    tarfile_path = Path(
        f"{invalid_subfolder_path} / non_existent_subfolder.tar.gz"
    )
    assert not tarfile_path.exists()
