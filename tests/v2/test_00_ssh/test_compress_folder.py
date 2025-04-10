from pathlib import Path

import pytest

from fractal_server.app.runner.compress_folder import compress_folder
from fractal_server.app.runner.compress_folder import main
from fractal_server.app.runner.run_subprocess import run_subprocess


def create_test_files(path: Path):
    path.mkdir()
    (path / "file1.txt").write_text("File 1")
    (path / "file2.txt").write_text("File 2")
    (path / "job.sbatch").write_text("Exclude this file")
    (path / "file_in_name.pickle").write_text("Exclude this pickle")


def test_compress_folder_success_local_to_remote(tmp_path):
    subfolder_path = Path(f"{tmp_path}/subfolder")
    create_test_files(subfolder_path)

    compress_folder(subfolder_path, filelist_path=None)

    tarfile_path = Path(f"{tmp_path}/subfolder.tar.gz")
    assert tarfile_path.exists()
    assert not Path(f"{subfolder_path.name}_copy").exists()
    assert tarfile_path.exists()

    extracted_path = Path(f"{tmp_path}/extracted")
    extracted_path.mkdir()

    run_subprocess(f"tar xzf {tarfile_path} -C {extracted_path}")
    assert (extracted_path / "file1.txt").exists()
    assert (extracted_path / "file2.txt").exists()
    assert (extracted_path / "job.sbatch").exists()
    assert (extracted_path / "file_in_name.pickle").exists()


@pytest.mark.xfail
def test_compress_folder_success_with_filelist(tmp_path):  # FIXME missing
    subfolder_path = Path(f"{tmp_path}/subfolder")
    create_test_files(subfolder_path)

    compress_folder(subfolder_path, filelist_path=None)

    tarfile_path = Path(f"{tmp_path}/subfolder.tar.gz")
    assert tarfile_path.exists()
    assert not Path(f"{subfolder_path.name}_copy").exists()
    assert tarfile_path.exists()

    extracted_path = Path(f"{tmp_path}/extracted")
    extracted_path.mkdir()

    run_subprocess(f"tar xzf {tarfile_path} -C {extracted_path}")
    assert (extracted_path / "file1.txt").exists()
    assert (extracted_path / "file2.txt").exists()
    assert not (extracted_path / "job.sbatch").exists()
    assert not (extracted_path / "file_in_name.pickle").exists()


def test_compress_folder_success_with_overwrite(tmp_path):
    subfolder_path = Path(f"{tmp_path}/subfolder")
    create_test_files(subfolder_path)
    compress_folder(subfolder_path, filelist_path=None)
    tarfile_path = Path(f"{tmp_path}/subfolder.tar.gz")
    assert tarfile_path.exists()

    extracted_path = Path(f"{tmp_path}/extracted")
    extracted_path.mkdir()
    run_subprocess(f"tar xzf {tarfile_path} -C {extracted_path}")
    assert (extracted_path / "file1.txt").exists()
    assert (extracted_path / "file2.txt").exists()
    assert not (extracted_path / "file3.txt").exists()

    (subfolder_path / "file3.txt").write_text("File 3")
    compress_folder(subfolder_path, filelist_path=None)
    extracted_path_new = Path(f"{tmp_path}/extracted_new")
    extracted_path_new.mkdir()
    run_subprocess(f"tar xzf {tarfile_path} -C {extracted_path_new}")
    assert (extracted_path_new / "file1.txt").exists()
    assert (extracted_path_new / "file2.txt").exists()
    assert (extracted_path_new / "file3.txt").exists()


def test_compress_folder_tar_failure(tmp_path):

    subfolder_path = tmp_path / "subfolder"
    create_test_files(subfolder_path)

    invalid_subfolder_path = Path(f"{tmp_path} / non_existent_subfolder")

    with pytest.raises(SystemExit):
        compress_folder(invalid_subfolder_path, filelist_path=None)
    tarfile_path = Path(
        f"{invalid_subfolder_path} / non_existent_subfolder.tar.gz"
    )
    assert not tarfile_path.exists()


def test_main_success(tmp_path):
    subfolder_path = tmp_path / "subfolder"
    create_test_files(subfolder_path)
    test_argv = ["compress_folder", str(subfolder_path)]
    main(test_argv)


def test_main_invalid_arguments():

    with pytest.raises(SystemExit):
        main([])

    with pytest.raises(SystemExit):
        main(["missing_command"])

    with pytest.raises(SystemExit):
        main(["compress_folder"])

    with pytest.raises(SystemExit):
        main(["compress_folder", "arg1", "arg2"])

    with pytest.raises(SystemExit):
        main(["compress_folder", "arg1", "arg2", "arg3"])
