import shlex
import subprocess

import pytest
from devtools import debug

from fractal_server.app.runner.executors.slurm_sudo._subprocess_run_as_user import (  # noqa: E501
    _glob_as_user,
)
from fractal_server.app.runner.executors.slurm_sudo._subprocess_run_as_user import (  # noqa: E501
    _glob_as_user_strict,
)
from fractal_server.app.runner.executors.slurm_sudo._subprocess_run_as_user import (  # noqa: E501
    _path_exists_as_user,
)


def test_glob_as_user(tmp_path):
    """
    Test _glob_as_user (but without impersonating another user)
    """

    empty_dir = tmp_path / "empty_folder"
    missing_dir = tmp_path / "missing_folder"
    empty_dir.mkdir()
    for fname in ["1.txt", "1.png", "2.asd"]:
        with (tmp_path / fname).open("w") as f:
            f.write("\n")

    # All files/folders from a folder
    files = _glob_as_user(
        folder=str(tmp_path),
        user=None,
    )
    debug(files)
    assert len(files) == 4

    # A subset of files/folders from a folder
    files = _glob_as_user(folder=str(tmp_path), user=None, startswith="1")
    debug(files)
    assert len(files) == 2

    # Empty folder
    files = _glob_as_user(
        folder=str(empty_dir),
        user=None,
    )
    debug(files)
    assert not files

    # Missing folder
    with pytest.raises(RuntimeError):
        files = _glob_as_user(
            folder=str(missing_dir),
            user=None,
        )


def test_glob_as_user_strict(tmp_path):
    """
    Test _glob_as_user_strict
    """

    empty_dir = tmp_path / "empty_folder"
    empty_dir.mkdir()
    file_list = [
        "1.err",
        "1.out",
        "1.metadiff.json",
        "1.args.json",
        "1_out_AAAAA.pickle",
        "11.err",
        "11.out",
        "11.metadiff.json",
        "11.args.json",
        "11_out_BBBBB.pickle",
        "11_out_BBBBB.pickle.tmp",
    ]
    for fname in file_list:
        with (tmp_path / fname).open("w") as f:
            f.write("\n")

    # All files/folders from a folder
    files = _glob_as_user_strict(
        folder=str(tmp_path), user=None, startswith="1"
    )
    debug(files)
    assert len(files) == 5


def test_path_exists_as_user(tmp_path):
    """
    Test _path_exists_as_user function (but without impersonating another user)

    Note: we also use a local _subprocess_run_ls function, just to highlight
    the different test cases addressed here.
    """

    def _subprocess_run_ls(path: str):
        debug(path)
        res = subprocess.run(
            shlex.split(f"ls {path}"), capture_output=True, encoding="utf-8"
        )
        debug(res.returncode)
        debug(res.stdout)
        debug(res.stderr)
        return res

    with (tmp_path / "1").open("w") as f:
        f.write("something\n")

    # OK
    filepath = str(tmp_path / "1")
    res = _subprocess_run_ls(filepath)
    assert res.returncode == 0
    assert _path_exists_as_user(path=filepath, user=None)

    # ls: cannot access '...': No such file or directory
    filepath = str(tmp_path / "missing-file-in-existing-folder")
    res = _subprocess_run_ls(filepath)
    assert res.returncode != 0
    assert not _path_exists_as_user(path=filepath, user=None)

    # ls: cannot access '...': No such file or directory
    filepath = str(tmp_path / "missing-folder/missing-file-in-missing-folder")
    res = _subprocess_run_ls(filepath)
    assert res.returncode != 0
    assert not _path_exists_as_user(path=filepath, user=None)

    # Permission denied
    filepath = "/root"
    res = _subprocess_run_ls(filepath)
    assert res.returncode != 0
    assert not _path_exists_as_user(path=filepath, user=None)
