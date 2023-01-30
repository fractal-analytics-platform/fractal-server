import shlex
import subprocess

from devtools import debug


def test__subprocess_run_ls(tmp_path):
    # Test a trivial _subprocess_run_ls, in view of its use within fractal
    # (with a sudo -u in front)

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

    # ls: cannot access '...': No such file or directory
    filepath = str(tmp_path / "missing-file-in-existing-folder")
    res = _subprocess_run_ls(filepath)
    assert res.returncode != 0

    # ls: cannot access '...': No such file or directory
    filepath = str(tmp_path / "missing-folder/missing-file-in-missing-folder")
    res = _subprocess_run_ls(filepath)
    assert res.returncode != 0

    # Permission denied
    filepath = "/root"
    res = _subprocess_run_ls(filepath)
    assert res.returncode != 0
