import os
import shlex
import subprocess
from pathlib import Path
from typing import Sequence

from devtools import debug

from ...config import get_settings
from ...syringe import Inject
from ...utils import execute_command


def _execute_command(cmd: str):
    cmd_split = shlex.split(cmd)
    res = subprocess.run(
        cmd_split, encoding="utf-8", check=True, capture_output=True
    )
    debug(res)
    print(res.stdout)
    print()
    print(res.stderr)
    print()
    assert res.returncode == 0


def _wrap_posix_setfacl(folder: Path, users: Sequence[str]):
    _execute_command(f"setfacl -b {folder}")
    for user in users:
        _execute_command(
            f"setfacl --modify user:{user}:rwx,group::---,other::--- {str(folder)}"
        )
    _execute_command(f"getfacl -p {folder}")


def _wrap_nfs_setfacl(folder: Path, user: str):
    raise NotImplementedError()


def mkdir_with_acl(folder: Path, *, workflow_user: str):
    """
    TBD
    """

    # Always create the folder, and make it 700
    if folder.exists():
        raise ValueError(f"{str(folder)} already exists.")
    # folder.mkdir(parents=True)
    # folder.chmod(0o700)
    _execute_command(f"mkdir -p {str(folder)}")
    _execute_command(f"ls -la {str(folder)}")
    _execute_command(f"chmod 700 {str(folder)}")
    _execute_command(f"ls -la {str(folder)}")

    current_user = os.getlogin()

    # Apply permissions
    settings = Inject(get_settings)
    FRACTAL_ACL_OPTIONS = settings.FRACTAL_ACL_OPTIONS
    if FRACTAL_ACL_OPTIONS == "none":
        return
    if FRACTAL_ACL_OPTIONS == "standard":  # FIXME say posix
        _wrap_posix_setfacl(
            folder, users=[current_user, workflow_user, "fractal"]
        )
    else:
        raise ValueError(f"{FRACTAL_ACL_OPTIONS=} not supported")
