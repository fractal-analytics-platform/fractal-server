import getpass
import logging
import os
import shlex
import subprocess
from pathlib import Path
from typing import Optional

from devtools import debug

from ...config import get_settings
from ...syringe import Inject


def _execute_command(cmd: str):
    debug(cmd)
    res = subprocess.run(
        shlex.split(cmd), encoding="utf-8", capture_output=True
    )
    debug(res)
    print(res.stdout)
    print()
    print(res.stderr)
    print()
    if res.returncode != 0:
        raise RuntimeError(str(res))


def _wrap_posix_setfacl(folder: Path, current_user: str, workflow_user: str):
    _execute_command(f"setfacl -b {folder}")
    _execute_command(
        "setfacl --default --recursive --modify "
        f"user:{current_user}:rwx,user:{workflow_user}:rwx,"
        f"group::---,other::--- {folder}"
    )


def _wrap_nfs_setfacl(folder: Path, user: str):
    raise NotImplementedError()


def mkdir_with_acl(
    folder: Path, *, workflow_user: str, acl_options: Optional[str] = None
):
    """
    TBD
    """
    # Preliminary check
    if folder.exists():
        raise ValueError(f"{str(folder)} already exists.")

    # Create the folder, and make it 700
    folder.mkdir(parents=True, mode=0o700)

    # Apply permissions
    if not acl_options:
        settings = Inject(get_settings)
        acl_options = settings.FRACTAL_ACL_OPTIONS
    if acl_options == "none":
        return
    elif acl_options == "posix":
        current_user = getpass.getuser()
        logging.info(f"{current_user=}")
        current_user = str(os.getuid())
        logging.info(f"{current_user=}")
        _wrap_posix_setfacl(
            folder, current_user=current_user, workflow_user=workflow_user
        )
    else:
        raise ValueError(f"{acl_options=} not supported")
