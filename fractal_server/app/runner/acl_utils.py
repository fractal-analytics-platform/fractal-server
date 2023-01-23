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
        shlex.split(cmd),
        encoding="utf-8",
        capture_output=True,
        # check=True, #FIXME: put this back, after debugging
    )
    debug(res)
    print(res.stdout)
    print()
    print(res.stderr)
    print()
    if res.returncode != 0:
        raise RuntimeError(str(res))
    return res.stdout


def _wrap_posix_setfacl(folder: Path, current_user: str, workflow_user: str):
    """
    Set ACL for a folder to be rwx-accessible to only two users

    Arguments:
        folder: TBD
        current_user: TBD
        workflow_user: TBD

    """
    acl_set = (
        f"user:{current_user}:rwx,"
        f"default:user:{current_user}:rwx,"
        f"user:{workflow_user}:rwx,"
        f"default:user:{workflow_user}:rwx,"
        "group::---,default:group::---,"
        "other::---,default:other::---,"
        "mask::rwx,default:mask::rwx"
    )
    _execute_command(f"setfacl -b {folder}")
    _execute_command(f"setfacl --recursive --modify {acl_set} {folder}")


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

    # FIXME if needed, create parent
    if not folder.parent.exists():
        folder.parent.mkdir(parents=True, mode=0o711)

    # Create the folder, and make it 700
    folder.mkdir(mode=0o700)

    # Apply permissions
    if not acl_options:
        settings = Inject(get_settings)
        acl_options = settings.FRACTAL_ACL_OPTIONS
    if acl_options == "none":
        return
    elif acl_options == "posix":
        current_user = str(os.getuid())
        logging.info(f"{current_user=}")
        _wrap_posix_setfacl(
            folder, current_user=current_user, workflow_user=workflow_user
        )
    else:
        raise ValueError(f"{acl_options=} not supported")
