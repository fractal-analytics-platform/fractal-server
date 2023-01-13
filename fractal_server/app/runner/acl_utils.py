import os
from pathlib import Path

from devtools import debug

from ...config import get_settings
from ...syringe import Inject
from ...utils import execute_command


def _wrap_setfacl(folder: Path, user: str):
    # NOTE: this function may also be used somewhere else, in the cfut executor

    cwd = os.getcwd()
    current_user = os.getlogin()
    command = (
        f"setfacl -m u:{user}:rwx,u:{current_user}:rwx" f" {folder.as_posix()}"
    )

    debug(current_user)
    debug(cwd)
    debug(command)
    execute_command(cwd=cwd, command=command)


def _wrap_nfs_setfacl(folder: Path, user: str):
    raise NotImplementedError()


def mkdir_with_acl(folder: Path, *, user: str):
    """
    TBD
    """

    # Always create the folder
    if folder.exists():
        raise ValueError(f"{str(folder)} already exists.")
    folder.mkdir(parents=True)

    # Apply permissions
    settings = Inject(get_settings)
    FRACTAL_ACL_OPTIONS = settings.FRACTAL_ACL_OPTIONS
    if FRACTAL_ACL_OPTIONS == "none":
        return
    if FRACTAL_ACL_OPTIONS == "standard":
        _wrap_setfacl(folder=folder, user=user)
        return
    elif FRACTAL_ACL_OPTIONS == "nfs4":
        _wrap_nfs_setfacl(folder=folder, user=user)
    else:
        raise ValueError(f"{FRACTAL_ACL_OPTIONS=} not supported")
