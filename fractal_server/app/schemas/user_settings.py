from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import validator
from pydantic.types import StrictStr

from ._validators import val_absolute_path
from ._validators import val_unique_list
from ._validators import valstr
from fractal_server.string_tools import validate_cmd

__all__ = (
    "UserSettingsRead",
    "UserSettingsReadStrict",
    "UserSettingsUpdate",
    "UserSettingsUpdateStrict",
)


class UserSettingsRead(BaseModel):
    """
    Schema reserved for superusers
    """

    id: int
    ssh_host: Optional[str] = None
    ssh_username: Optional[str] = None
    ssh_private_key_path: Optional[str] = None
    ssh_tasks_dir: Optional[str] = None
    ssh_jobs_dir: Optional[str] = None
    slurm_user: Optional[str] = None
    slurm_accounts: list[str]
    cache_dir: Optional[str] = None
    project_dir: Optional[str] = None


class UserSettingsReadStrict(BaseModel):
    slurm_user: Optional[str] = None
    slurm_accounts: list[str]
    cache_dir: Optional[str] = None
    ssh_username: Optional[str] = None
    project_dir: Optional[str] = None


class UserSettingsUpdate(BaseModel, extra=Extra.forbid):
    """
    Schema reserved for superusers
    """

    ssh_host: Optional[str] = None
    ssh_username: Optional[str] = None
    ssh_private_key_path: Optional[str] = None
    ssh_tasks_dir: Optional[str] = None
    ssh_jobs_dir: Optional[str] = None
    slurm_user: Optional[str] = None
    slurm_accounts: Optional[list[StrictStr]] = None
    cache_dir: Optional[str] = None
    project_dir: Optional[str] = None

    _ssh_host = validator("ssh_host", allow_reuse=True)(
        valstr("ssh_host", accept_none=True)
    )
    _ssh_username = validator("ssh_username", allow_reuse=True)(
        valstr("ssh_username", accept_none=True)
    )
    _ssh_private_key_path = validator(
        "ssh_private_key_path", allow_reuse=True
    )(val_absolute_path("ssh_private_key_path", accept_none=True))

    _ssh_tasks_dir = validator("ssh_tasks_dir", allow_reuse=True)(
        val_absolute_path("ssh_tasks_dir", accept_none=True)
    )
    _ssh_jobs_dir = validator("ssh_jobs_dir", allow_reuse=True)(
        val_absolute_path("ssh_jobs_dir", accept_none=True)
    )

    _slurm_user = validator("slurm_user", allow_reuse=True)(
        valstr("slurm_user", accept_none=True)
    )

    @validator("slurm_accounts")
    def slurm_accounts_validator(cls, value):
        if value is None:
            return value
        for i, item in enumerate(value):
            value[i] = valstr(f"slurm_accounts[{i}]")(item)
        return val_unique_list("slurm_accounts")(value)

    @validator("cache_dir")
    def cache_dir_validator(cls, value):
        if value is None:
            return None
        validate_cmd(value)
        return val_absolute_path("cache_dir")(value)

    @validator("project_dir")
    def project_dir_validator(cls, value):
        if value is None:
            return None
        validate_cmd(value)
        return val_absolute_path("project_dir")(value)


class UserSettingsUpdateStrict(BaseModel, extra=Extra.forbid):
    slurm_accounts: Optional[list[StrictStr]] = None
    cache_dir: Optional[str] = None

    _slurm_accounts = validator("slurm_accounts", allow_reuse=True)(
        val_unique_list("slurm_accounts")
    )

    @validator("cache_dir")
    def cache_dir_validator(cls, value):
        if value is None:
            return value
        validate_cmd(value)
        return val_absolute_path("cache_dir")(value)
