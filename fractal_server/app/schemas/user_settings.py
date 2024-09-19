from typing import Optional

from pydantic import BaseModel
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
    id: int
    # SSH
    ssh_host: Optional[str]
    ssh_username: Optional[str]
    ssh_private_key_path: Optional[str]
    ssh_tasks_dir: Optional[str]
    ssh_jobs_dir: Optional[str]
    # Slurm
    slurm_user: Optional[str]
    slurm_accounts: list[str]
    # Cache
    cache_dir: Optional[str]


class UserSettingsReadStrict(BaseModel):
    # Slurm
    slurm_user: Optional[str]
    slurm_accounts: list[str]
    # Cache
    cache_dir: Optional[str]


class UserSettingsUpdate(BaseModel):
    # SSH
    ssh_host: Optional[str]
    ssh_username: Optional[str]
    ssh_private_key_path: Optional[str]
    ssh_tasks_dir: Optional[str]
    ssh_jobs_dir: Optional[str]
    # Slurm
    slurm_user: Optional[str]
    slurm_accounts: Optional[list[StrictStr]]
    # Cache
    cache_dir: Optional[str]

    _ssh_host = validator("ssh_host", allow_reuse=True)(valstr("ssh_host"))
    _ssh_username = validator("ssh_username", allow_reuse=True)(
        valstr("ssh_username")
    )
    _ssh_private_key_path = validator(
        "ssh_private_key_path", allow_reuse=True
    )(val_absolute_path("ssh_private_key_path"))

    _ssh_tasks_dir = validator("ssh_tasks_dir", allow_reuse=True)(
        val_absolute_path("ssh_tasks_dir")
    )
    _ssh_jobs_dir = validator("ssh_jobs_dir", allow_reuse=True)(
        val_absolute_path("ssh_jobs_dir")
    )

    _slurm_user = validator("slurm_user", allow_reuse=True)(
        valstr("slurm_user")
    )
    _slurm_accounts = validator("slurm_accounts", allow_reuse=True)(
        val_unique_list("slurm_accounts")
    )

    @validator("cache_dir")
    def cache_dir_validator(cls, value):
        validate_cmd(value)
        return val_absolute_path("cache_dir")(value)


class UserSettingsUpdateStrict(BaseModel):
    # Slurm
    slurm_user: Optional[str]
    slurm_accounts: Optional[list[StrictStr]]
    # Cache
    cache_dir: Optional[str]

    _slurm_user = validator("slurm_user", allow_reuse=True)(
        valstr("slurm_user")
    )
    _slurm_accounts = validator("slurm_accounts", allow_reuse=True)(
        val_unique_list("slurm_accounts")
    )

    @validator("cache_dir")
    def cache_dir_validator(cls, value):
        validate_cmd(value)
        return val_absolute_path("cache_dir")(value)
