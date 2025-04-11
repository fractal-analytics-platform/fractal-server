from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator
from pydantic.types import StrictStr

from ._validators import NonEmptyString
from ._validators import val_absolute_path
from ._validators import val_unique_list
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
    project_dir: Optional[str] = None


class UserSettingsReadStrict(BaseModel):
    slurm_user: Optional[str] = None
    slurm_accounts: list[str]
    ssh_username: Optional[str] = None
    project_dir: Optional[str] = None


class UserSettingsUpdate(BaseModel):
    """
    Schema reserved for superusers
    """

    model_config = ConfigDict(extra="forbid")

    ssh_host: Optional[NonEmptyString] = None
    ssh_username: Optional[NonEmptyString] = None
    ssh_private_key_path: Optional[NonEmptyString] = None
    ssh_tasks_dir: Optional[NonEmptyString] = None
    ssh_jobs_dir: Optional[NonEmptyString] = None
    slurm_user: Optional[NonEmptyString] = None
    slurm_accounts: Optional[list[NonEmptyString]] = None
    project_dir: Optional[NonEmptyString] = None

    _ssh_private_key_path = field_validator("ssh_private_key_path")(
        classmethod(
            val_absolute_path("ssh_private_key_path", accept_none=True)
        )
    )

    _ssh_tasks_dir = field_validator("ssh_tasks_dir")(
        classmethod(val_absolute_path("ssh_tasks_dir", accept_none=True))
    )
    _ssh_jobs_dir = field_validator("ssh_jobs_dir")(
        classmethod(val_absolute_path("ssh_jobs_dir", accept_none=True))
    )

    @field_validator("slurm_accounts")
    @classmethod
    def slurm_accounts_validator(cls, value):
        if value is None:
            return value
        return val_unique_list("slurm_accounts")(cls, value)

    @field_validator("project_dir")
    @classmethod
    def project_dir_validator(cls, value):
        if value is None:
            return None
        validate_cmd(value)
        return val_absolute_path("project_dir")(cls, value)


class UserSettingsUpdateStrict(BaseModel):
    model_config = ConfigDict(extra="forbid")
    slurm_accounts: Optional[list[StrictStr]] = None

    _slurm_accounts = field_validator("slurm_accounts")(
        classmethod(val_unique_list("slurm_accounts"))
    )
