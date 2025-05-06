from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import model_validator

from ...types import AbsolutePathStr
from ...types import ListNonEmptyString
from ...types import NonEmptyString
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
    ssh_private_key_path: Optional[AbsolutePathStr] = None
    ssh_tasks_dir: Optional[AbsolutePathStr] = None
    ssh_jobs_dir: Optional[AbsolutePathStr] = None
    slurm_user: Optional[NonEmptyString] = None
    slurm_accounts: Optional[ListNonEmptyString] = None
    project_dir: Optional[AbsolutePathStr] = None

    @model_validator(mode="after")
    def validate_command(self):
        if self.project_dir is not None:
            validate_cmd(self.project_dir)
        return self


class UserSettingsUpdateStrict(BaseModel):
    model_config = ConfigDict(extra="forbid")
    slurm_accounts: Optional[ListNonEmptyString] = None
