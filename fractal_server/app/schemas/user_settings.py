from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator

from fractal_server.string_tools import validate_cmd
from fractal_server.types import AbsolutePathStr
from fractal_server.types import ListUniqueNonEmptyString
from fractal_server.types import NonEmptyStr

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

    ssh_host: Optional[NonEmptyStr] = None
    ssh_username: Optional[NonEmptyStr] = None
    ssh_private_key_path: Optional[AbsolutePathStr] = None
    ssh_tasks_dir: Optional[AbsolutePathStr] = None
    ssh_jobs_dir: Optional[AbsolutePathStr] = None
    slurm_user: Optional[NonEmptyStr] = None
    slurm_accounts: Optional[ListUniqueNonEmptyString] = None
    project_dir: Optional[AbsolutePathStr] = None

    @field_validator("project_dir", mode="after")
    @classmethod
    def validate_project_dir(cls, value):
        if value is not None:
            validate_cmd(value)
        return value


class UserSettingsUpdateStrict(BaseModel):
    model_config = ConfigDict(extra="forbid")
    slurm_accounts: Optional[ListUniqueNonEmptyString] = None
