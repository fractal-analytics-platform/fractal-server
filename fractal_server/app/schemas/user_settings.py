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
    ssh_host: str | None = None
    ssh_username: str | None = None
    ssh_private_key_path: str | None = None
    ssh_tasks_dir: str | None = None
    ssh_jobs_dir: str | None = None
    slurm_user: str | None = None
    slurm_accounts: list[str]
    project_dir: str | None = None


class UserSettingsReadStrict(BaseModel):
    slurm_user: str | None = None
    slurm_accounts: list[str]
    ssh_username: str | None = None
    project_dir: str | None = None


class UserSettingsUpdate(BaseModel):
    """
    Schema reserved for superusers
    """

    model_config = ConfigDict(extra="forbid")

    ssh_host: NonEmptyStr | None = None
    ssh_username: NonEmptyStr | None = None
    ssh_private_key_path: AbsolutePathStr | None = None
    ssh_tasks_dir: AbsolutePathStr | None = None
    ssh_jobs_dir: AbsolutePathStr | None = None
    slurm_user: NonEmptyStr | None = None
    slurm_accounts: ListUniqueNonEmptyString | None = None
    project_dir: AbsolutePathStr | None = None

    @field_validator("project_dir", mode="after")
    @classmethod
    def validate_project_dir(cls, value):
        if value is not None:
            validate_cmd(value)
        return value


class UserSettingsUpdateStrict(BaseModel):
    model_config = ConfigDict(extra="forbid")
    slurm_accounts: ListUniqueNonEmptyString | None = None
