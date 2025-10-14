from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict

from .resource import ResourceType
from fractal_server.types import AbsolutePathStr
from fractal_server.types import NonEmptyStr


def validate_profile(
    *,
    resource_type: str,
    profile_data: dict[str, Any],
) -> None:
    match resource_type:
        case ResourceType.LOCAL:
            ValidProfileLocal(**profile_data)
        case ResourceType.SLURM_SUDO:
            ValidProfileSlurmSudo(**profile_data)
        case ResourceType.SLURM_SSH:
            ValidProfileSlurmSSH(**profile_data)


class ValidProfileLocal(BaseModel):
    username: None = None
    ssh_key_path: None = None
    jobs_remote_dir: None = None
    tasks_remote_dir: None = None


class ValidProfileSlurmSudo(BaseModel):
    username: NonEmptyStr
    ssh_key_path: None = None
    jobs_remote_dir: None = None
    tasks_remote_dir: None = None


class ValidProfileSlurmSSH(BaseModel):
    username: NonEmptyStr
    ssh_key_path: AbsolutePathStr
    jobs_remote_dir: AbsolutePathStr
    tasks_remote_dir: AbsolutePathStr


class ProfileCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: NonEmptyStr
    username: NonEmptyStr | None = None
    ssh_key_path: NonEmptyStr | None = None
    jobs_remote_dir: NonEmptyStr | None = None
    tasks_remote_dir: NonEmptyStr | None = None


class ProfileRead(BaseModel):
    id: int
    name: str
    resource_id: int
    username: str | None = None
    ssh_key_path: str | None = None
    jobs_remote_dir: str | None = None
    tasks_remote_dir: str | None = None


class ProfileUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: NonEmptyStr | None = None
    username: NonEmptyStr | None = None
    ssh_key_path: NonEmptyStr | None = None
    jobs_remote_dir: NonEmptyStr | None = None
    tasks_remote_dir: NonEmptyStr | None = None
