from typing import Any

from pydantic import BaseModel

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


class _ValidProfileBase(BaseModel):
    pass


class ValidProfileLocal(_ValidProfileBase):
    pass


class ValidProfileSlurmSudo(_ValidProfileBase):
    username: NonEmptyStr


class ValidProfileSlurmSSH(_ValidProfileBase):
    username: NonEmptyStr
    ssh_key_path: AbsolutePathStr
    jobs_remote_dir: AbsolutePathStr
    tasks_remote_dir: AbsolutePathStr
