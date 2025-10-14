from pydantic import BaseModel

from fractal_server.types import AbsolutePathStr
from fractal_server.types import NonEmptyStr


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


class ProfileCreate(BaseModel):
    username: str | None = None
    ssh_key_path: str | None = None
    jobs_remote_dir: str | None = None
    tasks_remote_dir: str | None = None


class ProfileRead(BaseModel):
    id: int
    resource_id: int
    username: str | None = None
    ssh_key_path: str | None = None
    jobs_remote_dir: str | None = None
    tasks_remote_dir: str | None = None


class ProfileUpdate(BaseModel):
    username: str | None = None
    ssh_key_path: str | None = None
    jobs_remote_dir: str | None = None
    tasks_remote_dir: str | None = None
