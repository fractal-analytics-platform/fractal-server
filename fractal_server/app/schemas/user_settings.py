from typing import Optional

from pydantic import BaseModel


__all__ = (
    "UserSettingsRead",
    "UserSettingsReadStrict",
    "UserSettingsUpdate",
    "UserSettingsUpdateStrict",
)


class UserSettingsRead(BaseModel):
    id: int
    ssh_host: Optional[str]
    ssh_username: Optional[str]
    ssh_private_key_path: Optional[str]
    ssh_tasks_dir: Optional[str]
    ssh_jobs_dir: Optional[str]


class UserSettingsReadStrict(BaseModel):
    id: int


class UserSettingsUpdate(BaseModel):
    ssh_host: Optional[str]
    ssh_username: Optional[str]
    ssh_private_key_path: Optional[str]
    ssh_tasks_dir: Optional[str]
    ssh_jobs_dir: Optional[str]


class UserSettingsUpdateStrict(BaseModel):
    pass
