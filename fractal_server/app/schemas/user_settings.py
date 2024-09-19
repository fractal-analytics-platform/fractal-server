from typing import Optional

from pydantic import BaseModel


__all__ = (
    "SettingsRead",
    "SettingsReadStrict",
    "SettingsUpdate",
    "SettingsUpdateStrict",
)


class SettingsRead(BaseModel):
    id: int
    ssh_host: Optional[str]
    ssh_username: Optional[str]
    ssh_private_key_path: Optional[str]
    ssh_tasks_dir: Optional[str]
    ssh_jobs_dir: Optional[str]


class SettingsReadStrict(BaseModel):
    id: int


class SettingsUpdate(BaseModel):
    ssh_host: Optional[str]
    ssh_username: Optional[str]
    ssh_private_key_path: Optional[str]
    ssh_tasks_dir: Optional[str]
    ssh_jobs_dir: Optional[str]


class SettingsUpdateStrict(BaseModel):
    pass
