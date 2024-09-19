from typing import Optional

from pydantic import BaseModel
from pydantic import validator

from ._validators import val_absolute_path
from ._validators import valstr


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


class UserSettingsUpdateStrict(BaseModel):
    pass
