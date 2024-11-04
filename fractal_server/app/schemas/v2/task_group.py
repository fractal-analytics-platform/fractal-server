from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic import validator

from .._validators import val_absolute_path
from .._validators import valdictkeys
from .._validators import valstr
from .task import TaskReadV2


class TaskGroupV2OriginEnum(str, Enum):
    PYPI = "pypi"
    WHEELFILE = "wheel-file"
    OTHER = "other"


class TaskGroupCreateV2(BaseModel, extra=Extra.forbid):
    user_id: int
    user_group_id: int | None = None
    active: bool = True
    origin: TaskGroupV2OriginEnum
    pkg_name: str
    version: str | None = None
    python_version: str | None = None
    path: str | None = None
    venv_path: str | None = None
    wheel_path: str | None = None
    pip_extras: str | None = None
    pinned_package_versions: dict[str, str] = Field(default_factory=dict)

    # Validators
    _path = validator("path", allow_reuse=True)(val_absolute_path("path"))
    _venv_path = validator("venv_path", allow_reuse=True)(
        val_absolute_path("venv_path")
    )
    _wheel_path = validator("wheel_path", allow_reuse=True)(
        val_absolute_path("wheel_path")
    )
    _pinned_package_versions = validator(
        "pinned_package_versions", allow_reuse=True
    )(valdictkeys("pinned_package_versions"))
    _pip_extras = validator("pip_extras", allow_reuse=True)(
        valstr("pip_extras")
    )
    _python_version = validator("python_version", allow_reuse=True)(
        valstr("python_version")
    )


class TaskGroupReadV2(BaseModel):
    id: int
    task_list: list[TaskReadV2]

    user_id: int
    user_group_id: int | None = None

    origin: Literal["pypi", "wheel-file", "other"]
    pkg_name: str
    version: str | None = None
    python_version: str | None = None
    path: str | None = None
    venv_path: str | None = None
    wheel_path: str | None = None
    pip_extras: str | None = None
    pinned_package_versions: dict[str, str] = Field(default_factory=dict)

    active: bool
    timestamp_created: datetime


class TaskGroupUpdateV2(BaseModel, extra=Extra.forbid):
    user_group_id: int | None = None
    active: bool | None = None

    @validator("active")
    def active_cannot_be_None(cls, value):
        if value is None:
            raise ValueError("`active` cannot be set to None")
        return value
