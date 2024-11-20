from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic import root_validator
from pydantic import validator

from .._validators import val_absolute_path
from .._validators import valdictkeys
from .._validators import valstr
from .task import TaskReadV2


class TaskGroupV2OriginEnum(str, Enum):
    PYPI = "pypi"
    WHEELFILE = "wheel-file"
    OTHER = "other"


class TaskGroupActivityStatusV2(str, Enum):
    PENDING = "pending"
    ONGOING = "ongoing"
    FAILED = "failed"
    OK = "OK"


class TaskGroupActivityActionV2(str, Enum):
    COLLECT = "collect"
    DEACTIVATE = "deactivate"
    REACTIVATE = "reactivate"


class TaskGroupCreateV2(BaseModel, extra=Extra.forbid):
    user_id: int
    user_group_id: Optional[int] = None
    active: bool = True
    origin: TaskGroupV2OriginEnum
    pkg_name: str
    version: Optional[str] = None
    python_version: Optional[str] = None
    path: Optional[str] = None
    venv_path: Optional[str] = None
    wheel_path: Optional[str] = None
    pip_extras: Optional[str] = None
    pip_freeze: Optional[str] = None
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


class TaskGroupCreateV2Strict(TaskGroupCreateV2):
    """
    A strict version of TaskGroupCreateV2, to be used for task collection.
    """

    path: str
    venv_path: str
    version: str
    python_version: str

    @root_validator
    def check_wheel_file(cls, values):
        origin = values.get("origin")
        wheel_path = values.get("wheel_path")
        bad_condition_1 = (
            origin == TaskGroupV2OriginEnum.WHEELFILE and wheel_path is None
        )
        bad_condition_2 = (
            origin != TaskGroupV2OriginEnum.WHEELFILE
            and wheel_path is not None
        )
        if bad_condition_1 or bad_condition_2:
            raise ValueError(f"Cannot have {origin=} and {wheel_path=}.")
        return values


class TaskGroupReadV2(BaseModel):
    id: int
    task_list: list[TaskReadV2]

    user_id: int
    user_group_id: Optional[int] = None

    origin: TaskGroupV2OriginEnum
    pkg_name: str
    version: Optional[str] = None
    python_version: Optional[str] = None
    path: Optional[str] = None
    venv_path: Optional[str] = None
    wheel_path: Optional[str] = None
    pip_freeze: Optional[str] = None
    pip_extras: Optional[str] = None
    pinned_package_versions: dict[str, str] = Field(default_factory=dict)

    venv_size_in_kB: Optional[int] = None
    venv_file_number: Optional[int] = None

    active: bool
    timestamp_created: datetime
    timestamp_last_used: datetime


class TaskGroupUpdateV2(BaseModel, extra=Extra.forbid):
    user_group_id: Optional[int] = None


class TaskGroupActivityV2Read(BaseModel):
    id: int
    user_id: int
    taskgroupv2_id: Optional[int] = None
    timestamp_started: datetime
    timestamp_ended: Optional[datetime] = None
    pkg_name: str
    version: str
    status: TaskGroupActivityStatusV2
    action: TaskGroupActivityActionV2
    log: Optional[str] = None
