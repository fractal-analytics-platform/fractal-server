from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_serializer
from pydantic import field_validator
from pydantic.types import AwareDatetime

from .._validators import cant_set_none
from .._validators import NonEmptyString
from .._validators import val_absolute_path
from .._validators import valdict_keys
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


class TaskGroupCreateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")
    user_id: int
    user_group_id: Optional[int] = None
    active: bool = True
    origin: TaskGroupV2OriginEnum
    pkg_name: str
    version: Optional[str] = None
    python_version: Optional[NonEmptyString] = None
    path: Optional[str] = None
    venv_path: Optional[str] = None
    wheel_path: Optional[str] = None
    pip_extras: Optional[NonEmptyString] = None
    pip_freeze: Optional[str] = None
    pinned_package_versions: dict[str, str] = Field(default_factory=dict)

    # Validators

    @field_validator("python_version", "pip_extras")
    @classmethod
    def _cant_set_none(cls, v):
        return cant_set_none(v)

    _path = field_validator("path")(classmethod(val_absolute_path("path")))
    _venv_path = field_validator("venv_path")(
        classmethod(val_absolute_path("venv_path"))
    )
    _wheel_path = field_validator("wheel_path")(
        classmethod(val_absolute_path("wheel_path"))
    )
    _pinned_package_versions = field_validator("pinned_package_versions")(
        valdict_keys("pinned_package_versions")
    )


class TaskGroupCreateV2Strict(TaskGroupCreateV2):
    """
    A strict version of TaskGroupCreateV2, to be used for task collection.
    """

    path: str
    venv_path: str
    version: str
    python_version: str


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
    timestamp_created: AwareDatetime
    timestamp_last_used: AwareDatetime

    @field_serializer("timestamp_created", "timestamp_last_used")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()


class TaskGroupUpdateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")
    user_group_id: Optional[int] = None


class TaskGroupActivityV2Read(BaseModel):
    id: int
    user_id: int
    taskgroupv2_id: Optional[int] = None
    timestamp_started: AwareDatetime
    timestamp_ended: Optional[AwareDatetime] = None
    pkg_name: str
    version: str
    status: TaskGroupActivityStatusV2
    action: TaskGroupActivityActionV2
    log: Optional[str] = None

    @field_serializer("timestamp_started")
    def serialize_datetime_start(v: datetime) -> str:
        return v.isoformat()

    @field_serializer("timestamp_ended")
    def serialize_datetime_end(v: Optional[datetime]) -> Optional[str]:
        if v is None:
            return None
        else:
            return v.isoformat()
