from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_serializer
from pydantic.types import AwareDatetime

from fractal_server.app.schemas.v2.task import TaskReadV2
from fractal_server.types import AbsolutePathStr
from fractal_server.types import DictStrStr
from fractal_server.types import NonEmptyStr


class TaskGroupV2OriginEnum(StrEnum):
    PYPI = "pypi"
    WHEELFILE = "wheel-file"
    PIXI = "pixi"
    OTHER = "other"


class TaskGroupActivityStatusV2(StrEnum):
    PENDING = "pending"
    ONGOING = "ongoing"
    FAILED = "failed"
    OK = "OK"


class TaskGroupActivityActionV2(StrEnum):
    COLLECT = "collect"
    DEACTIVATE = "deactivate"
    REACTIVATE = "reactivate"


class TaskGroupCreateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")
    user_id: int
    user_group_id: int | None = None
    active: bool = True
    origin: TaskGroupV2OriginEnum
    pkg_name: str
    version: str | None = None
    python_version: NonEmptyStr = None
    pixi_version: NonEmptyStr = None
    path: AbsolutePathStr = None
    venv_path: AbsolutePathStr = None
    archive_path: AbsolutePathStr = None
    pip_extras: NonEmptyStr = None
    env_info: str | None = None
    pinned_package_versions: DictStrStr = Field(default_factory=dict)


class TaskGroupCreateV2Strict(TaskGroupCreateV2):
    """
    A strict version of TaskGroupCreateV2, to be used for task collection.
    """

    path: AbsolutePathStr
    version: NonEmptyStr
    venv_path: AbsolutePathStr
    python_version: NonEmptyStr


class TaskGroupReadV2(BaseModel):
    id: int
    task_list: list[TaskReadV2]

    user_id: int
    user_group_id: int | None = None

    origin: TaskGroupV2OriginEnum
    pkg_name: str
    version: str | None = None
    python_version: str | None = None
    pixi_version: str | None = None
    path: str | None = None
    venv_path: str | None = None
    archive_path: str | None = None
    pip_extras: str | None = None
    pinned_package_versions: dict[str, str] = Field(default_factory=dict)

    venv_size_in_kB: int | None = None
    venv_file_number: int | None = None

    active: bool
    timestamp_created: AwareDatetime
    timestamp_last_used: AwareDatetime

    @field_serializer("timestamp_created", "timestamp_last_used")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()


class TaskGroupUpdateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")
    user_group_id: int | None = None


class TaskGroupActivityV2Read(BaseModel):
    id: int
    user_id: int
    taskgroupv2_id: int | None = None
    timestamp_started: AwareDatetime
    timestamp_ended: AwareDatetime | None = None
    pkg_name: str
    version: str
    status: TaskGroupActivityStatusV2
    action: TaskGroupActivityActionV2
    log: str | None = None

    @field_serializer("timestamp_started")
    def serialize_datetime_start(v: datetime) -> str:
        return v.isoformat()

    @field_serializer("timestamp_ended")
    def serialize_datetime_end(v: datetime | None) -> str | None:
        if v is None:
            return None
        else:
            return v.isoformat()
