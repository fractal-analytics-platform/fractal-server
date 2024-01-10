from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator

from ._validators import val_absolute_path
from ._validators import valstr
from .dumps import WorkflowTaskDump
from .project import ProjectRead
from .workflow import WorkflowTaskStatusType

__all__ = (
    "DatasetUpdate",
    "DatasetCreate",
    "DatasetRead",
    "ResourceCreate",
    "ResourceRead",
    "ResourceUpdate",
    "DatasetStatusRead",
)


class _ResourceBase(BaseModel):
    """
    Base class for `Resource`.

    Attributes:
        path:
    """

    path: str


class ResourceCreate(_ResourceBase):
    """
    Class for `Resource` creation.
    """

    # Validators
    _path = validator("path", allow_reuse=True)(val_absolute_path("path"))


class ResourceUpdate(_ResourceBase):
    """
    Class for `Resource` update.
    """

    # Validators
    _path = validator("path", allow_reuse=True)(val_absolute_path("path"))


class ResourceRead(_ResourceBase):
    """
    Class for `Resource` read from database.

    Attributes:
        id:
        dataset_id:
    """

    id: int
    dataset_id: int


class _DatasetHistoryItem(BaseModel):
    """
    Class for an item of `Dataset.history`.

    Attributes:
        workflowtask:
        status:
        parallelization: If provided, it includes keys `parallelization_level`
            and `component_list`.
    """

    workflowtask: WorkflowTaskDump
    status: WorkflowTaskStatusType
    parallelization: Optional[dict]


class _DatasetBase(BaseModel):
    """
    Base class for `Dataset`.

    Attributes:
        name:
        type:
        meta:
        history:
        read_only:
    """

    name: str
    type: Optional[str]
    meta: dict[str, Any] = Field(default={})
    history: list[_DatasetHistoryItem] = Field(default=[])
    read_only: bool = False


class DatasetUpdate(_DatasetBase):
    """
    Class for `Dataset` update.

    Attributes:
        name:
        meta:
        history:
        read_only:
    """

    name: Optional[str]
    meta: Optional[dict[str, Any]] = None
    history: Optional[list[_DatasetHistoryItem]] = None
    read_only: Optional[bool]

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))
    _type = validator("type", allow_reuse=True)(valstr("type"))


class DatasetCreate(_DatasetBase):
    """
    Class for `Dataset` creation.
    """

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))
    _type = validator("type", allow_reuse=True)(valstr("type"))


class DatasetRead(_DatasetBase):
    """
    Class for `Dataset` read from database.

    Attributes:
        id:
        read_only:
        resource_list:
        project_id:
        project:
    """

    id: int
    resource_list: list[ResourceRead]
    project_id: int
    read_only: bool
    project: ProjectRead


class DatasetStatusRead(BaseModel):
    """
    Response type for the
    `/project/{project_id}/dataset/{dataset_id}/status/` endpoint
    """

    status: Optional[
        dict[
            int,
            WorkflowTaskStatusType,
        ]
    ] = None
