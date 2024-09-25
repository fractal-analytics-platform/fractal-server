from datetime import datetime
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator

from .._validators import valutc
from .dumps import WorkflowTaskDumpV1
from .project import ProjectReadV1
from .workflow import WorkflowTaskStatusTypeV1

__all__ = (
    "DatasetReadV1",
    "ResourceReadV1",
    "DatasetStatusReadV1",
)


class _ResourceBaseV1(BaseModel):
    """
    Base class for `Resource`.

    Attributes:
        path:
    """

    path: str


class ResourceReadV1(_ResourceBaseV1):
    """
    Class for `Resource` read from database.

    Attributes:
        id:
        dataset_id:
    """

    id: int
    dataset_id: int


class _DatasetHistoryItemV1(BaseModel):
    """
    Class for an item of `Dataset.history`.

    Attributes:
        workflowtask:
        status:
        parallelization: If provided, it includes keys `parallelization_level`
            and `component_list`.
    """

    workflowtask: WorkflowTaskDumpV1
    status: WorkflowTaskStatusTypeV1
    parallelization: Optional[dict]


class _DatasetBaseV1(BaseModel):
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
    history: list[_DatasetHistoryItemV1] = Field(default=[])
    read_only: bool = False


class DatasetReadV1(_DatasetBaseV1):
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
    resource_list: list[ResourceReadV1]
    project_id: int
    read_only: bool
    project: ProjectReadV1
    timestamp_created: datetime

    _timestamp_created = validator("timestamp_created", allow_reuse=True)(
        valutc("timestamp_created")
    )


class DatasetStatusReadV1(BaseModel):
    """
    Response type for the
    `/project/{project_id}/dataset/{dataset_id}/status/` endpoint
    """

    status: Optional[
        dict[
            int,
            WorkflowTaskStatusTypeV1,
        ]
    ] = None
