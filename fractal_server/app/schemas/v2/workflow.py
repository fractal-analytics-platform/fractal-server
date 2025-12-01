from datetime import datetime

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_serializer
from pydantic.types import AwareDatetime

from fractal_server.app.schemas.v2.project import ProjectRead
from fractal_server.app.schemas.v2.workflowtask import WorkflowTaskExport
from fractal_server.app.schemas.v2.workflowtask import WorkflowTaskImport
from fractal_server.app.schemas.v2.workflowtask import WorkflowTaskRead
from fractal_server.app.schemas.v2.workflowtask import (
    WorkflowTaskReadWithWarning,
)
from fractal_server.types import ListUniqueNonNegativeInt
from fractal_server.types import NonEmptyStr


class WorkflowCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: NonEmptyStr


class WorkflowRead(BaseModel):
    id: int
    name: str
    project_id: int
    task_list: list[WorkflowTaskRead]
    project: ProjectRead
    timestamp_created: AwareDatetime

    @field_serializer("timestamp_created")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()


class WorkflowReadWithWarnings(WorkflowRead):
    task_list: list[WorkflowTaskReadWithWarning]


class WorkflowUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: NonEmptyStr = None
    reordered_workflowtask_ids: ListUniqueNonNegativeInt | None = None


class WorkflowImport(BaseModel):
    """
    Class for `Workflow` import.

    Attributes:
        task_list:
    """

    model_config = ConfigDict(extra="forbid")
    name: NonEmptyStr
    task_list: list[WorkflowTaskImport]


class WorkflowExport(BaseModel):
    """
    Class for `Workflow` export.

    Attributes:
        task_list:
    """

    name: str
    task_list: list[WorkflowTaskExport]
