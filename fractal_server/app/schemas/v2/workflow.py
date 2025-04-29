from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_serializer
from pydantic.types import AwareDatetime

from ....types._validated_types import ListNonNegativeIntUnique
from ....types._validated_types import NonEmptyString
from .project import ProjectReadV2
from .workflowtask import WorkflowTaskExportV2
from .workflowtask import WorkflowTaskImportV2
from .workflowtask import WorkflowTaskReadV2
from .workflowtask import WorkflowTaskReadV2WithWarning


class WorkflowCreateV2(BaseModel):

    model_config = ConfigDict(extra="forbid")

    name: NonEmptyString


class WorkflowReadV2(BaseModel):

    id: int
    name: str
    project_id: int
    task_list: list[WorkflowTaskReadV2]
    project: ProjectReadV2
    timestamp_created: AwareDatetime

    @field_serializer("timestamp_created")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()


class WorkflowReadV2WithWarnings(WorkflowReadV2):
    task_list: list[WorkflowTaskReadV2WithWarning]


class WorkflowUpdateV2(BaseModel):

    model_config = ConfigDict(extra="forbid")

    name: NonEmptyString = None
    reordered_workflowtask_ids: Optional[ListNonNegativeIntUnique] = None


class WorkflowImportV2(BaseModel):
    """
    Class for `Workflow` import.

    Attributes:
        task_list:
    """

    model_config = ConfigDict(extra="forbid")
    name: NonEmptyString
    task_list: list[WorkflowTaskImportV2]


class WorkflowExportV2(BaseModel):
    """
    Class for `Workflow` export.

    Attributes:
        task_list:
    """

    name: str
    task_list: list[WorkflowTaskExportV2]
