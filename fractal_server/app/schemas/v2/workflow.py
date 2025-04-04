from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_serializer
from pydantic import field_validator
from pydantic.types import AwareDatetime

from .._validators import cant_set_none
from .._validators import NonEmptyString
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

    name: Optional[NonEmptyString] = None
    reordered_workflowtask_ids: Optional[list[int]] = None

    # Validators

    @field_validator("name")
    @classmethod
    def _cant_set_none(cls, v):
        return cant_set_none(v)

    @field_validator("reordered_workflowtask_ids")
    @classmethod
    def check_positive_and_unique(cls, value):
        if any(i < 0 for i in value):
            raise ValueError("Negative `id` in `reordered_workflowtask_ids`")
        if len(value) != len(set(value)):
            raise ValueError("`reordered_workflowtask_ids` has repetitions")
        return value


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
