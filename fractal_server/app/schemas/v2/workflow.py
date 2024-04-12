from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import validator

from .._validators import valstr
from .._validators import valutc
from .project import ProjectReadV2
from .workflowtask import WorkflowTaskExportV2
from .workflowtask import WorkflowTaskImportV2
from .workflowtask import WorkflowTaskReadV2


class WorkflowCreateV2(BaseModel, extra=Extra.forbid):

    name: str

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))


class WorkflowReadV2(BaseModel):

    id: int
    name: str
    project_id: int
    task_list: list[WorkflowTaskReadV2]
    project: ProjectReadV2
    timestamp_created: datetime

    _timestamp_created = validator("timestamp_created", allow_reuse=True)(
        valutc("timestamp_created")
    )


class WorkflowUpdateV2(BaseModel):

    name: Optional[str]
    reordered_workflowtask_ids: Optional[list[int]]

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))

    @validator("reordered_workflowtask_ids")
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

    name: str
    task_list: list[WorkflowTaskImportV2]

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))


class WorkflowExportV2(BaseModel):
    """
    Class for `Workflow` export.

    Attributes:
        task_list:
    """

    name: str
    task_list: list[WorkflowTaskExportV2]
