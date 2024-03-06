from typing import Any
from typing import Callable
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator

from ....images import SingleImage
from ....images import val_scalar_dict

DictStrAny = dict[str, Any]


class Dataset(BaseModel):
    id: Optional[int] = None
    history: list[DictStrAny] = []
    # New in v2
    zarr_dir: str
    images: list[SingleImage] = Field(default_factory=list)
    filters: DictStrAny = Field(default_factory=dict)
    # Temporary state
    buffer: Optional[DictStrAny] = None
    parallelization_list: Optional[list[DictStrAny]] = None
    # Removed from V1
    # resource_list (relationship)

    @property
    def image_paths(self) -> list[str]:
        return [image.path for image in self.images]


class Task(BaseModel):
    function: Callable  # mock of task.command
    meta: DictStrAny = Field(default_factory=dict)
    new_filters: DictStrAny = Field(default_factory=dict)
    task_type: Literal["non_parallel", "parallel"] = "non_parallel"

    _new_filters = validator("new_filters", allow_reuse=True)(
        val_scalar_dict("new_filters")
    )

    @property
    def name(self) -> str:
        return self.function.__name__


class WorkflowTask(BaseModel):
    args: DictStrAny = Field(default_factory=dict)
    meta: DictStrAny = Field(default_factory=dict)
    task: Optional[Task] = None
    filters: DictStrAny = Field(default_factory=dict)


class Workflow(BaseModel):
    task_list: list[WorkflowTask] = Field(default_factory=list)
