from typing import Any
from typing import Callable
from typing import Literal
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator

DictStrAny = dict[str, Any]


def val_scalar_dict(attribute: str):
    def val(
        kwargs: DictStrAny,
    ) -> dict[str, Union[int, float, str, bool, None]]:
        for key, value in kwargs.items():
            if not isinstance(value, (int, float, str, bool, type(None))):
                raise ValueError(
                    f"{attribute}[{key}] must be a scalar (int, float, str, "
                    f"bool, or None). Given {value} ({type(value)})"
                )
        return kwargs

    return val


class SingleImage(BaseModel):
    path: str
    attributes: DictStrAny = Field(default_factory=dict)

    _attributes = validator("attributes", allow_reuse=True)(
        val_scalar_dict("attributes")
    )

    def match_filter(self, filters: DictStrAny):
        for key, value in filters.items():
            if value is None:
                continue
            if self.attributes.get(key) != value:
                return False
        return True


class Dataset(BaseModel):
    id: Optional[int] = None
    history: list[DictStrAny] = []
    # New in v2
    # root_dir: str
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
