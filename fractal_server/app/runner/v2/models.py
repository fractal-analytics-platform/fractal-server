from typing import Any
from typing import Callable
from typing import Literal
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator


KwargsType = dict[str, Any]
Scalar = Union[int, float, str, bool, None]
ImageAttributesType = dict[str, Scalar]


def val_filters(attribute: str):
    def val(kwargs: dict):
        for key, value in kwargs.items():
            if not isinstance(key, str):
                raise ValueError("Key must be a string")
            if not isinstance(value, (int, float, str, bool, type(None))):
                raise ValueError(
                    f"{attribute}[{key}] must be a scalar (int, float, str, "
                    f"bool, or None). Given {value} ({type(value)})"
                )
        return kwargs

    return val


class SingleImage(BaseModel):
    path: str
    attributes: ImageAttributesType = Field(default_factory=dict)

    _attributes = validator("attributes", allow_reuse=True)(
        val_filters("attributes")
    )

    def match_filter(self, filters: ImageAttributesType):
        for key, value in filters.items():
            if value is None:
                continue
            if self.attributes.get(key) != value:
                return False
        return True


class Dataset(BaseModel):
    id: Optional[int] = None
    history: list[KwargsType] = []
    # New in v2
    # root_dir: str
    images: list[SingleImage] = Field(default_factory=list)
    filters: ImageAttributesType = Field(default_factory=dict)
    # Temporary state
    buffer: Optional[KwargsType] = None
    parallelization_list: Optional[list[KwargsType]] = None
    # Removed from V1
    # resource_list (relationship)

    @property
    def image_paths(self) -> list[str]:
        return [image["path"] for image in self.images]


class Task(BaseModel):
    function: Callable  # mock of task.command
    meta: KwargsType = Field(default_factory=dict)
    new_filters: KwargsType = Field(
        default_factory=dict
    )  # FIXME: this is not using ImageAttributesType any more!
    task_type: Literal["non_parallel", "parallel"] = "non_parallel"

    @validator("new_filters")
    def scalar_filters(cls, v):
        """
        Check that values of new_filters are all JSON-scalar.

        Replacement for `new_filters: ImageAttributesType` attribute type,
        which does not work in Pydantic.
        """
        for value in v.values():
            if type(value) not in [int, str, bool] and value is not None:
                raise ValueError(
                    f"{value=} in new_filters has invalid type {type(value)}"
                )

        return v

    @property
    def name(self) -> str:
        return self.function.__name__


class WorkflowTask(BaseModel):
    args: KwargsType = Field(default_factory=dict)
    meta: KwargsType = Field(default_factory=dict)
    task: Optional[Task] = None
    filters: ImageAttributesType = Field(default_factory=dict)


class Workflow(BaseModel):
    task_list: list[WorkflowTask] = Field(default_factory=list)
