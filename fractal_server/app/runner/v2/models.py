from typing import Any
from typing import Callable
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import root_validator

from ....images import SingleImage

DictStrAny = dict[str, Any]


class Dataset(BaseModel):
    id: Optional[int] = None
    history: list[DictStrAny] = []
    zarr_dir: str
    images: list[SingleImage] = Field(default_factory=list)
    attribute_filters: DictStrAny = Field(default_factory=dict)
    flag_filters: DictStrAny = Field(default_factory=dict)

    @property
    def image_paths(self) -> list[str]:
        return [image.path for image in self.images]


class Task(BaseModel):
    meta: DictStrAny = Field(default_factory=dict)
    input_flags: dict[str, bool] = Field(default_factory=dict)
    output_flags: dict[str, bool] = Field(default_factory=dict)

    function_non_parallel: Optional[Callable] = None
    function_parallel: Optional[Callable] = None

    @root_validator(pre=False)
    def _not_both_commands_none(cls, values):
        print(values)
        _function_non_parallel = values.get("function_non_parallel")
        _function_parallel = values.get("function_parallel")
        if _function_non_parallel is None and _function_parallel is None:
            raise ValueError(
                "Both function_non_parallel and function_parallel are None"
            )
        return values

    @property
    def task_type(
        self,
    ) -> Literal["compound", "parallel_standalone", "non_parallel_standalone"]:
        if self.function_non_parallel is None:
            if self.function_parallel is None:
                raise
            else:
                return "parallel_standalone"
        else:
            if self.function_parallel is None:
                return "non_parallel_standalone"
            else:
                return "compound"

    @property
    def name(self) -> str:
        if self.task_type == "parallel_standalone":
            return self.function_parallel.__name__
        elif self.task_type == "non_parallel_standalone":
            return self.function_non_parallel.__name__


class WorkflowTask(BaseModel):
    args_non_parallel: DictStrAny = Field(default_factory=dict)
    args_parallel: DictStrAny = Field(default_factory=dict)
    meta: DictStrAny = Field(default_factory=dict)
    task: Optional[Task] = None
    attribute_filters: DictStrAny = Field(default_factory=dict)
    flag_filters: dict[str, bool] = Field(default_factory=dict)


class Workflow(BaseModel):
    task_list: list[WorkflowTask] = Field(default_factory=list)
