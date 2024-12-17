from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import root_validator
from pydantic import validator


class DatasetV2Mock(BaseModel):
    id: Optional[int] = None
    name: str
    zarr_dir: str
    images: list[dict[str, Any]] = Field(default_factory=list)
    filters: dict[
        Literal["types", "attributes_include", "attributes_exclude"],
        dict[str, list[Any]],
    ] = Field(default_factory=dict)
    history: list = Field(default_factory=list)

    @property
    def image_zarr_urls(self) -> list[str]:
        return [image["zarr_urls"] for image in self.images]

    @validator("filters", always=True)
    def _default_filters(cls, value):
        if value == {}:
            return {
                "types": {},
                "attributes_include": {},
                "attributes_exclude": {},
            }
        return value


class TaskV2Mock(BaseModel):
    id: int
    name: str
    source: Optional[str] = None
    input_types: dict[str, bool] = Field(default_factory=dict)
    output_types: dict[str, bool] = Field(default_factory=dict)

    command_non_parallel: Optional[str] = None
    command_parallel: Optional[str] = None
    meta_parallel: Optional[dict[str, Any]] = Field(default_factory=dict)
    meta_non_parallel: Optional[dict[str, Any]] = Field(default_factory=dict)
    type: Optional[str]

    @root_validator(pre=False)
    def _not_both_commands_none(cls, values):
        print(values)
        _command_non_parallel = values.get("command_non_parallel")
        _command_parallel = values.get("command_parallel")
        if _command_non_parallel is None and _command_parallel is None:
            raise ValueError(
                "Both command_non_parallel and command_parallel are None"
            )
        return values

    @validator("type", always=True)
    def _set_type(cls, value, values):
        if values.get("command_non_parallel") is None:
            if values.get("command_parallel") is None:
                raise ValueError(
                    "This TaskV2Mock object has both commands unset."
                )
            else:
                return "parallel"
        else:
            if values.get("command_parallel") is None:
                return "non_parallel"
            else:
                return "compound"


class WorkflowTaskV2Mock(BaseModel):
    args_non_parallel: dict[str, Any] = Field(default_factory=dict)
    args_parallel: dict[str, Any] = Field(default_factory=dict)
    meta_non_parallel: dict[str, Any] = Field(default_factory=dict)
    meta_parallel: dict[str, Any] = Field(default_factory=dict)
    meta_parallel: Optional[dict[str, Any]] = Field()
    meta_non_parallel: Optional[dict[str, Any]] = Field()
    task: TaskV2Mock
    input_filters: dict[str, list[Any]] = Field(default_factory=dict)
    order: int
    id: int
    workflow_id: int = 0
    task_id: int

    @validator("input_filters", always=True)
    def _default_filters(cls, value):
        if value == {}:
            return {
                "types": {},
                "attributes_include": {},
                "attributes_exclude": {},
            }
