from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import model_validator


class DatasetV2Mock(BaseModel):
    id: Optional[int] = None
    name: str
    zarr_dir: str
    images: list[dict[str, Any]] = Field(default_factory=list)
    history: list = Field(default_factory=list)

    @property
    def image_zarr_urls(self) -> list[str]:
        return [image["zarr_urls"] for image in self.images]


class TaskV2Mock(BaseModel):
    id: int
    name: str
    source: str
    input_types: dict[str, bool] = Field(default_factory=dict)
    output_types: dict[str, bool] = Field(default_factory=dict)

    command_non_parallel: Optional[str] = None
    command_parallel: Optional[str] = None
    meta_non_parallel: Optional[dict[str, Any]] = Field(default_factory=dict)
    meta_parallel: Optional[dict[str, Any]] = Field(default_factory=dict)
    type: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _not_both_commands_none(cls, values):
        _command_non_parallel = values.get("command_non_parallel")
        _command_parallel = values.get("command_parallel")
        if _command_non_parallel is None and _command_parallel is None:
            raise ValueError(
                "Both command_non_parallel and command_parallel are None"
            )
        elif _command_non_parallel is None:
            values["type"] = "parallel"
        elif _command_parallel is None:
            values["type"] = "non_parallel"
        else:
            values["type"] = "compound"

        return values


class WorkflowTaskV2Mock(BaseModel):
    args_non_parallel: dict[str, Any] = Field(default_factory=dict)
    args_parallel: dict[str, Any] = Field(default_factory=dict)
    meta_non_parallel: dict[str, Any] = Field(default_factory=dict)
    meta_parallel: dict[str, Any] = Field(default_factory=dict)
    meta_parallel: Optional[dict[str, Any]] = Field(None)
    meta_non_parallel: Optional[dict[str, Any]] = Field(None)
    task: TaskV2Mock = None
    type_filters: dict[str, bool] = Field(default_factory=dict)
    order: int
    id: int
    workflow_id: int = 0
    task_id: int
