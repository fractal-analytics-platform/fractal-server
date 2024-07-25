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
    filters: dict[Literal["types", "attributes"], dict[str, Any]] = Field(
        default_factory=dict
    )
    history: list = Field(default_factory=list)

    @property
    def image_zarr_urls(self) -> list[str]:
        return [image["zarr_urls"] for image in self.images]

    @validator("filters", always=True)
    def _default_filters(cls, value):
        if value == {}:
            return {"types": {}, "attributes": {}}
        return value


class TaskV2Mock(BaseModel):
    id: int
    name: str
    source: str
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


class TaskV1Mock(BaseModel):
    id: int
    name: str
    command: str  # str
    source: str = Field(unique=True)
    input_type: str
    output_type: str
    meta: Optional[dict[str, Any]] = Field(default_factory=dict)

    @property
    def parallelization_level(self) -> Optional[str]:
        try:
            return self.meta["parallelization_level"]
        except KeyError:
            return None

    @property
    def is_parallel(self) -> bool:
        return bool(self.parallelization_level)


class WorkflowTaskV2Mock(BaseModel):
    args_non_parallel: dict[str, Any] = Field(default_factory=dict)
    args_parallel: dict[str, Any] = Field(default_factory=dict)
    meta_non_parallel: dict[str, Any] = Field(default_factory=dict)
    meta_parallel: dict[str, Any] = Field(default_factory=dict)
    is_legacy_task: Optional[bool]
    meta_parallel: Optional[dict[str, Any]] = Field()
    meta_non_parallel: Optional[dict[str, Any]] = Field()
    task: Optional[TaskV2Mock] = None
    task_legacy: Optional[TaskV1Mock] = None
    is_legacy_task: bool = False
    input_filters: dict[str, Any] = Field(default_factory=dict)
    order: int
    id: int
    workflow_id: int = 0
    task_legacy_id: Optional[int]
    task_id: Optional[int]

    @root_validator(pre=False)
    def _legacy_or_not(cls, values):
        is_legacy_task = values["is_legacy_task"]
        task = values.get("task")
        task_legacy = values.get("task_legacy")
        if is_legacy_task:
            if task_legacy is None or task is not None:
                raise ValueError(f"Invalid WorkflowTaskV2Mock with {values=}")
            values["task_legacy_id"] = task_legacy.id
        else:
            if task is None or task_legacy is not None:
                raise ValueError(f"Invalid WorkflowTaskV2Mock with {values=}")
            values["task_id"] = task.id
        return values

    @validator("input_filters", always=True)
    def _default_filters(cls, value):
        if value == {}:
            return {"types": {}, "attributes": {}}
