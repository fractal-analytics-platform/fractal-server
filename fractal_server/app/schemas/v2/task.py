from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from fractal_server.logger import set_logger
from fractal_server.string_tools import validate_cmd
from fractal_server.types import DictStrAny
from fractal_server.types import HttpUrlStr
from fractal_server.types import ListUniqueNonEmptyString
from fractal_server.types import NonEmptyStr
from fractal_server.types import TypeFilters

TaskTypeType = Literal[
    "compound",
    "converter_compound",
    "non_parallel",
    "converter_non_parallel",
    "parallel",
]


logger = set_logger(__name__)


class TaskCreateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: NonEmptyStr

    command_non_parallel: NonEmptyStr = None
    command_parallel: NonEmptyStr = None

    meta_non_parallel: Optional[DictStrAny] = None
    meta_parallel: Optional[DictStrAny] = None
    version: NonEmptyStr = None
    args_schema_non_parallel: Optional[DictStrAny] = None
    args_schema_parallel: Optional[DictStrAny] = None
    args_schema_version: NonEmptyStr = None
    docs_info: Optional[str] = None
    docs_link: Optional[HttpUrlStr] = None

    input_types: TypeFilters = Field(default={})
    output_types: TypeFilters = Field(default={})

    category: Optional[NonEmptyStr] = None
    modality: Optional[NonEmptyStr] = None
    tags: ListUniqueNonEmptyString = Field(default_factory=list)
    authors: Optional[NonEmptyStr] = None

    type: Optional[TaskTypeType] = None

    @model_validator(mode="after")
    def validate_commands(self):
        command_parallel = self.command_parallel
        command_non_parallel = self.command_non_parallel
        if (command_parallel is None) and (command_non_parallel is None):
            raise ValueError(
                "Task must have at least one valid command "
                "(parallel and/or non_parallel)"
            )
        if command_parallel is not None:
            validate_cmd(command_parallel)
        if command_non_parallel is not None:
            validate_cmd(command_non_parallel)

        return self

    @model_validator(mode="after")
    def set_task_type(self):
        if self.type is None:
            logger.warning(
                f"Task type is not set for task '{self.name}', "
                "which will be deprecated in a future version. "
                "Please move to `fractal-task-tools`."
            )
            if self.command_non_parallel is None:
                self.type = "parallel"
            elif self.command_parallel is None:
                self.type = "non_parallel"
            else:
                self.type = "compound"

        return self


class TaskReadV2(BaseModel):
    id: int
    name: str
    type: TaskTypeType
    source: Optional[str] = None
    version: Optional[str] = None

    command_non_parallel: Optional[str] = None
    command_parallel: Optional[str] = None
    meta_parallel: dict[str, Any]
    meta_non_parallel: dict[str, Any]
    args_schema_non_parallel: Optional[dict[str, Any]] = None
    args_schema_parallel: Optional[dict[str, Any]] = None
    args_schema_version: Optional[str] = None
    docs_info: Optional[str] = None
    docs_link: Optional[str] = None
    input_types: dict[str, bool]
    output_types: dict[str, bool]

    taskgroupv2_id: Optional[int] = None

    category: Optional[str] = None
    modality: Optional[str] = None
    authors: Optional[str] = None
    tags: list[str]


class TaskUpdateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command_parallel: NonEmptyStr = None
    command_non_parallel: NonEmptyStr = None
    input_types: TypeFilters = None
    output_types: TypeFilters = None

    category: Optional[NonEmptyStr] = None
    modality: Optional[NonEmptyStr] = None
    authors: Optional[NonEmptyStr] = None
    tags: Optional[ListUniqueNonEmptyString] = None


class TaskImportV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pkg_name: NonEmptyStr
    version: Optional[NonEmptyStr] = None
    name: NonEmptyStr


class TaskImportV2Legacy(BaseModel):
    source: NonEmptyStr


class TaskExportV2(BaseModel):
    pkg_name: NonEmptyStr
    version: Optional[NonEmptyStr] = None
    name: NonEmptyStr
