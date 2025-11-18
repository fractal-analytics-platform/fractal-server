from enum import StrEnum
from typing import Any

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


logger = set_logger(__name__)


class TaskType(StrEnum):
    """
    Define the available task types.
    """

    COMPOUND = "compound"
    CONVERTER_COMPOUND = "converter_compound"
    NON_PARALLEL = "non_parallel"
    CONVERTER_NON_PARALLEL = "converter_non_parallel"
    PARALLEL = "parallel"


class TaskCreateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: NonEmptyStr

    command_non_parallel: NonEmptyStr = None
    command_parallel: NonEmptyStr = None

    meta_non_parallel: DictStrAny | None = None
    meta_parallel: DictStrAny | None = None
    version: NonEmptyStr = None
    args_schema_non_parallel: DictStrAny | None = None
    args_schema_parallel: DictStrAny | None = None
    args_schema_version: NonEmptyStr = None
    docs_info: str | None = None
    docs_link: HttpUrlStr | None = None

    input_types: TypeFilters = Field(default={})
    output_types: TypeFilters = Field(default={})

    category: NonEmptyStr | None = None
    modality: NonEmptyStr | None = None
    tags: ListUniqueNonEmptyString = Field(default_factory=list)
    authors: NonEmptyStr | None = None

    type: TaskType | None = None

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
                self.type = TaskType.PARALLEL
            elif self.command_parallel is None:
                self.type = TaskType.NON_PARALLEL
            else:
                self.type = TaskType.COMPOUND

        return self


class TaskReadV2(BaseModel):
    id: int
    name: str
    type: TaskType
    source: str | None = None
    version: str | None = None

    command_non_parallel: str | None = None
    command_parallel: str | None = None
    meta_parallel: dict[str, Any]
    meta_non_parallel: dict[str, Any]
    args_schema_non_parallel: dict[str, Any] | None = None
    args_schema_parallel: dict[str, Any] | None = None
    args_schema_version: str | None = None
    docs_info: str | None = None
    docs_link: str | None = None
    input_types: dict[str, bool]
    output_types: dict[str, bool]

    taskgroupv2_id: int | None = None

    category: str | None = None
    modality: str | None = None
    authors: str | None = None
    tags: list[str]


class TaskUpdateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command_parallel: NonEmptyStr = None
    command_non_parallel: NonEmptyStr = None
    input_types: TypeFilters = None
    output_types: TypeFilters = None

    category: NonEmptyStr | None = None
    modality: NonEmptyStr | None = None
    authors: NonEmptyStr | None = None
    tags: ListUniqueNonEmptyString | None = None


class TaskImportV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pkg_name: NonEmptyStr
    version: NonEmptyStr | None = None
    name: NonEmptyStr


class TaskImportV2Legacy(BaseModel):
    source: NonEmptyStr


class TaskExportV2(BaseModel):
    pkg_name: NonEmptyStr
    version: NonEmptyStr | None = None
    name: NonEmptyStr
