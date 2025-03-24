from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import HttpUrl
from pydantic import model_validator

from .._validators import cant_set_none
from fractal_server.app.schemas._validators import NonEmptyString
from fractal_server.app.schemas._validators import val_unique_list
from fractal_server.app.schemas._validators import valdict_keys
from fractal_server.logger import set_logger
from fractal_server.string_tools import validate_cmd

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

    name: NonEmptyString

    command_non_parallel: Optional[NonEmptyString] = None
    command_parallel: Optional[NonEmptyString] = None

    meta_non_parallel: Optional[dict[str, Any]] = None
    meta_parallel: Optional[dict[str, Any]] = None
    version: Optional[NonEmptyString] = None
    args_schema_non_parallel: Optional[dict[str, Any]] = None
    args_schema_parallel: Optional[dict[str, Any]] = None
    args_schema_version: Optional[NonEmptyString] = None
    docs_info: Optional[str] = None
    docs_link: Optional[str] = None

    input_types: dict[str, bool] = Field(default={})
    output_types: dict[str, bool] = Field(default={})

    category: Optional[NonEmptyString] = None
    modality: Optional[NonEmptyString] = None
    tags: list[NonEmptyString] = Field(default_factory=list)
    authors: Optional[NonEmptyString] = None

    type: Optional[TaskTypeType] = None

    # Validators

    @field_validator(
        "command_non_parallel",
        "command_parallel",
        "version",
        "args_schema_version",
    )
    @classmethod
    def _cant_set_none(cls, v):
        return cant_set_none(v)

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

    _meta_non_parallel = field_validator("meta_non_parallel")(
        classmethod(valdict_keys("meta_non_parallel"))
    )
    _meta_parallel = field_validator("meta_parallel")(
        classmethod(valdict_keys("meta_parallel"))
    )
    _args_schema_non_parallel = field_validator("args_schema_non_parallel")(
        classmethod(valdict_keys("args_schema_non_parallel"))
    )
    _args_schema_parallel = field_validator("args_schema_parallel")(
        classmethod(valdict_keys("args_schema_parallel"))
    )
    _input_types = field_validator("input_types")(
        classmethod(valdict_keys("input_types"))
    )
    _output_types = field_validator("output_types")(
        classmethod(valdict_keys("output_types"))
    )

    @field_validator("tags")
    @classmethod
    def validate_list_of_strings(cls, value):
        return val_unique_list("tags")(cls, value)

    @field_validator("docs_link", mode="after")
    @classmethod
    def validate_docs_link(cls, value):
        if value is not None:
            HttpUrl(value)
        return value


class TaskReadV2(BaseModel):
    id: int
    name: str
    type: Literal["parallel", "non_parallel", "compound"]
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

    type: Optional[TaskTypeType] = None


class TaskUpdateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command_parallel: Optional[NonEmptyString] = None
    command_non_parallel: Optional[NonEmptyString] = None
    input_types: Optional[dict[str, bool]] = None
    output_types: Optional[dict[str, bool]] = None

    category: Optional[NonEmptyString] = None
    modality: Optional[NonEmptyString] = None
    authors: Optional[NonEmptyString] = None
    tags: Optional[list[NonEmptyString]] = None

    # Validators

    @field_validator("command_parallel", "command_non_parallel")
    @classmethod
    def _cant_set_none(cls, v):
        return cant_set_none(v)

    @field_validator("input_types", "output_types")
    @classmethod
    def val_is_dict(cls, v):
        if not isinstance(v, dict):
            raise ValueError
        return v

    _input_types = field_validator("input_types")(
        classmethod(valdict_keys("input_types"))
    )
    _output_types = field_validator("output_types")(
        classmethod(valdict_keys("output_types"))
    )

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, value):
        return val_unique_list("tags")(cls, value)


class TaskImportV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pkg_name: NonEmptyString
    version: Optional[NonEmptyString] = None
    name: NonEmptyString


class TaskImportV2Legacy(BaseModel):
    source: NonEmptyString


class TaskExportV2(BaseModel):
    pkg_name: NonEmptyString
    version: Optional[NonEmptyString] = None
    name: NonEmptyString
