from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from .._validators import valdictkeys
from .._validators import valstr
from ..v1.task import TaskReadV1


class TaskCreateV2(BaseModel):

    model_config = ConfigDict(extra="forbid")

    name: str

    command_non_parallel: Optional[str] = None
    command_parallel: Optional[str] = None
    source: str

    meta_non_parallel: Optional[dict[str, Any]] = None
    meta_parallel: Optional[dict[str, Any]] = None
    version: Optional[str] = None
    args_schema_non_parallel: Optional[dict[str, Any]] = None
    args_schema_parallel: Optional[dict[str, Any]] = None
    args_schema_version: Optional[str] = None
    docs_info: Optional[str] = None
    docs_link: Optional[str] = None

    input_types: dict[str, bool] = Field(default={})
    output_types: dict[str, bool] = Field(default={})

    # Validators
    @model_validator(mode="before")
    def validate_commands(cls, values):
        command_parallel = values.get("command_parallel")
        command_non_parallel = values.get("command_non_parallel")
        if (command_parallel is None) and (command_non_parallel is None):
            raise ValueError(
                "Task must have at least one valid command "
                "(parallel and/or non_parallel)"
            )
        return values

    _name = field_validator("name")(valstr("name"))
    _command_non_parallel = field_validator("command_non_parallel")(
        valstr("command_non_parallel")
    )
    _command_parallel = field_validator("command_parallel")(
        valstr("command_parallel")
    )
    _source = field_validator("source")(valstr("source"))
    _version = field_validator("version")(valstr("version"))

    _meta_non_parallel = field_validator("meta_non_parallel")(
        valdictkeys("meta_non_parallel")
    )
    _meta_parallel = field_validator("meta_parallel")(
        valdictkeys("meta_parallel")
    )
    _args_schema_non_parallel = field_validator("args_schema_non_parallel")(
        valdictkeys("args_schema_non_parallel")
    )
    _args_schema_parallel = field_validator("args_schema_parallel")(
        valdictkeys("args_schema_parallel")
    )
    _args_schema_version = field_validator("args_schema_version")(
        valstr("args_schema_version")
    )
    _input_types = field_validator("input_types")(valdictkeys("input_types"))
    _output_types = field_validator("output_types")(
        valdictkeys("output_types")
    )


class TaskReadV2(BaseModel):

    id: int
    name: str
    type: Literal["parallel", "non_parallel", "compound"]
    source: str
    owner: Optional[str] = None
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


class TaskLegacyReadV2(TaskReadV1):
    is_v2_compatible: bool


class TaskUpdateV2(BaseModel):

    name: Optional[str] = None
    version: Optional[str] = None
    command_parallel: Optional[str] = None
    command_non_parallel: Optional[str] = None
    input_types: Optional[dict[str, bool]] = None
    output_types: Optional[dict[str, bool]] = None

    # Validators
    @field_validator("input_types", "output_types")
    @classmethod
    def val_is_dict(cls, v):
        if not isinstance(v, dict):
            raise ValueError
        return v

    _name = field_validator("name")(valstr("name"))
    _version = field_validator("version")(valstr("version", accept_none=True))
    _command_parallel = field_validator("command_parallel")(
        valstr("command_parallel")
    )
    _command_non_parallel = field_validator("command_non_parallel")(
        valstr("command_non_parallel")
    )
    _input_types = field_validator("input_types")(valdictkeys("input_types"))
    _output_types = field_validator("output_types")(
        valdictkeys("output_types")
    )


class TaskImportV2(BaseModel):

    source: str
    _source = field_validator("source")(valstr("source"))


class TaskExportV2(BaseModel):

    source: str
    _source = field_validator("source")(valstr("source"))
