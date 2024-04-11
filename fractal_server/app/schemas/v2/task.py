from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic import HttpUrl
from pydantic import root_validator
from pydantic import validator

from .._validators import valdictkeys
from .._validators import valstr
from ..v1.task import TaskReadV1


class TaskCreateV2(BaseModel, extra=Extra.forbid):

    name: str

    command_non_parallel: Optional[str]
    command_parallel: Optional[str]
    source: str

    meta_non_parallel: Optional[dict[str, Any]]
    meta_parallel: Optional[dict[str, Any]]
    version: Optional[str]
    args_schema_non_parallel: Optional[dict[str, Any]]
    args_schema_parallel: Optional[dict[str, Any]]
    args_schema_version: Optional[str]
    docs_info: Optional[str]
    docs_link: Optional[HttpUrl]

    input_types: dict[str, bool] = Field(default={})
    output_types: dict[str, bool] = Field(default={})

    # Validators
    @root_validator
    def validate_commands(cls, values):
        command_parallel = values.get("command_parallel")
        command_non_parallel = values.get("command_non_parallel")
        if (command_parallel is None) and (command_non_parallel is None):
            raise ValueError(
                "Task must have at least one valid command "
                "(parallel and/or non_parallel)"
            )
        return values

    _name = validator("name", allow_reuse=True)(valstr("name"))
    _command_non_parallel = validator(
        "command_non_parallel", allow_reuse=True
    )(valstr("command_non_parallel"))
    _command_parallel = validator("command_parallel", allow_reuse=True)(
        valstr("command_parallel")
    )
    _source = validator("source", allow_reuse=True)(valstr("source"))
    _version = validator("version", allow_reuse=True)(valstr("version"))

    _meta_non_parallel = validator("meta_non_parallel", allow_reuse=True)(
        valdictkeys("meta_non_parallel")
    )
    _meta_parallel = validator("meta_parallel", allow_reuse=True)(
        valdictkeys("meta_parallel")
    )
    _args_schema_non_parallel = validator(
        "args_schema_non_parallel", allow_reuse=True
    )(valdictkeys("args_schema_non_parallel"))
    _args_schema_parallel = validator(
        "args_schema_parallel", allow_reuse=True
    )(valdictkeys("args_schema_parallel"))
    _args_schema_version = validator("args_schema_version", allow_reuse=True)(
        valstr("args_schema_version")
    )
    _input_types = validator("input_types", allow_reuse=True)(
        valdictkeys("input_types")
    )
    _output_types = validator("output_types", allow_reuse=True)(
        valdictkeys("output_types")
    )


class TaskReadV2(BaseModel):

    id: int
    name: str
    type: Literal["parallel", "non_parallel", "compound"]
    source: str
    owner: Optional[str]
    version: Optional[str]

    command_non_parallel: Optional[str]
    command_parallel: Optional[str]
    meta_parallel: dict[str, Any]
    meta_non_parallel: dict[str, Any]
    args_schema_non_parallel: Optional[dict[str, Any]] = None
    args_schema_parallel: Optional[dict[str, Any]] = None
    args_schema_version: Optional[str]
    docs_info: Optional[str]
    docs_link: Optional[HttpUrl]
    input_types: dict[str, bool]
    output_types: dict[str, bool]


class TaskLegacyReadV2(TaskReadV1):
    is_v2_compatible: bool


class TaskUpdateV2(BaseModel):

    name: Optional[str]
    version: Optional[str]
    command_parallel: Optional[str]
    command_non_parallel: Optional[str]
    input_types: Optional[dict[str, bool]]
    output_types: Optional[dict[str, bool]]

    # Validators
    @validator("input_types", "output_types")
    def val_is_dict(cls, v):
        if not isinstance(v, dict):
            raise ValueError
        return v

    _name = validator("name", allow_reuse=True)(valstr("name"))
    _version = validator("version", allow_reuse=True)(
        valstr("version", accept_none=True)
    )
    _command_parallel = validator("command_parallel", allow_reuse=True)(
        valstr("command_parallel")
    )
    _command_non_parallel = validator(
        "command_non_parallel", allow_reuse=True
    )(valstr("command_non_parallel"))
    _input_types = validator("input_types", allow_reuse=True)(
        valdictkeys("input_types")
    )
    _output_types = validator("output_types", allow_reuse=True)(
        valdictkeys("output_types")
    )


class TaskImportV2(BaseModel):

    source: str
    _source = validator("source", allow_reuse=True)(valstr("source"))


class TaskExportV2(BaseModel):

    source: str
    _source = validator("source", allow_reuse=True)(valstr("source"))
