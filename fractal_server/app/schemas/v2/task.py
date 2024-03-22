from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import HttpUrl
from pydantic import validator

from .._validators import valstr


class TaskCreateV2(BaseModel):

    name: str

    command_non_parallel: Optional[str]
    command_parallel: Optional[str]
    source: str

    meta: Optional[dict[str, Any]] = Field(default={})
    version: Optional[str]
    args_schema: Optional[dict[str, Any]]
    args_schema_version: Optional[str]
    docs_info: Optional[str]
    docs_link: Optional[HttpUrl]

    input_types: dict[str, bool] = Field(default={})
    output_types: dict[str, bool] = Field(default={})

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))
    _command_non_parallel = validator(
        "command_non_parallel", allow_reuse=True
    )(valstr("command_non_parallel"))
    _command_parallel = validator("command_parallel", allow_reuse=True)(
        valstr("command_parallel")
    )
    _source = validator("source", allow_reuse=True)(valstr("source"))
    _version = validator("version", allow_reuse=True)(valstr("version"))
    _args_schema_version = validator("args_schema_version", allow_reuse=True)(
        valstr("args_schema_version")
    )


class TaskReadV2(BaseModel):

    id: int
    name: str

    command_non_parallel: Optional[str]
    command_parallel: Optional[str]
    source: str
    meta: dict[str, Any]
    owner: Optional[str]
    version: Optional[str]
    args_schema: Optional[dict[str, Any]]
    args_schema_version: Optional[str]
    docs_info: Optional[str]
    docs_link: Optional[HttpUrl]
    input_types: dict[str, bool]
    output_types: dict[str, bool]


class TaskUpdateV2(BaseModel):

    name: Optional[str]
    command_non_parallel: Optional[str]
    command_parallel: Optional[str]
    source: Optional[str]
    meta: Optional[dict[str, Any]]
    version: Optional[str]
    args_schema: Optional[dict[str, Any]]
    args_schema_version: Optional[str]
    docs_info: Optional[str]
    docs_link: Optional[HttpUrl]
    input_types: Optional[dict[str, bool]]
    output_types: Optional[dict[str, bool]]

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))
    _command_non_parallel = validator(
        "command_non_parallel", allow_reuse=True
    )(valstr("command_non_parallel"))
    _command_parallel = validator("command_parallel", allow_reuse=True)(
        valstr("command_parallel")
    )

    _source = validator("source", allow_reuse=True)(valstr("source"))
    _version = validator("version", allow_reuse=True)(
        valstr("version", accept_none=True)
    )


class TaskImportV2(BaseModel):

    source: str
    _source = validator("source", allow_reuse=True)(valstr("source"))


class TaskExportV2(BaseModel):

    source: str
    _source = validator("source", allow_reuse=True)(valstr("source"))
