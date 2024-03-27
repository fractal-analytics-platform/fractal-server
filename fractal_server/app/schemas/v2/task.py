from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import HttpUrl
from pydantic import validator

from .._validators import valstr


class TaskCreateV2(BaseModel):
    class Config:
        extra = "forbid"

    name: str

    command_non_parallel: Optional[str]
    command_parallel: Optional[str]
    source: str

    meta: Optional[dict[str, Any]] = Field(default={})
    version: Optional[str]
    args_schema_non_parallel: Optional[dict[str, Any]]
    args_schema_parallel: Optional[dict[str, Any]]
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
    type: Literal["parallel", "non parallel", "compound"]

    command_non_parallel: Optional[str]
    command_parallel: Optional[str]
    source: str
    meta: dict[str, Any]
    owner: Optional[str]
    version: Optional[str]
    args_schema_non_parallel: Optional[dict[str, Any]] = None
    args_schema_parallel: Optional[dict[str, Any]] = None
    args_schema_version: Optional[str]
    docs_info: Optional[str]
    docs_link: Optional[HttpUrl]
    input_types: dict[str, bool]
    output_types: dict[str, bool]


class TaskUpdateV2(BaseModel):

    name: Optional[str]
    version: Optional[str]
    command_parallel: Optional[str]
    command_non_parallel: Optional[str]
    input_types: Optional[dict[str, bool]]
    output_types: Optional[dict[str, bool]]

    # source: Optional[str]
    # meta: Optional[dict[str, Any]]
    # args_schema_parallel: Optional[dict[str, Any]] = None
    # args_schema_non_parallel: Optional[dict[str, Any]] = None
    # args_schema_version: Optional[str]
    # docs_info: Optional[str]
    # docs_link: Optional[HttpUrl]
    # _source = validator("source", allow_reuse=True)(valstr("source"))

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


class TaskImportV2(BaseModel):

    source: str
    _source = validator("source", allow_reuse=True)(valstr("source"))


class TaskExportV2(BaseModel):

    source: str
    _source = validator("source", allow_reuse=True)(valstr("source"))
