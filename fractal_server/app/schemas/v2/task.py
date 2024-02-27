from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import HttpUrl
from pydantic import validator

from .._validators import valstr


class TaskCreateV2(BaseModel):

    name: str
    is_parallel: bool

    command: str
    source: str

    meta: Optional[dict[str, Any]] = Field(default={})
    version: Optional[str]
    args_schema: Optional[dict[str, Any]]
    args_schema_version: Optional[str]
    docs_info: Optional[str]
    docs_link: Optional[HttpUrl]

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))
    _command = validator("command", allow_reuse=True)(valstr("command"))
    _source = validator("source", allow_reuse=True)(valstr("source"))
    _version = validator("version", allow_reuse=True)(valstr("version"))
    _args_schema_version = validator("args_schema_version", allow_reuse=True)(
        valstr("args_schema_version")
    )


class TaskReadV2(BaseModel):

    id: int
    name: str
    is_parallel: bool
    command: str
    source: str
    meta: dict[str, Any]
    owner: Optional[str]
    version: Optional[str]
    args_schema: Optional[dict[str, Any]]
    args_schema_version: Optional[str]
    docs_info: Optional[str]
    docs_link: Optional[HttpUrl]


class TaskUpdateV2(BaseModel):

    name: Optional[str]
    is_parallel: Optional[bool]
    command: Optional[str]
    source: Optional[str]
    meta: Optional[dict[str, Any]]
    version: Optional[str]
    args_schema: Optional[dict[str, Any]]
    args_schema_version: Optional[str]
    docs_info: Optional[str]
    docs_link: Optional[HttpUrl]

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))
    _command = validator("command", allow_reuse=True)(valstr("command"))
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
