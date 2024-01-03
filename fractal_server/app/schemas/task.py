from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator
from pydantic import HttpUrl

from ._validators import valstr

__all__ = (
    "TaskCreate",
    "TaskUpdate",
    "TaskRead",
    "TaskImport",
    "TaskExport",
)


class _TaskBase(BaseModel):
    """

    Base class for `Task`.

    Attributes:
        source:
            This is the information is used to match tasks across fractal
            installations when a workflow is imported.
    """

    source: str
    _source = field_validator("source")(valstr("source"))


class TaskUpdate(_TaskBase):
    """
    Class for `Task` update.

    Attributes:
        name:
        input_type:
        output_type:
        command:
        source:
        meta:
        version:
        args_schema:
        args_schema_version:
        docs_info:
        docs_link:
    """

    name: Optional[str] = None
    input_type: Optional[str] = None
    output_type: Optional[str] = None
    command: Optional[str] = None
    source: Optional[str] = None
    meta: Optional[dict[str, Any]] = None
    version: Optional[str] = None
    args_schema: Optional[dict[str, Any]] = None
    args_schema_version: Optional[str] = None
    docs_info: Optional[str] = None
    docs_link: Optional[HttpUrl] = None

    # Validators
    _name = field_validator("name")(valstr("name"))
    _input_type = field_validator("input_type")(valstr("input_type"))
    _output_type = field_validator("output_type")(valstr("output_type"))
    _command = field_validator("command")(valstr("command"))
    _version = field_validator("version")(valstr("version", accept_none=True))


class TaskImport(_TaskBase):
    """
    Class for `Task` import.
    """

    pass


class TaskExport(_TaskBase):
    """
    Class for `Task` export.
    """

    pass


class TaskRead(_TaskBase):
    """
    Class for `Task` read from database.

    Attributes:
        id:
        name:
        command:
        input_type:
        output_type:
        meta:
        version:
        args_schema:
        args_schema_version:
        docs_info:
        docs_link:
    """

    id: int
    name: str
    command: str
    input_type: str
    output_type: str
    meta: Optional[dict[str, Any]] = Field(default={})
    owner: Optional[str] = None
    version: Optional[str] = None
    args_schema: Optional[dict[str, Any]] = None
    args_schema_version: Optional[str] = None
    docs_info: Optional[str] = None
    docs_link: Optional[HttpUrl] = None


class TaskCreate(_TaskBase):
    """
    Class for `Task` creation.

    Attributes:
        name:
        command:
        input_type:
        output_type:
        meta:
        version:
        args_schema:
        args_schema_version:
        docs_info:
        docs_link:
    """

    name: str
    command: str
    input_type: str
    output_type: str
    meta: Optional[dict[str, Any]] = Field(default={})
    version: Optional[str] = None
    args_schema: Optional[dict[str, Any]] = None
    args_schema_version: Optional[str] = None
    docs_info: Optional[str] = None
    docs_link: Optional[HttpUrl] = None

    # Validators
    _name = field_validator("name")(valstr("name"))
    _input_type = field_validator("input_type")(valstr("input_type"))
    _output_type = field_validator("output_type")(valstr("output_type"))
    _command = field_validator("command")(valstr("command"))
    _version = field_validator("version")(valstr("version"))
    _args_schema_version = field_validator("args_schema_version")(
        valstr("args_schema_version")
    )
