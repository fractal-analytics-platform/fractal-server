from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import HttpUrl
from pydantic import validator

from .._validators import valstr

__all__ = (
    "TaskCreateV1",
    "TaskUpdateV1",
    "TaskReadV1",
    "TaskImportV1",
    "TaskExportV1",
)


class _TaskBaseV1(BaseModel):
    """

    Base class for `Task`.

    Attributes:
        source:
            This is the information is used to match tasks across fractal
            installations when a workflow is imported.
    """

    source: str
    _source = validator("source", allow_reuse=True)(valstr("source"))


class TaskUpdateV1(_TaskBaseV1):
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

    name: Optional[str]
    input_type: Optional[str]
    output_type: Optional[str]
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
    _input_type = validator("input_type", allow_reuse=True)(
        valstr("input_type")
    )
    _output_type = validator("output_type", allow_reuse=True)(
        valstr("output_type")
    )
    _command = validator("command", allow_reuse=True)(valstr("command"))
    _version = validator("version", allow_reuse=True)(
        valstr("version", accept_none=True)
    )


class TaskImportV1(_TaskBaseV1):
    """
    Class for `Task` import.
    """

    pass


class TaskExportV1(_TaskBaseV1):
    """
    Class for `Task` export.
    """

    pass


class TaskReadV1(_TaskBaseV1):
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
    owner: Optional[str]
    version: Optional[str]
    args_schema: Optional[dict[str, Any]]
    args_schema_version: Optional[str]
    docs_info: Optional[str]
    docs_link: Optional[HttpUrl]


class TaskCreateV1(_TaskBaseV1):
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
    version: Optional[str]
    args_schema: Optional[dict[str, Any]]
    args_schema_version: Optional[str]
    docs_info: Optional[str]
    docs_link: Optional[HttpUrl]

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))
    _input_type = validator("input_type", allow_reuse=True)(
        valstr("input_type")
    )
    _output_type = validator("output_type", allow_reuse=True)(
        valstr("output_type")
    )
    _command = validator("command", allow_reuse=True)(valstr("command"))
    _version = validator("version", allow_reuse=True)(valstr("version"))
    _args_schema_version = validator("args_schema_version", allow_reuse=True)(
        valstr("args_schema_version")
    )
