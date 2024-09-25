from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import HttpUrl
from pydantic import validator

from .._validators import valstr

__all__ = (
    "TaskReadV1",
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
