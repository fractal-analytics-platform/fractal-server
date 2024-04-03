import json
import logging
from typing import Any
from typing import Optional

from pydantic import HttpUrl
from sqlalchemy import Column
from sqlalchemy import sql
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import SQLModel

from ...schemas.v1.task import _TaskBaseV1


class Task(_TaskBaseV1, SQLModel, table=True):
    """
    Task model

    Attributes:
        id: Primary key
        command: Executable command
        input_type: Expected type of input `Dataset`
        output_type: Expected type of output `Dataset`
        meta:
            Additional metadata related to execution (e.g. computational
            resources)
        source: inherited from `_TaskBase`
        name: inherited from `_TaskBase`
        args_schema: JSON schema of task arguments
        args_schema_version:
            label pointing at how the JSON schema of task arguments was
            generated
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    command: str
    source: str = Field(unique=True)
    input_type: str
    output_type: str
    meta: Optional[dict[str, Any]] = Field(sa_column=Column(JSON), default={})
    owner: Optional[str] = None
    version: Optional[str] = None
    args_schema: Optional[dict[str, Any]] = Field(
        sa_column=Column(JSON), default=None
    )
    args_schema_version: Optional[str]
    docs_info: Optional[str] = None
    docs_link: Optional[HttpUrl] = None

    is_v2_compatible: bool = Field(
        default=False, sa_column_kwargs={"server_default": sql.false()}
    )

    @property
    def parallelization_level(self) -> Optional[str]:
        try:
            return self.meta["parallelization_level"]
        except KeyError:
            return None

    @property
    def is_parallel(self) -> bool:
        return bool(self.parallelization_level)

    @property
    def default_args_from_args_schema(self) -> dict[str, Any]:
        """
        Extract default arguments from args_schema
        """
        # Return {} if there is no args_schema
        if self.args_schema is None:
            return {}
        # Try to construct default_args
        try:
            default_args = {}
            properties = self.args_schema["properties"]
            for prop_name, prop_schema in properties.items():
                default_value = prop_schema.get("default", None)
                if default_value is not None:
                    default_args[prop_name] = default_value
            return default_args
        except KeyError as e:
            logging.warning(
                "Cannot set default_args from args_schema="
                f"{json.dumps(self.args_schema)}\n"
                f"Original KeyError: {str(e)}"
            )
            return {}
