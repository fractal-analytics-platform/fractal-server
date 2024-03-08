import json
import logging
from typing import Any
from typing import Optional

from pydantic import HttpUrl
from sqlalchemy import Column
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import SQLModel


class TaskV2(SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str

    type: str  # Literal[“parallel”, “non_parallel”, “compound”]

    command_pre: Optional[str] = None
    command: str
    source: str = Field(unique=True)

    meta: Optional[dict[str, Any]] = Field(sa_column=Column(JSON), default={})

    owner: Optional[str] = None
    version: Optional[str] = None
    args_schema: Optional[dict[str, Any]] = Field(
        sa_column=Column(JSON), default=None
    )
    args_schema_version: Optional[str]
    docs_info: Optional[str] = None
    docs_link: Optional[HttpUrl] = None

    new_filters: dict[str, Any] = Field(sa_column=Column(JSON), default={})

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
