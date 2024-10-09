from datetime import datetime
from typing import Any
from typing import Optional

from pydantic import HttpUrl
from sqlalchemy import Column
from sqlalchemy.types import DateTime
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from fractal_server.utils import get_timestamp


class TaskV2(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str

    type: str
    command_non_parallel: Optional[str] = None
    command_parallel: Optional[str] = None
    source: Optional[str] = None

    meta_non_parallel: dict[str, Any] = Field(
        sa_column=Column(JSON, server_default="{}", default={}, nullable=False)
    )
    meta_parallel: dict[str, Any] = Field(
        sa_column=Column(JSON, server_default="{}", default={}, nullable=False)
    )

    owner: Optional[str] = None
    version: Optional[str] = None
    args_schema_non_parallel: Optional[dict[str, Any]] = Field(
        sa_column=Column(JSON), default=None
    )
    args_schema_parallel: Optional[dict[str, Any]] = Field(
        sa_column=Column(JSON), default=None
    )
    args_schema_version: Optional[str]
    docs_info: Optional[str] = None
    docs_link: Optional[HttpUrl] = None

    input_types: dict[str, bool] = Field(sa_column=Column(JSON), default={})
    output_types: dict[str, bool] = Field(sa_column=Column(JSON), default={})

    taskgroupv2_id: Optional[int] = Field(foreign_key="taskgroupv2.id")

    category: Optional[str] = None
    modality: Optional[str] = None
    authors: Optional[str] = None
    tags: list[str] = Field(
        sa_column=Column(JSON, server_default="[]", nullable=False)
    )


class TaskGroupV2(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    task_list: list[TaskV2] = Relationship(
        sa_relationship_kwargs=dict(
            lazy="selectin", cascade="all, delete-orphan"
        ),
    )

    user_id: int = Field(foreign_key="user_oauth.id")
    user_group_id: Optional[int] = Field(foreign_key="usergroup.id")

    origin: str
    pkg_name: str
    version: Optional[str] = None
    python_version: Optional[str] = None
    path: Optional[str] = None
    venv_path: Optional[str] = None
    wheel_path: Optional[str] = None
    pip_extras: Optional[str] = None
    pinned_package_versions: dict[str, str] = Field(
        sa_column=Column(
            JSON,
            server_default="{}",
            default={},
            nullable=True,
        ),
    )

    active: bool = True
    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

    @property
    def pip_install_string(self) -> str:
        """
        Prepare string to be used in `python -m pip install`.
        """
        extras = f"[{self.pip_extras}]" if self.pip_extras is not None else ""

        if self.wheel_path is not None:
            return f"{self.wheel_path}{extras}"
        else:
            if self.version is None:
                raise ValueError(
                    "Cannot run `pip_install_string` with "
                    f"{self.pkg_name=}, {self.wheel_path=}, {self.version=}."
                )
            return f"{self.pkg_name}{extras}=={self.version}"
