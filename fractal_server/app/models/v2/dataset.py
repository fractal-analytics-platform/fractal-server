from datetime import datetime
from typing import Any
from typing import Optional

from sqlalchemy import Column
from sqlalchemy.types import DateTime
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from ....utils import get_timestamp


class DatasetV2(SQLModel, table=True):
    class Config:
        arbitrary_types_allowed = True

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str

    project_id: int = Field(foreign_key="project.id")
    project: "Project" = Relationship(  # noqa: F821
        sa_relationship_kwargs=dict(lazy="selectin"),
    )

    meta: dict[str, Any] = Field(sa_column=Column(JSON), default={})
    history: list[dict[str, Any]] = Field(
        sa_column=Column(JSON, server_default="[]", nullable=False)
    )
    read_only: bool = False

    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

    # New in V2

    images: list[dict[str, Any]] = Field(
        sa_column=Column(JSON, server_default="[]", nullable=False)
    )
    filters: dict[str, Any] = Field(
        sa_column=Column(JSON, server_default="{}", nullable=False)
    )
    buffer: Optional[dict[str, Any]] = Field(
        sa_column=Column(JSON, nullable=True)
    )
    parallelization_list: Optional[list[dict[str, Any]]] = Field(
        sa_column=Column(JSON, nullable=True)
    )

    @property
    def image_paths(self) -> list[str]:
        return [image["path"] for image in self.images]
