from datetime import datetime
from typing import Any
from typing import Optional

from sqlalchemy import Column
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.types import DateTime
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from ....utils import get_timestamp
from ...schemas.v1.dataset import _DatasetBaseV1
from ...schemas.v1.dataset import _ResourceBaseV1


class Resource(_ResourceBaseV1, SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    dataset_id: int = Field(foreign_key="dataset.id")


class Dataset(_DatasetBaseV1, SQLModel, table=True):
    """
    Represent a dataset

    Attributes:
        id:
            Primary key
        project_id:
            ID of the project the workflow belongs to.
        meta:
            Metadata of the Dataset
        history:
            History of the Dataset
        resource_list:
            (Mapper attribute)

    """

    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: int = Field(foreign_key="project.id")
    project: "Project" = Relationship(  # noqa: F821
        sa_relationship_kwargs=dict(lazy="selectin"),
    )

    resource_list: list[Resource] = Relationship(
        sa_relationship_kwargs={
            "lazy": "selectin",
            "order_by": "Resource.id",
            "collection_class": ordering_list("id"),
            "cascade": "all, delete-orphan",
        }
    )

    meta: dict[str, Any] = Field(sa_column=Column(JSON), default={})
    history: list[dict[str, Any]] = Field(
        sa_column=Column(JSON, server_default="[]", nullable=False)
    )

    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

    class Config:
        arbitrary_types_allowed = True

    @property
    def paths(self) -> list[str]:
        return [r.path for r in self.resource_list]
