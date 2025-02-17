from datetime import datetime
from typing import Optional

from sqlalchemy import Column
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from ....utils import get_timestamp
from .workflowtask import WorkflowTaskV2


class WorkflowV2(SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    project_id: int = Field(foreign_key="projectv2.id")
    project: "ProjectV2" = Relationship(  # noqa: F821
        sa_relationship_kwargs=dict(lazy="selectin"),
    )

    task_list: list[WorkflowTaskV2] = Relationship(
        sa_relationship_kwargs=dict(
            lazy="selectin",
            order_by="WorkflowTaskV2.order",
            collection_class=ordering_list("order"),
            cascade="all, delete-orphan",
        ),
    )
    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
