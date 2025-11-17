from datetime import datetime

from sqlalchemy import Column
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import SQLModel

from fractal_server.utils import get_timestamp


class ProjectV2(SQLModel, table=True):
    """
    Project table.
    """

    id: int | None = Field(default=None, primary_key=True)
    name: str

    resource_id: int = Field(foreign_key="resource.id", ondelete="RESTRICT")
    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
