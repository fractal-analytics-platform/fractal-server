from datetime import datetime

from sqlalchemy import Column
from sqlalchemy.types import DateTime
from sqlmodel import BOOLEAN
from sqlmodel import Field
from sqlmodel import SQLModel

from fractal_server.utils import get_timestamp


class ProjectV2(SQLModel, table=True):
    """
    Project table.
    """

    id: int | None = Field(default=None, primary_key=True)
    name: str

    is_starred: bool = Field(
        sa_column=Column(
            BOOLEAN,
            server_default="false",
            nullable=False,
        ),
    )

    resource_id: int = Field(foreign_key="resource.id", ondelete="RESTRICT")
    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
