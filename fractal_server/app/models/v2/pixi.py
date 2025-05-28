from datetime import datetime

from sqlalchemy import Column
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import SQLModel

from fractal_server.utils import get_timestamp


class PixiVersion(SQLModel, table=True):

    id: int | None = Field(default=None, primary_key=True)
    version: str = Field(unique=True)
    path: str = Field(unique=True)
    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
