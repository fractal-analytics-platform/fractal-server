from datetime import datetime
from typing import Optional

from sqlalchemy import Column
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import SQLModel

from ....utils import get_timestamp
from ...schemas.v1.project import _ProjectBaseV1


class Project(_ProjectBaseV1, SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
