from datetime import datetime
from typing import Optional

from sqlalchemy import Column
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from . import LinkUserProjectV2
from ....utils import get_timestamp
from ..security import UserOAuth


class ProjectV2(SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

    user_list: list[UserOAuth] = Relationship(
        link_model=LinkUserProjectV2,
        sa_relationship_kwargs={
            "lazy": "selectin",
        },
    )
