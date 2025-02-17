from datetime import datetime

from sqlalchemy import Column
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import SQLModel

from fractal_server.utils import get_timestamp


class LinkUserGroup(SQLModel, table=True):
    """
    Crossing table between User and UserGroup
    """

    group_id: int = Field(foreign_key="usergroup.id", primary_key=True)
    user_id: int = Field(foreign_key="user_oauth.id", primary_key=True)

    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
