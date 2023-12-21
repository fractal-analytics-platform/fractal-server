from datetime import datetime
from typing import Optional

from sqlalchemy import Column
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from ...utils import get_timestamp
from ..schemas.project import _ProjectBase
from .linkuserproject import LinkUserProject
from .security import UserOAuth


class Project(_ProjectBase, SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

    user_list: list[UserOAuth] = Relationship(
        link_model=LinkUserProject,
        back_populates="project_list",
        sa_relationship_kwargs={
            "lazy": "selectin",
        },
    )
