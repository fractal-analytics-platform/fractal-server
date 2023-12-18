from typing import Optional

from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from ..schemas.project import _ProjectBase
from .linkuserproject import LinkUserProject
from .security import UserOAuth


class Project(_ProjectBase, SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    user_list: list[UserOAuth] = Relationship(
        link_model=LinkUserProject,
        back_populates="project_list",
        sa_relationship_kwargs={
            "lazy": "selectin",
        },
    )
