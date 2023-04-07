from typing import Any
from typing import Optional

from pydantic import UUID4
from sqlalchemy import Column
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from ...common.schemas.project import _DatasetBase
from ...common.schemas.project import _ProjectBase
from ...common.schemas.project import _ResourceBase
from .security import UserOAuth as User
from .workflow import Workflow


class LinkUserProject(SQLModel, table=True):
    """
    Crossing table between User and Project
    """

    project_id: int = Field(foreign_key="project.id", primary_key=True)
    user_id: UUID4 = Field(foreign_key="user_oauth.id", primary_key=True)


class Dataset(_DatasetBase, table=True):
    """
    Represent a dataset

    Attributes:
        id:
            Primary key
        project_id:
            ID of the project the workflow belongs to.
        meta:
            Metadata of the Dataset

        resource_list:
            (Mapper attribute)

    """

    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: int = Field(foreign_key="project.id")
    resource_list: list["Resource"] = Relationship(
        sa_relationship_kwargs={
            "lazy": "selectin",
            "cascade": "all, delete-orphan",
        }
    )
    meta: dict[str, Any] = Field(sa_column=Column(JSON), default={})

    class Config:
        arbitrary_types_allowed = True

    @property
    def paths(self) -> list[str]:
        return [r.path for r in self.resource_list]


class Project(_ProjectBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    user_member_list: list[User] = Relationship(
        link_model=LinkUserProject,
        sa_relationship_kwargs={
            "lazy": "selectin",
        },
    )

    dataset_list: list[Dataset] = Relationship(
        sa_relationship_kwargs={
            "lazy": "selectin",
            "cascade": "all, delete-orphan",
        }
    )

    workflow_list: list[Workflow] = Relationship(
        sa_relationship_kwargs={
            "lazy": "selectin",
            "cascade": "all, delete-orphan",
        },
    )


class Resource(_ResourceBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    dataset_id: int = Field(foreign_key="dataset.id")
