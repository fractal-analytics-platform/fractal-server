from typing import Optional

from sqlalchemy.ext.orderinglist import ordering_list
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from ..schemas.project import _ProjectBase
from .dataset import Dataset
from .job import ApplyWorkflow
from .linkuserproject import LinkUserProject
from .security import UserOAuth
from .workflow import Workflow


class Project(_ProjectBase, SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    user_list: list[UserOAuth] = Relationship(
        link_model=LinkUserProject,
        back_populates="project_list",
        sa_relationship_kwargs={
            "lazy": "selectin",
            "order_by": "UserOAuth.email",
            "collection_class": ordering_list("email"),
        },
    )

    dataset_list: list[Dataset] = Relationship(
        sa_relationship_kwargs={
            "lazy": "selectin",
            "cascade": "all, delete-orphan",
            "order_by": "Dataset.name",
            "collection_class": ordering_list("name"),
        }
    )

    workflow_list: list[Workflow] = Relationship(
        sa_relationship_kwargs={
            "lazy": "selectin",
            "cascade": "all, delete-orphan",
            "order_by": "Workflow.name",
            "collection_class": ordering_list("name"),
        },
    )

    job_list: list[ApplyWorkflow] = Relationship(
        sa_relationship_kwargs={
            "lazy": "selectin",
        }
    )
