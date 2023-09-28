from typing import Any
from typing import Optional

from pydantic import BaseModel
from sqlalchemy import Column
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from ..schemas.dataset import _DatasetBase
from ..schemas.dataset import _ResourceBase
from .workflow import WorkflowTaskStatusType


class Resource(_ResourceBase, SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    dataset_id: int = Field(foreign_key="dataset.id")


class DatasetStatusRead(BaseModel):
    """
    Response type for the
    `/project/{project_id}/dataset/{dataset_id}/status/` endpoint
    """

    status: Optional[
        dict[
            int,
            WorkflowTaskStatusType,
        ]
    ] = None


class Dataset(_DatasetBase, SQLModel, table=True):
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
    resource_list: list[Resource] = Relationship(
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
