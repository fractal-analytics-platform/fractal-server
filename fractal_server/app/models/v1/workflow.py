from typing import Optional

from sqlmodel import Field
from sqlmodel import SQLModel


class WorkflowTask(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)


class Workflow(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
