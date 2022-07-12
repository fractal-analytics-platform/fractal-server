from typing import List
from typing import Optional

from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel


class TaskBase(SQLModel):
    name: str = Field(sa_column_kwargs=dict(unique=True))
    module: str
    input_type: str
    output_type: str
    default_parameters: Optional[str]
    subtask_list: Optional[List["TaskBase"]] = Field(default=[])


class TaskCreate(TaskBase):
    pass


class AssTaskTask(SQLModel, table=True):  # type: ignore
    parent_task_id: Optional[int] = Field(
        default=None, foreign_key="task.id", primary_key=True
    )
    subtask_id: Optional[int] = Field(
        default=None, foreign_key="task.id", primary_key=True
    )


class Task(TaskBase, table=True):  # type: ignore
    id: Optional[int] = Field(default=None, primary_key=True)
    subtask_list: List["Task"] = Relationship(
        link_model=AssTaskTask,
        sa_relationship_kwargs=dict(
            primaryjoin="Task.id==AssTaskTask.parent_task_id",
            secondaryjoin="Task.id==AssTaskTask.subtask_id",
            lazy="selectin",
        ),
    )

    @property
    def callable(self):
        return self.module.partition(":")[2]

    @property
    def import_path(self):
        return self.module.partition(":")[0]


class TaskRead(TaskBase):
    id: int
    subtask_list: List["TaskRead"]
