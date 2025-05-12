from datetime import datetime

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import SQLModel

from ....utils import get_timestamp


class AccountingRecord(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user_oauth.id", nullable=False)
    timestamp: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    num_tasks: int
    num_new_images: int


class AccountingRecordSlurm(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user_oauth.id", nullable=False)
    timestamp: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    slurm_job_ids: list[int] = Field(
        default_factory=list,
        sa_column=Column(ARRAY(Integer)),
    )
