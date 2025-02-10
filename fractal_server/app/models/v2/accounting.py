from datetime import datetime
from typing import Optional

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from ....utils import get_timestamp
from ..security import UserOAuth


class Accounting(SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user_oauth.id", nullable=False)
    user: UserOAuth = Relationship(back_populates="oauth_accounts")
    timestamp: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

    num_tasks: int
    num_new_images: int


class AccountingSlurm(SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user_oauth.id", nullable=False)
    user: UserOAuth = Relationship(back_populates="oauth_accounts")
    timestamp: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

    slurm_job_ids: list[int] = Field(
        default_factory=list, sa_column=Column(ARRAY(Integer))
    )
