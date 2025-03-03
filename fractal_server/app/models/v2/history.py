from datetime import datetime
from typing import Any
from typing import Optional

from pydantic import ConfigDict
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import SQLModel

from ....utils import get_timestamp


class HistoryItemV2(SQLModel, table=True):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: Optional[int] = Field(default=None, primary_key=True)
    dataset_id: int = Field(foreign_key="datasetv2.id")
    workflowtask_id: Optional[int] = Field(
        foreign_key="workflowtaskv2.id",
        default=None,
    )
    timestamp_started: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(
            DateTime(timezone=True),
            nullable=False,
        ),
    )
    workflowtask_dump: dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False)
    )
    task_group_dump: dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False)
    )
    parameters_hash: str
    num_available_images: int
    num_current_images: int
    images: dict[str, str] = Field(sa_column=Column(JSONB, nullable=False))


class ImageStatus(SQLModel, table=True):

    zarr_url: str = Field(primary_key=True)
    workflowtask_id: int = Field(
        primary_key=True, foreign_key="workflowtaskv2.id"
    )
    dataset_id: int = Field(primary_key=True, foreign_key="datasetv2.id")

    parameters_hash: str
    status: str
    logfile: Optional[str]
