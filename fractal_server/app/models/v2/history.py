from datetime import datetime
from typing import Any
from typing import Optional

from pydantic import ConfigDict
from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import SQLModel

from ....utils import get_timestamp


class HistoryRun(SQLModel, table=True):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: Optional[int] = Field(default=None, primary_key=True)
    dataset_id: int = Field(foreign_key="datasetv2.id")
    workflowtask_id: Optional[int] = Field(
        foreign_key="workflowtaskv2.id", default=None
    )

    workflowtask_dump: dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False)
    )
    task_group_dump: dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False)
    )

    timestamp_started: datetime = Field(
        sa_column=Column(DateTime(timezone=True), nullable=False),
        default_factory=get_timestamp,
    )
    status: str
    num_current_images: int


class HistoryUnit(SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    history_run_id: int = Field(foreign_key="historyrun.id")

    logfile: Optional[str]
    status: str
    zarr_urls: list[str] = Field(
        sa_column=Column(ARRAY(String)), default_factory=list
    )


class HistoryImageCache(SQLModel, table=True):

    zarr_url: str = Field(primary_key=True)
    dataset_id: int = Field(primary_key=True, foreign_key="datasetv2.id")
    workflowtask_id: int = Field(
        primary_key=True, foreign_key="workflowtaskv2.id"
    )

    latest_history_unit_id: int = Field(foreign_key="historyunit.id")
