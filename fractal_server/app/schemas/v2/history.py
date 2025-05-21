from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import AwareDatetime
from pydantic import BaseModel
from pydantic import field_serializer


class HistoryUnitStatus(StrEnum):
    """
    Available status for images

    Attributes:
        SUBMITTED:
        DONE:
        FAILED:
    """

    SUBMITTED = "submitted"
    DONE = "done"
    FAILED = "failed"


class HistoryUnitStatusWithUnset(StrEnum):
    """
    Available status for history queries

    Attributes:
        SUBMITTED:
        DONE:
        FAILED:
    """

    SUBMITTED = "submitted"
    DONE = "done"
    FAILED = "failed"
    UNSET = "unset"


class HistoryUnitRead(BaseModel):
    id: int
    logfile: str | None = None
    status: HistoryUnitStatus
    zarr_urls: list[str]


class HistoryRunRead(BaseModel):
    id: int
    dataset_id: int
    workflowtask_id: int | None = None
    job_id: int
    workflowtask_dump: dict[str, Any]
    task_group_dump: dict[str, Any]
    timestamp_started: AwareDatetime
    status: HistoryUnitStatus
    num_available_images: int

    @field_serializer("timestamp_started")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()


class HistoryRunReadAggregated(BaseModel):
    id: int
    timestamp_started: AwareDatetime
    workflowtask_dump: dict[str, Any]
    num_submitted_units: int
    num_done_units: int
    num_failed_units: int
    args_schema_parallel: dict[str, Any] | None = None
    args_schema_non_parallel: dict[str, Any] | None = None
    version: str | None = None

    @field_serializer("timestamp_started")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()


class ImageLogsRequest(BaseModel):
    workflowtask_id: int
    dataset_id: int
    zarr_url: str
