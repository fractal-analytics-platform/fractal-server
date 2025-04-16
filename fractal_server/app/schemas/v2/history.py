from datetime import datetime
from enum import Enum
from typing import Any
from typing import Optional

from pydantic import AwareDatetime
from pydantic import BaseModel
from pydantic import field_serializer

from ....images import SingleImage


class HistoryUnitStatus(str, Enum):
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


class HistoryUnitStatusQuery(str, Enum):

    SUBMITTED = "submitted"
    DONE = "done"
    FAILED = "failed"

    UNSET = "unset"


class HistoryUnitRead(BaseModel):
    id: int
    logfile: Optional[str] = None
    status: HistoryUnitStatus
    zarr_urls: list[str]


class HistoryRunRead(BaseModel):
    id: int
    dataset_id: int
    workflowtask_id: Optional[int] = None
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

    @field_serializer("timestamp_started")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()


class ImageLogsRequest(BaseModel):
    workflowtask_id: int
    dataset_id: int
    zarr_url: str


class SingleImageWithStatus(SingleImage):
    status: Optional[HistoryUnitStatus] = None
