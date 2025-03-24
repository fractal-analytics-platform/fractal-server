from datetime import datetime
from enum import Enum
from typing import Any
from typing import Optional

from pydantic import AwareDatetime
from pydantic import BaseModel
from pydantic import field_serializer


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


class HistoryUnitRead(BaseModel):
    id: int
    logfile: Optional[str] = None
    status: HistoryUnitStatus
    zarr_urls: list[str]


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


class ZarrUrlAndStatus(BaseModel):
    zarr_url: str
    status: Optional[HistoryUnitStatus] = None
