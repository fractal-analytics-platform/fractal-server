from datetime import datetime

from pydantic import BaseModel
from pydantic import field_serializer
from pydantic.types import AwareDatetime

# from typing import Any


class HistoryItemV2Read(BaseModel):
    id: int
    dataset_id: int
    workflowtask_id: int
    timestamp_started: AwareDatetime
    parameters_hash: str
    num_available_images: int
    num_current_images: int
    images: dict[str, str]

    # Provisionally commented, for simplicity
    # worfklowtask_dump: dict[str, Any]
    # task_group_dump: dict[str, Any]

    @field_serializer("timestamp_started")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()
