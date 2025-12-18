from datetime import datetime

from pydantic import BaseModel
from pydantic import field_serializer
from pydantic.types import AwareDatetime


class AccountingRecordRead(BaseModel):
    """
    AccountingRecordRead

    Attributes:
        id:
        user_id:
        timestamp:
        num_tasks:
        num_new_images:
    """

    id: int
    user_id: int
    timestamp: AwareDatetime
    num_tasks: int
    num_new_images: int

    @field_serializer("timestamp")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()
