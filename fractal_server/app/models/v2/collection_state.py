from datetime import datetime
from typing import Any
from typing import Optional

from sqlalchemy import Column
from sqlalchemy.types import DateTime
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import SQLModel

from ....utils import get_timestamp


class CollectionStateV2(SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    data: dict[str, Any] = Field(sa_column=Column(JSON), default={})
    timestamp: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True)),
    )
