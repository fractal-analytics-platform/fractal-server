from datetime import datetime

from sqlalchemy import BOOLEAN
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.types import DateTime

from fractal_server.app.models.base import Base
from fractal_server.utils import get_timestamp


class ProjectV2(Base):
    """
    Project table.
    """

    __tablename__ = "projectv2"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()

    is_starred: Mapped[bool] = mapped_column(
        BOOLEAN,
        server_default="false",
        nullable=False,
    )
    description: Mapped[str | None] = mapped_column(default=lambda: None)

    resource_id: Mapped[int] = mapped_column(
        ForeignKey("resource.id", ondelete="RESTRICT")
    )
    timestamp_created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=get_timestamp
    )
