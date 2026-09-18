from datetime import datetime
from typing import Any

from sqlalchemy import BOOLEAN
from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.types import DateTime

from fractal_server.app.models.base import Base
from fractal_server.utils import get_timestamp

from .project import ProjectV2


class DatasetV2(Base):
    """
    Dataset table.
    """

    __tablename__ = "datasetv2"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]

    project_id: Mapped[int] = mapped_column(
        ForeignKey("projectv2.id", ondelete="CASCADE")
    )
    project: Mapped["ProjectV2"] = relationship(lazy="selectin")
    is_starred: Mapped[bool] = mapped_column(
        BOOLEAN,
        server_default="false",
        nullable=False,
    )

    timestamp_created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=get_timestamp
    )

    zarr_dir: Mapped[str]
    images: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB, server_default="[]", nullable=False
    )

    @property
    def image_zarr_urls(self) -> list[str]:
        return [image["zarr_url"] for image in self.images]
