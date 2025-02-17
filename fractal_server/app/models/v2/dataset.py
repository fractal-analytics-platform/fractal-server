from datetime import datetime
from typing import Any
from typing import Optional

from pydantic import ConfigDict
from sqlalchemy import Column
from sqlalchemy.types import DateTime
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from ....utils import get_timestamp
from fractal_server.images.models import AttributeFiltersType


class DatasetV2(SQLModel, table=True):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str

    project_id: int = Field(foreign_key="projectv2.id")
    project: "ProjectV2" = Relationship(  # noqa: F821
        sa_relationship_kwargs=dict(lazy="selectin"),
    )

    history: list[dict[str, Any]] = Field(
        sa_column=Column(JSON, server_default="[]", nullable=False)
    )

    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

    # New in V2

    zarr_dir: str
    images: list[dict[str, Any]] = Field(
        sa_column=Column(JSON, server_default="[]", nullable=False)
    )

    type_filters: dict[str, bool] = Field(
        sa_column=Column(JSON, nullable=False, server_default="{}")
    )
    attribute_filters: AttributeFiltersType = Field(
        sa_column=Column(JSON, nullable=False, server_default="{}")
    )

    @property
    def image_zarr_urls(self) -> list[str]:
        return [image["zarr_url"] for image in self.images]
