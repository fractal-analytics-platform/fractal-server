from datetime import datetime

from sqlalchemy import BOOLEAN
from sqlalchemy import ForeignKey
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.types import DateTime

from fractal_server.app.models.base import Base
from fractal_server.utils import get_timestamp

from .project import ProjectV2
from .workflowtask import WorkflowTaskV2


class WorkflowV2(Base):
    __tablename__ = "workflowv2"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    project_id: Mapped[int] = mapped_column(
        ForeignKey("projectv2.id", ondelete="CASCADE")
    )
    project: Mapped["ProjectV2"] = relationship(lazy="selectin")
    is_starred: Mapped[bool] = mapped_column(
        BOOLEAN,
        server_default="false",
        nullable=False,
    )

    task_list: Mapped[list["WorkflowTaskV2"]] = relationship(
        lazy="selectin",
        order_by="WorkflowTaskV2.order",
        collection_class=ordering_list("order"),
        cascade="all, delete-orphan",
    )
    template_id: Mapped[int | None] = mapped_column(
        ForeignKey("workflowtemplate.id", ondelete="SET NULL"),
        default=lambda: None,
    )

    timestamp_created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=get_timestamp
    )

    description: Mapped[str | None] = mapped_column(
        default=lambda: None, nullable=True
    )
