from datetime import datetime

from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.types import DateTime

from fractal_server.app.models.base import Base
from fractal_server.utils import get_timestamp


class AccountingRecord(Base):
    """
    AccountingRecord table.
    """

    __tablename__ = "accountingrecord"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("user_oauth.id"), nullable=False
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=get_timestamp
    )
    num_tasks: Mapped[int]
    num_new_images: Mapped[int]


class AccountingRecordSlurm(Base):
    """
    AccountingRecordSlurm table.
    """

    __tablename__ = "accountingrecordslurm"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("user_oauth.id"), nullable=False
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=get_timestamp
    )
    slurm_job_ids: Mapped[list[int]] = mapped_column(
        ARRAY(Integer), nullable=True, default=list
    )
    fractal_job_id: Mapped[int | None] = mapped_column(
        ForeignKey("jobv2.id"), nullable=True, default=lambda: None
    )
    resource_id: Mapped[int | None] = mapped_column(
        ForeignKey("resource.id", ondelete="SET NULL"),
        nullable=True,
        default=lambda: None,
    )
