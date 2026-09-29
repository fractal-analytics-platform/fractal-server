from datetime import datetime

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.types import DateTime

from fractal_server.app.models.base import Base
from fractal_server.utils import get_timestamp


class LinkUserGroup(Base):
    """
    Crossing table between User and UserGroup
    """

    __tablename__ = "linkusergroup"

    group_id: Mapped[int] = mapped_column(
        ForeignKey("usergroup.id", ondelete="CASCADE"), primary_key=True
    )
    user_id: Mapped[int] = mapped_column(
        ForeignKey("user_oauth.id", ondelete="CASCADE"), primary_key=True
    )

    timestamp_created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=get_timestamp
    )
