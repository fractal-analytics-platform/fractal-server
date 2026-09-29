from sqlalchemy import CheckConstraint
from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy import column
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

from fractal_server.app.models.base import Base


class LinkUserProjectV2(Base):
    """
    Crossing table between User and ProjectV2
    """

    __tablename__ = "linkuserprojectv2"

    project_id: Mapped[int] = mapped_column(
        ForeignKey("projectv2.id", ondelete="CASCADE"), primary_key=True
    )
    user_id: Mapped[int] = mapped_column(
        ForeignKey("user_oauth.id"), primary_key=True
    )

    is_owner: Mapped[bool]
    is_verified: Mapped[bool]
    permissions: Mapped[str]

    __table_args__ = (
        Index(
            "ix_linkuserprojectv2_one_owner_per_project",
            "project_id",
            unique=True,
            postgresql_where=column("is_owner").is_(True),
        ),
        CheckConstraint(
            "NOT (is_owner AND NOT is_verified)",
            name="owner_is_verified",
        ),
        CheckConstraint(
            "NOT (is_owner AND permissions <> 'rwx')",
            name="owner_full_permissions",
        ),
        CheckConstraint(
            "permissions IN ('r', 'rw', 'rwx')",
            name="valid_permissions",
        ),
    )
