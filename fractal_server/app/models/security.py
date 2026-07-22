from datetime import datetime

from sqlalchemy import ARRAY
from sqlalchemy import BOOLEAN
from sqlalchemy import CheckConstraint
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.types import DateTime

from fractal_server.app.models.base import Base
from fractal_server.utils import get_timestamp


class OAuthAccount(Base):
    """
    ORM model for OAuth accounts (`oauthaccount` database table).

    This class is based on fastapi_users_db_sqlmodel::SQLModelBaseOAuthAccount.
    Original Copyright: 2021 François Voron, released under MIT licence.

    Attributes:
        id:
        user_id:
        user:
        oauth_name:
        access_token:
        expires_at:
        refresh_token:
        account_id:
        account_email:
    """

    __tablename__ = "oauthaccount"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("user_oauth.id"), nullable=False
    )
    user: Mapped["UserOAuth | None"] = relationship(
        back_populates="oauth_accounts"
    )
    oauth_name: Mapped[str] = mapped_column(index=True, nullable=False)
    access_token: Mapped[str] = mapped_column(nullable=False)
    expires_at: Mapped[int | None] = mapped_column(
        nullable=True, default=lambda: None
    )
    refresh_token: Mapped[str | None] = mapped_column(
        nullable=True, default=lambda: None
    )
    account_id: Mapped[str] = mapped_column(index=True, nullable=False)
    account_email: Mapped[str] = mapped_column(nullable=False)


class UserOAuth(Base):
    """
    ORM model for the `user_oauth` database table.

    This class is a modification of
    [`SQLModelBaseUserDB`](https://github.com/fastapi-users/fastapi-users-db-sqlmodel/blob/83980d7f20886120f4636a102ab1822b4c366f63/fastapi_users_db_sqlmodel/__init__.py#L15-L32)
    from `fastapi_users_db_sqlmodel`.
    Original Copyright: 2022 François Voron, released under MIT licence.

    Note that several class attributes are
    [the default ones from `fastapi-users`
    ](https://fastapi-users.github.io/fastapi-users/latest/configuration/schemas/).

    Attributes:
        id:
        email:
        hashed_password:
        is_active:
            If this is `False`, the user has no access to the `/api/v2/`
            endpoints.
        is_superuser:
        is_verified:
            If this is `False`, the user has no access to the `/api/v2/`
            endpoints.
        oauth_accounts:
        profile_id:
            Foreign key linking the user to a `Profile`. If this is unset,
            the user has no access to the `/api/v2/` endpoints.
        project_dirs:
            Absolute paths of the user's project directory. This is used (A) as
            a default base folder for the `zarr_dir` of new datasets (where
            the output Zarr are located), and (B) as a folder which is included
            by default in the paths that a user is allowed to stream (if the
            `fractal-data` integration is set up).
            two goals:
        slurm_accounts:
            List of SLURM accounts that the user can select upon running a job.
    """

    __tablename__ = "user_oauth"

    id: Mapped[int] = mapped_column(primary_key=True)

    email: Mapped[str] = mapped_column(unique=True, index=True, nullable=False)
    hashed_password: Mapped[str]
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)
    is_superuser: Mapped[bool] = mapped_column(default=False, nullable=False)
    is_verified: Mapped[bool] = mapped_column(default=False, nullable=False)
    is_guest: Mapped[bool] = mapped_column(
        BOOLEAN,
        server_default="false",
        nullable=False,
    )

    oauth_accounts: Mapped[list["OAuthAccount"]] = relationship(
        back_populates="user",
        lazy="joined",
        cascade="all, delete",
    )

    profile_id: Mapped[int | None] = mapped_column(
        ForeignKey("profile.id", ondelete="RESTRICT"), default=lambda: None
    )

    project_dirs: Mapped[list[str]] = mapped_column(
        ARRAY(String), nullable=False
    )

    slurm_accounts: Mapped[list[str]] = mapped_column(
        ARRAY(String), nullable=True, server_default="{}"
    )

    __table_args__ = (
        CheckConstraint(
            "NOT (is_superuser AND is_guest)",
            name="superuser_is_not_guest",
        ),
    )


class UserGroup(Base):
    """
    ORM model for the `usergroup` database table.

    Attributes:
        id: ID of the group
        name: Name of the group
        timestamp_created: Time of creation
    """

    __tablename__ = "usergroup"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(unique=True)
    timestamp_created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=get_timestamp
    )
