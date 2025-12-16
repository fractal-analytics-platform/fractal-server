from datetime import datetime
from typing import Optional

from pydantic import ConfigDict
from pydantic import EmailStr
from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from fractal_server.utils import get_timestamp


class OAuthAccount(SQLModel, table=True):
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

    id: int | None = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user_oauth.id", nullable=False)
    user: Optional["UserOAuth"] = Relationship(back_populates="oauth_accounts")
    oauth_name: str = Field(index=True, nullable=False)
    access_token: str = Field(nullable=False)
    expires_at: int | None = Field(nullable=True, default=None)
    refresh_token: str | None = Field(nullable=True, default=None)
    account_id: str = Field(index=True, nullable=False)
    account_email: str = Field(nullable=False)
    model_config = ConfigDict(from_attributes=True)


class UserOAuth(SQLModel, table=True):
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

    model_config = ConfigDict(from_attributes=True)

    __tablename__ = "user_oauth"

    id: int | None = Field(default=None, primary_key=True)

    email: EmailStr = Field(
        sa_column_kwargs={"unique": True, "index": True},
        nullable=False,
    )
    hashed_password: str
    is_active: bool = Field(default=True, nullable=False)
    is_superuser: bool = Field(default=False, nullable=False)
    is_verified: bool = Field(default=False, nullable=False)

    oauth_accounts: list["OAuthAccount"] = Relationship(
        back_populates="user",
        sa_relationship_kwargs={"lazy": "joined", "cascade": "all, delete"},
    )

    profile_id: int | None = Field(
        foreign_key="profile.id",
        default=None,
        ondelete="RESTRICT",
    )

    project_dirs: list[str] = Field(
        sa_column=Column(ARRAY(String), nullable=False),
    )

    slurm_accounts: list[str] = Field(
        sa_column=Column(ARRAY(String), server_default="{}"),
    )


class UserGroup(SQLModel, table=True):
    """
    ORM model for the `usergroup` database table.

    Attributes:
        id: ID of the group
        name: Name of the group
        timestamp_created: Time of creation
    """

    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(unique=True)
    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
