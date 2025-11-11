from datetime import datetime
from typing import Optional

from pydantic import ConfigDict
from pydantic import EmailStr
from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import JSONB
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

    This class is a modification of SQLModelBaseUserDB from from
    fastapi_users_db_sqlmodel. Original Copyright: 2022 François Voron,
    released under MIT licence.

    Attributes:
        id:
        email:
        hashed_password:
        is_active:
        is_superuser:
        is_verified:
        oauth_accounts:
        profile_id:
        project_dir:
        slurm_accounts:
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

    project_dir: str
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
    viewer_paths: list[str] = Field(
        sa_column=Column(JSONB, server_default="[]", nullable=False)
    )
