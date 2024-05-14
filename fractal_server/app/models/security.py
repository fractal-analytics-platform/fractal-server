# This is based on fastapi_users_db_sqlmodel
# <https://github.com/fastapi-users/fastapi-users-db-sqlmodel>
# Original Copyright
# Copyright 2022 François Voron
# License: MIT
#
# Modified by:
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
#
# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
from typing import Optional

from pydantic import EmailStr
from sqlalchemy import Column
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel


class OAuthAccount(SQLModel, table=True):
    """
    OAuth account model

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

    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user_oauth.id", nullable=False)
    user: Optional["UserOAuth"] = Relationship(back_populates="oauth_accounts")
    oauth_name: str = Field(index=True, nullable=False)
    access_token: str = Field(nullable=False)
    expires_at: Optional[int] = Field(nullable=True)
    refresh_token: Optional[str] = Field(nullable=True)
    account_id: str = Field(index=True, nullable=False)
    account_email: str = Field(nullable=False)

    class Config:
        orm_mode = True


class UserOAuth(SQLModel, table=True):
    """
    User model

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
        slurm_user:
        slurm_accounts:
        cache_dir:
        username:
        oauth_accounts:
        project_list:
    """

    __tablename__ = "user_oauth"

    id: Optional[int] = Field(default=None, primary_key=True)

    email: EmailStr = Field(
        sa_column_kwargs={"unique": True, "index": True}, nullable=False
    )
    hashed_password: str
    is_active: bool = Field(True, nullable=False)
    is_superuser: bool = Field(False, nullable=False)
    is_verified: bool = Field(False, nullable=False)

    slurm_user: Optional[str]
    slurm_accounts: list[str] = Field(
        sa_column=Column(JSON, server_default="[]", nullable=False)
    )
    cache_dir: Optional[str]
    username: Optional[str]

    oauth_accounts: list["OAuthAccount"] = Relationship(
        back_populates="user",
        sa_relationship_kwargs={"lazy": "joined", "cascade": "all, delete"},
    )

    class Config:
        orm_mode = True
