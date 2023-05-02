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
import uuid
from typing import Optional
from typing import TYPE_CHECKING

from pydantic import EmailStr
from pydantic import UUID4
from sqlalchemy_utils import UUIDType
from sqlmodel import Column
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel


class SQLModelBaseUserDB(SQLModel):
    """
    This class is from fastapi_users_db_sqlmodel
    Original Copyright: 2022 François Voron, released under MIT licence
    """

    __tablename__ = "user"

    id: UUID4 = Field(
        default_factory=uuid.uuid4, primary_key=True, nullable=False
    )  # noqa
    if TYPE_CHECKING:  # pragma: no cover
        email: str
    else:
        email: EmailStr = Field(
            sa_column_kwargs={"unique": True, "index": True}, nullable=False
        )
    hashed_password: str

    is_active: bool = Field(True, nullable=False)
    is_superuser: bool = Field(False, nullable=False)
    is_verified: bool = Field(False, nullable=False)

    class Config:
        orm_mode = True


class SQLModelBaseOAuthAccount(SQLModel):
    """
    This class is from fastapi_users_db_sqlmodel
    Original Copyright: 2022 François Voron, released under MIT licence
    """

    __tablename__ = "oauthaccount"

    id: UUID4 = Field(default_factory=uuid.uuid4, primary_key=True)
    user_id: UUID4 = Field(foreign_key="user.id", nullable=False)
    oauth_name: str = Field(index=True, nullable=False)
    access_token: str = Field(nullable=False)
    expires_at: Optional[int] = Field(nullable=True)
    refresh_token: Optional[str] = Field(nullable=True)
    account_id: str = Field(index=True, nullable=False)
    account_email: str = Field(nullable=False)

    class Config:
        orm_mode = True


class UserOAuth(SQLModelBaseUserDB, table=True):
    __tablename__ = "user_oauth"
    id: UUID4 = Field(
        default_factory=uuid.uuid4,
        nullable=False,
        sa_column=Column(UUIDType(), primary_key=True),
    )
    slurm_user: Optional[str]
    cache_dir: Optional[str]
    oauth_accounts: list["OAuthAccount"] = Relationship(
        back_populates="user",
        sa_relationship_kwargs={"lazy": "selectin", "cascade": "all, delete"},
    )


class OAuthAccount(SQLModelBaseOAuthAccount, table=True):
    user_id: UUID4 = Field(foreign_key="user_oauth.id", nullable=False)
    user: Optional[UserOAuth] = Relationship(back_populates="oauth_accounts")
