from fastapi_users import schemas
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import EmailStr
from pydantic import Field

from fractal_server.types import ListUniqueNonNegativeInt
from fractal_server.types import NonEmptyStr

__all__ = (
    "UserRead",
    "UserUpdate",
    "UserUpdateGroups",
    "UserCreate",
)


class OAuthAccountRead(BaseModel):
    """
    Schema for storing essential `OAuthAccount` information within
    `UserRead.oauth_accounts`.

    Attributes:
        id: ID of the row in fractal-owned `oauthaccount` table.
        account_email: Email associated to OAuth account
        oauth_name: Name of the OAuth provider (e.g. `github`)
    """

    id: int
    account_email: str
    oauth_name: str


class UserRead(schemas.BaseUser[int]):
    """
    Schema for `User` read from database.

    Attributes:
        username:
    """

    username: str | None = None
    group_ids_names: list[tuple[int, str]] | None = None
    oauth_accounts: list[OAuthAccountRead]


class UserUpdate(schemas.BaseUserUpdate):
    """
    Schema for `User` update.

    Attributes:
        username:
    """

    model_config = ConfigDict(extra="forbid")
    username: NonEmptyStr = None
    password: NonEmptyStr = None
    email: EmailStr = None
    is_active: bool = None
    is_superuser: bool = None
    is_verified: bool = None


class UserUpdateStrict(BaseModel):
    """
    Schema for `User` self-editing.

    Attributes:
    """

    model_config = ConfigDict(extra="forbid")


class UserCreate(schemas.BaseUserCreate):
    """
    Schema for `User` creation.

    Attributes:
        username:
    """

    username: NonEmptyStr = None


class UserUpdateGroups(BaseModel):
    """
    Schema for `POST /auth/users/{user_id}/set-groups/`

    """

    model_config = ConfigDict(extra="forbid")

    group_ids: ListUniqueNonNegativeInt = Field(min_length=1)
