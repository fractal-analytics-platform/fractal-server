from typing import Optional

from fastapi_users import schemas
from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic import validator

from ._validators import val_unique_list
from ._validators import valstr

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

    username: Optional[str]
    group_ids_names: Optional[list[tuple[int, str]]] = None
    oauth_accounts: list[OAuthAccountRead]


class UserUpdate(schemas.BaseUserUpdate, extra=Extra.forbid):
    """
    Schema for `User` update.

    Attributes:
        username:
    """

    username: Optional[str]

    # Validators
    _username = validator("username", allow_reuse=True)(valstr("username"))

    @validator(
        "is_active",
        "is_verified",
        "is_superuser",
        "email",
        "password",
        always=False,
    )
    def cant_set_none(cls, v, field):
        if v is None:
            raise ValueError(f"Cannot set {field.name}=None")
        return v


class UserUpdateStrict(BaseModel, extra=Extra.forbid):
    """
    Schema for `User` self-editing.

    Attributes:
    """

    pass


class UserCreate(schemas.BaseUserCreate):
    """
    Schema for `User` creation.

    Attributes:
        username:
    """

    username: Optional[str]

    # Validators

    _username = validator("username", allow_reuse=True)(valstr("username"))


class UserUpdateGroups(BaseModel, extra=Extra.forbid):
    """
    Schema for `POST /auth/users/{user_id}/set-groups/`

    """

    group_ids: list[int] = Field(min_items=1)

    _group_ids = validator("group_ids", allow_reuse=True)(
        val_unique_list("group_ids")
    )
