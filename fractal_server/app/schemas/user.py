from typing import Optional

from fastapi_users import schemas
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import ValidationInfo

from ._validators import NonEmptyString
from ._validators import val_unique_list

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

    username: Optional[str] = None
    group_ids_names: Optional[list[tuple[int, str]]] = None
    oauth_accounts: list[OAuthAccountRead]


class UserUpdate(schemas.BaseUserUpdate):
    """
    Schema for `User` update.

    Attributes:
        username:
    """

    model_config = ConfigDict(extra="forbid")

    username: Optional[NonEmptyString] = None

    # Validators

    @field_validator(
        "username",
        "is_active",
        "is_verified",
        "is_superuser",
        "email",
        "password",
    )
    @classmethod
    def cant_set_none(cls, v, info: ValidationInfo):
        if v is None:
            raise ValueError(f"Cannot set {info.field_name}=None")
        return v


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

    username: Optional[NonEmptyString] = None

    @field_validator("username")
    @classmethod
    def cant_set_none(cls, v, info: ValidationInfo):
        if v is None:
            raise ValueError(f"Cannot set {info.field_name}=None")
        return v


class UserUpdateGroups(BaseModel):
    """
    Schema for `POST /auth/users/{user_id}/set-groups/`

    """

    model_config = ConfigDict(extra="forbid")

    group_ids: list[int] = Field(min_length=1)

    _group_ids = field_validator("group_ids")(
        classmethod(val_unique_list("group_ids"))
    )
