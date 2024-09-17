from typing import Optional

from fastapi_users import schemas
from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic import validator
from pydantic.types import StrictStr

from ._validators import val_absolute_path
from ._validators import val_unique_list
from ._validators import valstr
from fractal_server.string_tools import validate_cmd

__all__ = (
    "UserRead",
    "UserUpdate",
    "UserCreate",
    "UserUpdateWithNewGroupIds",
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
        slurm_user:
        cache_dir:
        username:
        slurm_accounts:
    """

    slurm_user: Optional[str]
    cache_dir: Optional[str]
    username: Optional[str]
    slurm_accounts: list[str]
    group_names: Optional[list[str]] = None
    group_ids: Optional[list[int]] = None
    oauth_accounts: list[OAuthAccountRead]


class UserUpdate(schemas.BaseUserUpdate):
    """
    Schema for `User` update.

    Attributes:
        slurm_user:
        cache_dir:
        username:
        slurm_accounts:
    """

    slurm_user: Optional[str]
    cache_dir: Optional[str]
    username: Optional[str]
    slurm_accounts: Optional[list[StrictStr]]

    # Validators
    _slurm_user = validator("slurm_user", allow_reuse=True)(
        valstr("slurm_user")
    )
    _username = validator("username", allow_reuse=True)(valstr("username"))

    _slurm_accounts = validator("slurm_accounts", allow_reuse=True)(
        val_unique_list("slurm_accounts")
    )

    @validator("cache_dir")
    def cache_dir_validator(cls, value):
        validate_cmd(value)
        return val_absolute_path("cache_dir")(value)

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
        cache_dir:
        slurm_accounts:
    """

    cache_dir: Optional[str]
    slurm_accounts: Optional[list[StrictStr]]

    _slurm_accounts = validator("slurm_accounts", allow_reuse=True)(
        val_unique_list("slurm_accounts")
    )

    @validator("cache_dir")
    def cache_dir_validator(cls, value):
        validate_cmd(value)
        return val_absolute_path("cache_dir")(value)


class UserUpdateWithNewGroupIds(UserUpdate):
    new_group_ids: Optional[list[int]] = None

    _val_unique = validator("new_group_ids", allow_reuse=True)(
        val_unique_list("new_group_ids")
    )


class UserCreate(schemas.BaseUserCreate):
    """
    Schema for `User` creation.

    Attributes:
        slurm_user:
        cache_dir:
        username:
        slurm_accounts:
    """

    slurm_user: Optional[str]
    cache_dir: Optional[str]
    username: Optional[str]
    slurm_accounts: list[StrictStr] = Field(default_factory=list)

    # Validators

    @validator("slurm_accounts")
    def slurm_accounts_validator(cls, value):
        for i, element in enumerate(value):
            value[i] = valstr(attribute=f"slurm_accounts[{i}]")(element)
        val_unique_list("slurm_accounts")(value)
        return value

    _slurm_user = validator("slurm_user", allow_reuse=True)(
        valstr("slurm_user")
    )
    _username = validator("username", allow_reuse=True)(valstr("username"))

    @validator("cache_dir")
    def cache_dir_validator(cls, value):
        validate_cmd(value)
        return val_absolute_path("cache_dir")(value)
