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


__all__ = (
    "UserRead",
    "UserUpdate",
    "UserCreate",
)


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
    _cache_dir = validator("cache_dir", allow_reuse=True)(
        val_absolute_path("cache_dir")
    )

    _slurm_accounts = validator("slurm_accounts", allow_reuse=True)(
        val_unique_list("slurm_accounts")
    )

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

    _cache_dir = validator("cache_dir", allow_reuse=True)(
        val_absolute_path("cache_dir")
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
    _cache_dir = validator("cache_dir", allow_reuse=True)(
        val_absolute_path("cache_dir")
    )
