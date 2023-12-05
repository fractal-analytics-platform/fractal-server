from typing import Optional

from fastapi_users import schemas
from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic import validator

from ._validators import val_absolute_path
from ._validators import valstr


__all__ = (
    "UserRead",
    "UserUpdate",
    "UserCreate",
)


class UserRead(schemas.BaseUser[int]):
    """
    Task for `User` read from database.

    Attributes:
        slurm_user:
        cache_dir:
        username:
    """

    slurm_user: Optional[str]
    cache_dir: Optional[str]
    username: Optional[str]
    slurm_accounts: list[str]


class UserUpdate(schemas.BaseUserUpdate):
    """
    Task for `User` update.

    Attributes:
        slurm_user:
        cache_dir:
        username:
    """

    slurm_user: Optional[str]
    cache_dir: Optional[str]
    username: Optional[str]
    slurm_accounts: Optional[list[str]]

    # Validators
    _slurm_user = validator("slurm_user", allow_reuse=True)(
        valstr("slurm_user")
    )
    _username = validator("username", allow_reuse=True)(valstr("username"))
    _cache_dir = validator("cache_dir", allow_reuse=True)(
        val_absolute_path("cache_dir")
    )

    @validator("slurm_accounts")
    def is_unique(cls, v: list[str]) -> list[str]:
        if len(set(v)) != len(v):
            raise ValueError("`slurm_accounts` list has repetitions")
        return v

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
    Attributes that every user can self-edit
    """

    cache_dir: Optional[str]
    slurm_accounts: Optional[list[str]]

    @validator("slurm_accounts")
    def is_unique(cls, v: list[str]) -> list[str]:
        if len(set(v)) != len(v):
            raise ValueError("`slurm_accounts` list has repetitions")
        return v

    _cache_dir = validator("cache_dir", allow_reuse=True)(
        val_absolute_path("cache_dir")
    )


class UserCreate(schemas.BaseUserCreate):
    """
    Task for `User` creation.

    Attributes:
        slurm_user:
        cache_dir:
        username:
    """

    slurm_user: Optional[str]
    cache_dir: Optional[str]
    username: Optional[str]
    slurm_accounts: list[str] = Field(default_factory=list)

    # Validators
    @validator("slurm_accounts")
    def is_unique(cls, v: list[str]) -> list[str]:
        if len(set(v)) != len(v):
            raise ValueError("`slurm_accounts` list has repetitions")
        return v

    _slurm_user = validator("slurm_user", allow_reuse=True)(
        valstr("slurm_user")
    )
    _username = validator("username", allow_reuse=True)(valstr("username"))
    _cache_dir = validator("cache_dir", allow_reuse=True)(
        val_absolute_path("cache_dir")
    )
