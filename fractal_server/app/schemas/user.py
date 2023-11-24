from typing import Optional

from fastapi_users import schemas
from pydantic import BaseModel
from pydantic import Extra
from pydantic import root_validator
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

    # Validators
    _slurm_user = validator("slurm_user", allow_reuse=True)(
        valstr("slurm_user")
    )
    _username = validator("username", allow_reuse=True)(valstr("username"))
    _cache_dir = validator("cache_dir", allow_reuse=True)(
        val_absolute_path("cache_dir")
    )

    @root_validator(pre=True)
    def cant_set_none(cls, values):
        for attribute in [
            "is_active",
            "is_verified",
            "is_superuser",
            "email",
            "password",
        ]:
            if attribute in values:
                if values.get(attribute) is None:
                    raise ValueError(f"Cannot set {attribute}=None")
        return values


class UserUpdateStrict(BaseModel, extra=Extra.forbid):
    """
    Attributes that every user can self-edit
    """

    cache_dir: Optional[str]
    password: Optional[str]

    _cache_dir = validator("cache_dir", allow_reuse=True)(
        val_absolute_path("cache_dir")
    )
    _password = validator("password", allow_reuse=True)(valstr("password"))


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

    # Validators
    _slurm_user = validator("slurm_user", allow_reuse=True)(
        valstr("slurm_user")
    )
    _username = validator("username", allow_reuse=True)(valstr("username"))
    _cache_dir = validator("cache_dir", allow_reuse=True)(
        val_absolute_path("cache_dir")
    )
