from typing import Optional

from fastapi_users import schemas
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
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

    slurm_user: Optional[str] = None
    cache_dir: Optional[str] = None
    username: Optional[str] = None
    slurm_accounts: Optional[list[StrictStr]] = None

    # Validators
    _slurm_user = field_validator("slurm_user")(valstr("slurm_user"))
    _username = field_validator("username")(valstr("username"))
    _cache_dir = field_validator("cache_dir")(val_absolute_path("cache_dir"))

    _slurm_accounts = field_validator("slurm_accounts")(
        val_unique_list("slurm_accounts")
    )


class UserUpdateStrict(BaseModel):
    """
    Schema for `User` self-editing.

    Attributes:
        cache_dir:
        slurm_accounts:
    """

    model_config = ConfigDict(extra="forbid")

    cache_dir: Optional[str] = None
    slurm_accounts: Optional[list[StrictStr]] = None

    _slurm_accounts = field_validator("slurm_accounts")(
        val_unique_list("slurm_accounts")
    )

    _cache_dir = field_validator("cache_dir")(val_absolute_path("cache_dir"))


class UserCreate(schemas.BaseUserCreate):
    """
    Schema for `User` creation.

    Attributes:
        slurm_user:
        cache_dir:
        username:
        slurm_accounts:
    """

    slurm_user: Optional[str] = None
    cache_dir: Optional[str] = None
    username: Optional[str] = None
    slurm_accounts: list[StrictStr] = Field(default_factory=list)

    # Validators

    @field_validator("slurm_accounts")
    @classmethod
    def slurm_accounts_validator(cls, value):
        for i, element in enumerate(value):
            value[i] = valstr(attribute=f"slurm_accounts[{i}]")(element)
        val_unique_list("slurm_accounts")(value)
        return value

    _slurm_user = field_validator("slurm_user")(valstr("slurm_user"))
    _username = field_validator("username")(valstr("username"))
    _cache_dir = field_validator("cache_dir")(val_absolute_path("cache_dir"))
