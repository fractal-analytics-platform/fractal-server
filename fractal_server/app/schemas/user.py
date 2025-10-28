from typing import Annotated

from fastapi_users import schemas
from pydantic import AfterValidator
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import EmailStr
from pydantic import Field

from fractal_server.string_tools import validate_cmd
from fractal_server.types import AbsolutePathStr
from fractal_server.types import ListUniqueNonEmptyString
from fractal_server.types import ListUniqueNonNegativeInt
from fractal_server.types import NonEmptyStr


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
        group_ids_names:
        oauth_accounts:
        profile_id:
    """

    group_ids_names: list[tuple[int, str]] | None = None
    oauth_accounts: list[OAuthAccountRead]
    profile_id: int | None = None
    project_dir: str
    slurm_accounts: list[str]


def _validate_cmd(value: str) -> str:
    validate_cmd(value)
    return value


class UserUpdate(schemas.BaseUserUpdate):
    """
    Schema for `User` update.

    Attributes:
        password:
        email:
        is_active:
        is_superuser:
        is_verified:
        profile_id:
        project_dir:
        slurm_accounts:
    """

    model_config = ConfigDict(extra="forbid")
    password: NonEmptyStr = None
    email: EmailStr = None
    is_active: bool = None
    is_superuser: bool = None
    is_verified: bool = None
    profile_id: int | None = None
    project_dir: Annotated[
        AbsolutePathStr, AfterValidator(_validate_cmd)
    ] = None
    slurm_accounts: ListUniqueNonEmptyString = None


class UserUpdateStrict(BaseModel):
    """
    Schema for `User` self-editing.

    Attributes:
        slurm_accounts:
    """

    model_config = ConfigDict(extra="forbid")
    slurm_accounts: ListUniqueNonEmptyString = None


class UserCreate(schemas.BaseUserCreate):
    """
    Schema for `User` creation.

    Attributes:
        profile_id:
    """

    profile_id: int | None = None
    project_dir: Annotated[AbsolutePathStr, AfterValidator(_validate_cmd)]
    slurm_accounts: list[str] = Field(default_factory=list)


class UserUpdateGroups(BaseModel):
    """
    Schema for `POST /auth/users/{user_id}/set-groups/`

    """

    model_config = ConfigDict(extra="forbid")

    group_ids: ListUniqueNonNegativeInt = Field(min_length=1)


class UserProfileInfo(BaseModel):
    has_profile: bool
    resource_name: str | None = None
    profile_name: str | None = None
    username: str | None = None
