from fastapi import HTTPException
from fastapi import status
from pydantic import ValidationError

from fractal_server.app.db import AsyncSession
from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.models import UserOAuth
from fractal_server.logger import set_logger

logger = set_logger(__name__)


async def user_has_profile_or_422(*, user: UserOAuth) -> None:
    if user.profile_id is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"User {user.email} is not associated to a computational "
                "profile. Please contact an admin."
            ),
        )


async def validate_user_profile(
    *,
    user: UserOAuth,
    db: AsyncSession,
) -> tuple[Resource, Profile]:
    """
    Validate profile and resource associated to a given user.

    Note: this only returns non-db-bound objects.
    """
    await user_has_profile_or_422(user=user)
    profile = await db.get(Profile, user.profile_id)
    resource = await db.get(Resource, profile.resource_id)
    try:
        # FIXME: these are mocks!
        # FIXME: Move them somewhere else, and actually implement them.
        from typing import Literal, Self
        from pydantic import BaseModel, model_validator
        from fractal_server.types import NonEmptyStr

        class ResourceValidationModel(BaseModel):
            resource_type: Literal["slurm_sudo", "slurm_ssh", "local"]

        class ProfileValidationModel(BaseModel):
            resource_type: Literal["slurm_sudo", "slurm_ssh", "local"]
            username: NonEmptyStr | None = None

            @model_validator(mode="after")
            def validate_username(self) -> Self:
                if self.resource_type != "local" and self.username is None:
                    raise ValueError("username is required")
                return self

        # FIXME - end mocks

        ResourceValidationModel(**resource.model_dump())
        ProfileValidationModel(
            **profile.model_dump(),
            resource_type=resource.resource_type,
        )

        db.expunge(resource)
        db.expunge(profile)

        return resource, profile

    except ValidationError as e:
        error_msg = (
            "User resource/profile are not valid for "
            f"resource type '{resource.resource_type}'. "
            f"Original error: {str(e)}"
        )
        logger.warning(error_msg)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=error_msg,
        )
