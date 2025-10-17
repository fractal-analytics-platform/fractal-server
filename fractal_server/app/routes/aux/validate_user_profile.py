from fastapi import HTTPException
from fastapi import status
from pydantic import validate_call
from pydantic import ValidationError

from fractal_server.app.db import AsyncSession
from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.models import UserOAuth
from fractal_server.app.schemas.v2.profile import ProfileCreate
from fractal_server.app.schemas.v2.resource import ResourceCreate
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


@validate_call
def _validate_resource_data(_data: ResourceCreate):
    """
    We use `@validate_call` because `ResourceCreate` is a `Union` type and it
    cannot be instantiated directly.
    """


@validate_call
def _validate_profile_data(_data: ProfileCreate):
    """
    We use `@validate_call` because `ProfileCreate` is a `Union` type and it
    cannot be instantiated directly.
    """


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
        _validate_resource_data(
            resource.model_dump(exclude={"id", "timestamp_created"}),
        )
        _validate_profile_data(
            profile.model_dump(exclude={"resource_id", "id"}),
        )
        db.expunge(resource)
        db.expunge(profile)

        return resource, profile

    except ValidationError as e:
        error_msg = (
            "User resource/profile are not valid for "
            f"resource type '{resource.type}'. "
            f"Original error: {str(e)}"
        )
        logger.warning(error_msg)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=error_msg,
        )
