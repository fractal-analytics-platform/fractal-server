from fastapi import HTTPException
from fastapi import status

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
) -> None:
    """
    Validate profile and resource associated to a given user.
    """
    await user_has_profile_or_422(user=user)
    profile = await db.get(Profile, user.profile_id)
    resource = await db.get(Resource, profile.resource_id)
    print(profile, resource)  # FIXME
    if False:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=("invalid profile or resource - FIXME"),
        )
    # validate profile&resource with some pydantic schema, which depends on
    # the resource.type


"""
EXAMPLE


    if backend == "slurm_ssh":
        UserSettingsValidationModel = SlurmSshUserSettings
    elif backend == "slurm":
        UserSettingsValidationModel = SlurmSudoUserSettings
    else:
        # For other backends, we don't validate anything
        return user_settings

    try:
        UserSettingsValidationModel(**user_settings.model_dump())
    except ValidationError as e:
        error_msg = (
            "User settings are not valid for "
            f"FRACTAL_RUNNER_BACKEND='{backend}'. "
            f"Original error: {str(e)}"
        )
        logger.warning(error_msg)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=error_msg,
        )

    return user_settings
"""
