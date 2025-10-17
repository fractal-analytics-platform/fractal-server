from fastapi import HTTPException
from fastapi import status
from pydantic import ValidationError

from fractal_server.app.db import AsyncSession
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.app.user_settings import SlurmSshUserSettings
from fractal_server.app.user_settings import SlurmSudoUserSettings
from fractal_server.logger import set_logger


logger = set_logger(__name__)


def verify_user_has_settings(user: UserOAuth) -> None:
    """
    Check that the `user.user_settings_id` foreign-key is set.

    NOTE: This check will become useless when we make the foreign-key column
    required, but for the moment (as of v2.6.0) we have to keep it in place.

    Args:
        user: The user to be checked.
    """
    if user.user_settings_id is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Error: user '{user.email}' has no settings.",
        )


async def validate_user_settings(
    *, user: UserOAuth, backend: str, db: AsyncSession
) -> UserSettings:
    """
    Get a UserSettings object and validate it based on a given Fractal backend.

    Args:
        user: The user whose settings we should validate.
        backend: The value of `FRACTAL_RUNNER_BACKEND`
        db: An async DB session

    Returns:
        `UserSetting` object associated to `user`, if valid.
    """

    verify_user_has_settings(user)

    user_settings = await db.get(UserSettings, user.user_settings_id)

    match backend:
        case ResourceType.SLURM_SSH:
            UserSettingsValidationModel = SlurmSshUserSettings
        case ResourceType.SLURM_SUDO:
            UserSettingsValidationModel = SlurmSudoUserSettings
        case _:
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
