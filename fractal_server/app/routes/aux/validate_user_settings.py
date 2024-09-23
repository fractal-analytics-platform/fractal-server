from fastapi import HTTPException
from fastapi import status
from pydantic import BaseModel
from pydantic import ValidationError

from fractal_server.app.db import AsyncSession
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings
from fractal_server.logger import set_logger
from fractal_server.user_settings import SlurmSshUserSettings
from fractal_server.user_settings import SlurmSudoUserSettings

logger = set_logger(__name__)


async def validate_user_settings(
    *, user: UserOAuth, backend: str, db: AsyncSession
) -> UserSettings:
    """
    FIXME docstring
    """
    # First: check that the foreign-key exists. TODO: remove this check,
    # after this column is made required
    if user.user_settings_id is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Error: user '{user.email}' has no settings.",
        )

    user_settings = await db.get(UserSettings, user.user_settings_id)

    if backend == "slurm_ssh":
        UserSettingsModel = SlurmSshUserSettings
    elif backend == "slurm":
        UserSettingsModel = SlurmSudoUserSettings
    else:
        UserSettingsModel = BaseModel

    try:
        UserSettingsModel(**user_settings.model_dump())
    except ValidationError as e:
        error_msg = (
            "User settings are not valid for "
            f"FRACTAL_RUNNER_BACKEND='{backend}'. "
            f"Original error: {str(e)}"
        )
        logger.warning(error_msg)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=error_msg,
        )

    return user_settings
