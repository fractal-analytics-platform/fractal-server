from fastapi import APIRouter
from fastapi import Depends
from pydantic import BaseModel

from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_superuser_act
from fractal_server.config import get_settings
from fractal_server.config import Settings
from fractal_server.syringe import Inject


class SettingsUpdate(BaseModel):
    FRACTAL_LOGGING_LEVEL: int = None


router = APIRouter()


@router.patch("/app/", response_model=Settings, status_code=200)
async def patch_settings_app(
    settings_update: SettingsUpdate,
    superuser: UserOAuth = Depends(current_superuser_act),
) -> Settings:
    """
    Patch `Settings`.
    """

    settings = Inject(get_settings)

    def _get_patched_settings():
        settings_args = settings.model_dump() | settings_update.model_dump(
            exclude_unset=True
        )
        return Settings(**settings_args)

    Inject.override(get_settings, _get_patched_settings)

    return _get_patched_settings()
