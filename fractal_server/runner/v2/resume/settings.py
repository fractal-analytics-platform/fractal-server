from typing import Literal

from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

from fractal_server.config._settings_config import SETTINGS_CONFIG_DICT


class ResumeSettings(BaseSettings):
    """
    Settings for the resume of orphaned jobs.

    Attributes:
        FRACTAL_RESUME_ON_STARTUP:
            If `"true"`, every server start resumes (or marks done/failed)
            the orphaned jobs, as `POST /admin/v2/resume/job/{id}/` does.
            Only jobs of runners that support resume are considered.
    """

    model_config = SettingsConfigDict(**SETTINGS_CONFIG_DICT)

    FRACTAL_RESUME_ON_STARTUP: Literal["true", "false"] = "false"


def get_resume_settings() -> ResumeSettings:
    return ResumeSettings()
