import logging
from typing import Literal

from pydantic import HttpUrl
from pydantic import SecretStr
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

from ._settings_config import SETTINGS_CONFIG_DICT


class Settings(BaseSettings):
    """
    Contains all the configuration variables for Fractal Server

    The attributes of this class are set from the environment.
    """

    model_config = SettingsConfigDict(**SETTINGS_CONFIG_DICT)

    JWT_EXPIRE_SECONDS: int = 180
    """
    JWT token lifetime, in seconds.
    """

    JWT_SECRET_KEY: SecretStr
    """
    JWT secret

    ⚠️ **IMPORTANT**: set this variable to a secure string, and do not disclose
    it.
    """

    COOKIE_EXPIRE_SECONDS: int = 86400
    """
    Cookie token lifetime, in seconds.
    """

    # Note: we do not use ResourceType here to avoid circular imports
    FRACTAL_RUNNER_BACKEND: Literal[
        "local", "slurm_ssh", "slurm_sudo"
    ] = "local"
    """
    Select which runner backend to use.
    """

    FRACTAL_LOGGING_LEVEL: int = logging.INFO
    """
    Logging-level threshold for logging

    Only logs of with this level (or higher) will appear in the console logs.
    """

    FRACTAL_API_MAX_JOB_LIST_LENGTH: int = 25
    """
    Number of ids that can be stored in the `jobsV2` attribute of
    `app.state`.
    """

    FRACTAL_GRACEFUL_SHUTDOWN_TIME: float = 30.0
    """
    Waiting time for the shutdown phase of executors
    """

    FRACTAL_HELP_URL: HttpUrl | None = None
    """
    The URL of an instance-specific Fractal help page.
    """

    FRACTAL_DEFAULT_GROUP_NAME: Literal["All"] | None = None
    """
    Name of the default user group.

    If set to `"All"`, then the user group with that name is a special user
    group (e.g. it cannot be deleted, and new users are automatically added
    to it). If set to `None` (the default value), then user groups are all
    equivalent, independently on their name.
    """
