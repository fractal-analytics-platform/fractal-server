import logging
from typing import Literal

from pydantic import HttpUrl
from pydantic import SecretStr
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

from ._settings_config import SETTINGS_CONFIG_DICT


class Settings(BaseSettings):
    """
    Contains the general configuration variables for Fractal Server.

    Attributes:
        JWT_EXPIRE_SECONDS:
            JWT token lifetime, in seconds.
        JWT_SECRET_KEY:
            JWT secret.<br>
            ⚠️ Set this variable to a secure string, and do not disclose it.
        COOKIE_EXPIRE_SECONDS:
            Cookie token lifetime, in seconds.
        FRACTAL_RUNNER_BACKEND:
            Select which runner backend to use.
        FRACTAL_LOGGING_LEVEL:
            Logging-level threshold for logging
            Only logs of with this level (or higher) will appear in the console
            logs.
        FRACTAL_API_MAX_JOB_LIST_LENGTH:
            Number of ids that can be stored in the `jobs` attribute of
            `app.state`.
        FRACTAL_GRACEFUL_SHUTDOWN_TIME:
            Waiting time for the shutdown phase of executors, in seconds.
        FRACTAL_HELP_URL:
            The URL of an instance-specific Fractal help page.
        FRACTAL_DEFAULT_GROUP_NAME:
            Name of the default user group.

            If set to `"All"`, then the user group with that name is a special
            user group (e.g. it cannot be deleted, and new users are
            automatically added to it). If set to `None` (the default value),
            then user groups are all equivalent, independently on their name.
        FRACTAL_LONG_REQUEST_TIME:
            Time limit beyond which the execution of an API request is
            considered *slow* and an appropriate warning is logged by the
            middleware.
    """

    model_config = SettingsConfigDict(**SETTINGS_CONFIG_DICT)

    JWT_EXPIRE_SECONDS: int = 180
    JWT_SECRET_KEY: SecretStr
    COOKIE_EXPIRE_SECONDS: int = 86400
    # Note: we do not use ResourceType here to avoid circular imports
    FRACTAL_RUNNER_BACKEND: Literal["local", "slurm_ssh", "slurm_sudo"] = (
        "local"
    )
    FRACTAL_LOGGING_LEVEL: int = logging.INFO
    FRACTAL_API_MAX_JOB_LIST_LENGTH: int = 25
    FRACTAL_GRACEFUL_SHUTDOWN_TIME: float = 30.0
    FRACTAL_HELP_URL: HttpUrl | None = None
    FRACTAL_DEFAULT_GROUP_NAME: Literal["All"] | None = None
    FRACTAL_LONG_REQUEST_TIME: float = 30.0
