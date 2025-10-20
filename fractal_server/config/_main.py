import logging
from typing import Literal
from typing import TypeVar

from pydantic import SecretStr
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

from ._settings_config import SETTINGS_CONFIG_DICT
from fractal_server.types import AbsolutePathStr


class FractalConfigurationError(ValueError):
    pass


T = TypeVar("T")


class Settings(BaseSettings):
    """
    Contains all the configuration variables for Fractal Server

    The attributes of this class are set from the environment.
    """

    model_config = SettingsConfigDict(**SETTINGS_CONFIG_DICT)

    # JWT TOKEN
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

    # COOKIE TOKEN
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

    FRACTAL_API_MAX_JOB_LIST_LENGTH: int = 50
    """
    Number of ids that can be stored in the `jobsV2` attribute of
    `app.state`.
    """

    FRACTAL_GRACEFUL_SHUTDOWN_TIME: int = 30
    """
    Waiting time for the shutdown phase of executors
    """

    FRACTAL_VIEWER_AUTHORIZATION_SCHEME: Literal[
        "viewer-paths", "users-folders", "none"
    ] = "none"
    """
    Defines how the list of allowed viewer paths is built.

    This variable affects the `GET /auth/current-user/allowed-viewer-paths/`
    response, which is then consumed by
    [fractal-vizarr-viewer](https://github.com/fractal-analytics-platform/fractal-vizarr-viewer).

    Options:

    - "viewer-paths": The list of allowed viewer paths will include the user's
      `project_dir` along with any path defined in user groups' `viewer_paths`
      attributes.
    - "users-folders": The list will consist of the user's `project_dir` and a
       user-specific folder. The user folder is constructed by concatenating
       the base folder `FRACTAL_VIEWER_BASE_FOLDER` with the user's
       `slurm_user`.
    - "none": An empty list will be returned, indicating no access to
       viewer paths. Useful when vizarr viewer is not used.
    """

    FRACTAL_VIEWER_BASE_FOLDER: AbsolutePathStr | None = None
    """
    Base path to Zarr files that will be served by fractal-vizarr-viewer;
    This variable is required and used only when
    FRACTAL_VIEWER_AUTHORIZATION_SCHEME is set to "users-folders".
    """

    def check(self):
        """
        Make sure that required variables are set

        This method must be called before the server starts
        """
        # FRACTAL_VIEWER_BASE_FOLDER is required when
        # FRACTAL_VIEWER_AUTHORIZATION_SCHEME is set to "users-folders"
        # and it must be an absolute path
        if self.FRACTAL_VIEWER_AUTHORIZATION_SCHEME == "users-folders":
            viewer_base_folder = self.FRACTAL_VIEWER_BASE_FOLDER
            if viewer_base_folder is None:
                raise FractalConfigurationError(
                    "FRACTAL_VIEWER_BASE_FOLDER is required when "
                    "FRACTAL_VIEWER_AUTHORIZATION_SCHEME is set to "
                    "users-folders"
                )
