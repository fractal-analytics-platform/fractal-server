from enum import StrEnum
from typing import Self

from pydantic import model_validator
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

from ._settings_config import SETTINGS_CONFIG_DICT
from fractal_server.types import AbsolutePathStr


class DataAuthScheme(StrEnum):
    VIEWER_PATHS = "viewer-paths"
    USERS_FOLDERS = "users-folders"
    NONE = "none"


class DataSettings(BaseSettings):
    """
    Settings for the `fractal-data` integration.
    """

    model_config = SettingsConfigDict(**SETTINGS_CONFIG_DICT)

    FRACTAL_DATA_AUTH_SCHEME: DataAuthScheme = "none"
    """
    Defines how the list of allowed viewer paths is built.

    This variable affects the `GET /auth/current-user/allowed-viewer-paths/`
    response, which is then consumed by
    [fractal-data](https://github.com/fractal-analytics-platform/fractal-data).

    Options:

    - "viewer-paths": The list of allowed viewer paths will include the user's
      `project_dir` along with any path defined in user groups' `viewer_paths`
      attributes.
    - "users-folders": The list will consist of the user's `project_dir` and a
       user-specific folder. The user folder is constructed by concatenating
       the base folder `FRACTAL_DATA_BASE_FOLDER` with the user's profile
       `username`.
    - "none": An empty list will be returned, indicating no access to
       viewer paths. Useful when vizarr viewer is not used.
    """

    FRACTAL_DATA_BASE_FOLDER: AbsolutePathStr | None = None
    """
    Base path to Zarr files that will be served by fractal-vizarr-viewer;
    This variable is required and used only when
    FRACTAL_DATA_AUTHORIZATION_SCHEME is set to "users-folders".
    """

    @model_validator(mode="after")
    def check(self: Self) -> Self:
        """
        `FRACTAL_DATA_BASE_FOLDER` is required when
        `FRACTAL_DATA_AUTHORIZATION_SCHEME` is set to `"users-folders"`.
        """
        if (
            self.FRACTAL_DATA_AUTH_SCHEME == DataAuthScheme.USERS_FOLDERS
            and self.FRACTAL_DATA_BASE_FOLDER is None
        ):
            raise ValueError(
                "FRACTAL_DATA_BASE_FOLDER is required when "
                "FRACTAL_DATA_AUTH_SCHEME is set to "
                "users-folders"
            )
        return self
