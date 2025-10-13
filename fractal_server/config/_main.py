import logging
from os import environ
from os import getenv
from typing import Literal
from typing import TypeVar

from pydantic import BaseModel
from pydantic import Field
from pydantic import model_validator
from pydantic import SecretStr
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

from ._settings_config import SETTINGS_CONFIG_DICT
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.types import AbsolutePathStr


class FractalConfigurationError(ValueError):
    pass


T = TypeVar("T")


class OAuthClientConfig(BaseModel):
    """
    OAuth Client Config Model

    This model wraps the variables that define a client against an Identity
    Provider. As some providers are supported by the libraries used within the
    server, some attributes are optional.

    Attributes:
        CLIENT_NAME:
            The name of the client
        CLIENT_ID:
            ID of client
        CLIENT_SECRET:
            Secret to authorise against the identity provider
        OIDC_CONFIGURATION_ENDPOINT:
            OpenID configuration endpoint,
            allowing to discover the required endpoints automatically
        REDIRECT_URL:
            String to be used as `redirect_url` argument for
            `fastapi_users.get_oauth_router`, and then in
            `httpx_oauth.integrations.fastapi.OAuth2AuthorizeCallback`.
    """

    CLIENT_NAME: str
    CLIENT_ID: str
    CLIENT_SECRET: SecretStr
    OIDC_CONFIGURATION_ENDPOINT: str | None = None
    REDIRECT_URL: str | None = None

    @model_validator(mode="before")
    @classmethod
    def check_configuration(cls, values):
        if values.get("CLIENT_NAME") not in ["GOOGLE", "GITHUB"]:
            if not values.get("OIDC_CONFIGURATION_ENDPOINT"):
                raise FractalConfigurationError(
                    f"Missing OAUTH_{values.get('CLIENT_NAME')}"
                    "_OIDC_CONFIGURATION_ENDPOINT"
                )
        return values


class Settings(BaseSettings):
    """
    Contains all the configuration variables for Fractal Server

    The attributes of this class are set from the environment.
    """

    model_config = SettingsConfigDict(**SETTINGS_CONFIG_DICT)

    OAUTH_CLIENTS_CONFIG: list[OAuthClientConfig] = Field(default_factory=list)

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

    @model_validator(mode="before")
    @classmethod
    def collect_oauth_clients(cls, values):
        """
        Automatic collection of OAuth Clients

        This method collects the environment variables relative to a single
        OAuth client and saves them within the `Settings` object in the form
        of an `OAuthClientConfig` instance.

        Fractal can support an arbitrary number of OAuth providers, which are
        automatically detected by parsing the environment variable names. In
        particular, to set the provider `FOO`, one must specify the variables

            OAUTH_FOO_CLIENT_ID
            OAUTH_FOO_CLIENT_SECRET
            ...

        etc (cf. OAuthClientConfig).
        """
        oauth_env_variable_keys = [
            key for key in environ.keys() if key.startswith("OAUTH_")
        ]
        clients_available = {
            var.split("_")[1] for var in oauth_env_variable_keys
        }

        values["OAUTH_CLIENTS_CONFIG"] = []
        for client in clients_available:
            prefix = f"OAUTH_{client}"
            oauth_client_config = OAuthClientConfig(
                CLIENT_NAME=client,
                CLIENT_ID=getenv(f"{prefix}_CLIENT_ID", None),
                CLIENT_SECRET=getenv(f"{prefix}_CLIENT_SECRET", None),
                OIDC_CONFIGURATION_ENDPOINT=getenv(
                    f"{prefix}_OIDC_CONFIGURATION_ENDPOINT", None
                ),
                REDIRECT_URL=getenv(f"{prefix}_REDIRECT_URL", None),
            )
            values["OAUTH_CLIENTS_CONFIG"].append(oauth_client_config)
        return values

    ###########################################################################
    # FRACTAL SPECIFIC
    ###########################################################################

    FRACTAL_RUNNER_BACKEND: ResourceType = ResourceType.LOCAL
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
