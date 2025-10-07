import logging
from os import environ
from os import getenv
from pathlib import Path
from typing import Literal
from typing import TypeVar

from cryptography.fernet import Fernet
from dotenv import load_dotenv
from pydantic import BaseModel
from pydantic import EmailStr
from pydantic import Field
from pydantic import model_validator
from pydantic import SecretStr
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from sqlalchemy.engine import URL


class MailSettings(BaseModel):
    """
    Schema for `MailSettings`

    Attributes:
        sender: Sender email address
        recipients: List of recipients email address
        smtp_server: SMTP server address
        port: SMTP server port
        password: Sender password
        instance_name: Name of SMTP server instance
        use_starttls: Whether to use the security protocol
        use_login: Whether to use login
    """

    sender: EmailStr
    recipients: list[EmailStr] = Field(min_length=1)
    smtp_server: str
    port: int
    encrypted_password: SecretStr | None = None
    encryption_key: SecretStr | None = None
    instance_name: str
    use_starttls: bool
    use_login: bool


class FractalConfigurationError(RuntimeError):
    pass


T = TypeVar("T")


load_dotenv(".fractal_server.env")


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

    model_config = SettingsConfigDict(case_sensitive=True)
    ###########################################################################
    # AUTH
    ###########################################################################

    OAUTH_CLIENTS_CONFIG: list[OAuthClientConfig] = Field(default_factory=list)

    # JWT TOKEN
    JWT_EXPIRE_SECONDS: int = 180
    """
    JWT token lifetime, in seconds.
    """

    JWT_SECRET_KEY: SecretStr | None = None
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
    # DATABASE
    ###########################################################################
    DB_ECHO: bool = False
    """
    If `True`, make database operations verbose.
    """
    POSTGRES_USER: str | None = None
    """
    User to use when connecting to the PostgreSQL database.
    """
    POSTGRES_PASSWORD: SecretStr | None = None
    """
    Password to use when connecting to the PostgreSQL database.
    """
    POSTGRES_HOST: str | None = "localhost"
    """
    URL to the PostgreSQL server or path to a UNIX domain socket.
    """
    POSTGRES_PORT: str | None = "5432"
    """
    Port number to use when connecting to the PostgreSQL server.
    """
    POSTGRES_DB: str | None = None
    """
    Name of the PostgreSQL database to connect to.
    """

    @property
    def DATABASE_ASYNC_URL(self) -> URL:
        if self.POSTGRES_PASSWORD is None:
            password = None
        else:
            password = self.POSTGRES_PASSWORD.get_secret_value()

        url = URL.create(
            drivername="postgresql+psycopg",
            username=self.POSTGRES_USER,
            password=password,
            host=self.POSTGRES_HOST,
            port=self.POSTGRES_PORT,
            database=self.POSTGRES_DB,
        )
        return url

    @property
    def DATABASE_SYNC_URL(self):
        return self.DATABASE_ASYNC_URL.set(drivername="postgresql+psycopg")

    ###########################################################################
    # FRACTAL SPECIFIC
    ###########################################################################

    FRACTAL_DEFAULT_ADMIN_EMAIL: str = "admin@fractal.xy"
    """
    Admin default email, used upon creation of the first superuser during
    server startup.

    ⚠️  **IMPORTANT**: After the server startup, you should always edit the
    default admin credentials.
    """

    FRACTAL_DEFAULT_ADMIN_PASSWORD: SecretStr = "1234"
    """
    Admin default password, used upon creation of the first superuser during
    server startup.

    ⚠️ **IMPORTANT**: After the server startup, you should always edit the
    default admin credentials.
    """

    FRACTAL_DEFAULT_ADMIN_USERNAME: str = "admin"
    """
    Admin default username, used upon creation of the first superuser during
    server startup.

    ⚠️ **IMPORTANT**: After the server startup, you should always edit the
    default admin credentials.
    """

    FRACTAL_RUNNER_BACKEND: Literal[
        "local",
        "slurm_sudo",
        "slurm_ssh",
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

    FRACTAL_VIEWER_BASE_FOLDER: str | None = None
    """
    Base path to Zarr files that will be served by fractal-vizarr-viewer;
    This variable is required and used only when
    FRACTAL_VIEWER_AUTHORIZATION_SCHEME is set to "users-folders".
    """

    ###########################################################################
    # SMTP SERVICE
    ###########################################################################

    FRACTAL_EMAIL_SENDER: EmailStr | None = None
    """
    Address of the OAuth-signup email sender.
    """
    FRACTAL_EMAIL_PASSWORD: SecretStr | None = None
    """
    Password for the OAuth-signup email sender.
    """
    FRACTAL_EMAIL_PASSWORD_KEY: SecretStr | None = None
    """
    Key value for `cryptography.fernet` decrypt
    """
    FRACTAL_EMAIL_SMTP_SERVER: str | None = None
    """
    SMTP server for the OAuth-signup emails.
    """
    FRACTAL_EMAIL_SMTP_PORT: int | None = None
    """
    SMTP server port for the OAuth-signup emails.
    """
    FRACTAL_EMAIL_INSTANCE_NAME: str | None = None
    """
    Fractal instance name, to be included in the OAuth-signup emails.
    """
    FRACTAL_EMAIL_RECIPIENTS: str | None = None
    """
    Comma-separated list of recipients of the OAuth-signup emails.
    """
    FRACTAL_EMAIL_USE_STARTTLS: Literal["true", "false"] = "true"
    """
    Whether to use StartTLS when using the SMTP server.
    Accepted values: 'true', 'false'.
    """
    FRACTAL_EMAIL_USE_LOGIN: Literal["true", "false"] = "true"
    """
    Whether to use login when using the SMTP server.
    If 'true', FRACTAL_EMAIL_PASSWORD and FRACTAL_EMAIL_PASSWORD_KEY must be
    provided.
    Accepted values: 'true', 'false'.
    """
    email_settings: MailSettings | None = None

    @model_validator(mode="after")
    def validate_email_settings(self):
        email_values = [
            self.FRACTAL_EMAIL_SENDER,
            self.FRACTAL_EMAIL_SMTP_SERVER,
            self.FRACTAL_EMAIL_SMTP_PORT,
            self.FRACTAL_EMAIL_INSTANCE_NAME,
            self.FRACTAL_EMAIL_RECIPIENTS,
        ]
        if len(set(email_values)) == 1:
            # All required EMAIL attributes are None
            pass
        elif None in email_values:
            # Not all required EMAIL attributes are set
            error_msg = (
                "Invalid FRACTAL_EMAIL configuration. "
                f"Given values: {email_values}."
            )
            raise ValueError(error_msg)
        else:
            use_starttls = self.FRACTAL_EMAIL_USE_STARTTLS == "true"
            use_login = self.FRACTAL_EMAIL_USE_LOGIN == "true"

            if use_login:
                if self.FRACTAL_EMAIL_PASSWORD is None:
                    raise ValueError(
                        "'FRACTAL_EMAIL_USE_LOGIN' is 'true' but "
                        "'FRACTAL_EMAIL_PASSWORD' is not provided."
                    )
                if self.FRACTAL_EMAIL_PASSWORD_KEY is None:
                    raise ValueError(
                        "'FRACTAL_EMAIL_USE_LOGIN' is 'true' but "
                        "'FRACTAL_EMAIL_PASSWORD_KEY' is not provided."
                    )
                try:
                    (
                        Fernet(
                            self.FRACTAL_EMAIL_PASSWORD_KEY.get_secret_value()
                        )
                        .decrypt(
                            self.FRACTAL_EMAIL_PASSWORD.get_secret_value()
                        )
                        .decode("utf-8")
                    )
                except Exception as e:
                    raise ValueError(
                        "Invalid pair (FRACTAL_EMAIL_PASSWORD, "
                        "FRACTAL_EMAIL_PASSWORD_KEY). "
                        f"Original error: {str(e)}."
                    )
                password = self.FRACTAL_EMAIL_PASSWORD.get_secret_value()
            else:
                password = None

            if self.FRACTAL_EMAIL_PASSWORD_KEY is not None:
                key = self.FRACTAL_EMAIL_PASSWORD_KEY.get_secret_value()
            else:
                key = None

            self.email_settings = MailSettings(
                sender=self.FRACTAL_EMAIL_SENDER,
                recipients=self.FRACTAL_EMAIL_RECIPIENTS.split(","),
                smtp_server=self.FRACTAL_EMAIL_SMTP_SERVER,
                port=self.FRACTAL_EMAIL_SMTP_PORT,
                encrypted_password=password,
                encryption_key=key,
                instance_name=self.FRACTAL_EMAIL_INSTANCE_NAME,
                use_starttls=use_starttls,
                use_login=use_login,
            )

        return self

    ###########################################################################
    # BUSINESS LOGIC
    ###########################################################################

    def check_db(self) -> None:
        """
        Checks that db environment variables are properly set.
        """
        if not self.POSTGRES_DB:
            raise FractalConfigurationError("POSTGRES_DB cannot be None.")

    def check(self):
        """
        Make sure that required variables are set

        This method must be called before the server starts
        """

        if not self.JWT_SECRET_KEY:
            raise FractalConfigurationError("JWT_SECRET_KEY cannot be None")

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
            if not Path(viewer_base_folder).is_absolute():
                raise FractalConfigurationError(
                    f"Non-absolute value for "
                    f"FRACTAL_VIEWER_BASE_FOLDER={viewer_base_folder}"
                )

        self.check_db()


def get_settings(settings=Settings()) -> Settings:
    return settings
