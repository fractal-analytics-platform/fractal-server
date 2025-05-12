# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
# Yuri Chiucconi <yuri.chiucconi@exact-lab.it>
# Marco Franzon <marco.franzon@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
import logging
import shutil
import sys
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
from pydantic import field_validator
from pydantic import model_validator
from pydantic import SecretStr
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from sqlalchemy.engine import URL

import fractal_server
from fractal_server.types import AbsolutePathStr


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

    PROJECT_NAME: str = "Fractal Server"
    PROJECT_VERSION: str = fractal_server.__VERSION__

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

    FRACTAL_TASKS_DIR: Path | None = None
    """
    Directory under which all the tasks will be saved (either an absolute path
    or a path relative to current working directory).
    """

    FRACTAL_RUNNER_WORKING_BASE_DIR: Path | None = None
    """
    Base directory for job files (either an absolute path or a path relative to
    current working directory).
    """

    @field_validator(
        "FRACTAL_TASKS_DIR",
        "FRACTAL_RUNNER_WORKING_BASE_DIR",
        mode="after",
    )
    @classmethod
    def make_paths_absolute(cls, path: Path | None) -> Path | None:
        if path is None or path.is_absolute():
            return path
        else:
            logging.warning(
                f"'{path}' is not an absolute path; "
                f"converting it to '{path.resolve()}'"
            )
            return path.resolve()

    FRACTAL_RUNNER_BACKEND: Literal[
        "local",
        "slurm",
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

    FRACTAL_LOCAL_CONFIG_FILE: Path | None = None
    """
    Path of JSON file with configuration for the local backend.
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

    FRACTAL_SLURM_CONFIG_FILE: Path | None = None
    """
    Path of JSON file with configuration for the SLURM backend.
    """

    FRACTAL_SLURM_WORKER_PYTHON: AbsolutePathStr | None = None
    """
    Absolute path to Python interpreter that will run the jobs on the SLURM
    nodes. If not specified, the same interpreter that runs the server is used.
    """

    FRACTAL_TASKS_PYTHON_DEFAULT_VERSION: None | (
        Literal["3.9", "3.10", "3.11", "3.12"]
    ) = None
    """
    Default Python version to be used for task collection. Defaults to the
    current version. Requires the corresponding variable (e.g
    `FRACTAL_TASKS_PYTHON_3_10`) to be set.
    """

    FRACTAL_TASKS_PYTHON_3_9: str | None = None
    """
    Absolute path to the Python 3.9 interpreter that serves as base for virtual
    environments tasks. Note that this interpreter must have the `venv` module
    installed. If set, this must be an absolute path. If the version specified
    in `FRACTAL_TASKS_PYTHON_DEFAULT_VERSION` is `"3.9"` and this attribute is
    unset, `sys.executable` is used as a default.
    """

    FRACTAL_TASKS_PYTHON_3_10: str | None = None
    """
    Same as `FRACTAL_TASKS_PYTHON_3_9`, for Python 3.10.
    """

    FRACTAL_TASKS_PYTHON_3_11: str | None = None
    """
    Same as `FRACTAL_TASKS_PYTHON_3_9`, for Python 3.11.
    """

    FRACTAL_TASKS_PYTHON_3_12: str | None = None
    """
    Same as `FRACTAL_TASKS_PYTHON_3_9`, for Python 3.12.
    """

    @model_validator(mode="before")
    @classmethod
    def check_tasks_python(cls, values):
        """
        Perform multiple checks of the Python-interpreter variables.

        1. Each `FRACTAL_TASKS_PYTHON_X_Y` variable must be an absolute path,
            if set.
        2. If `FRACTAL_TASKS_PYTHON_DEFAULT_VERSION` is unset, use
            `sys.executable` and set the corresponding
            `FRACTAL_TASKS_PYTHON_X_Y` (and unset all others).
        """
        # `FRACTAL_TASKS_PYTHON_X_Y` variables can only be absolute paths
        for version in ["3_9", "3_10", "3_11", "3_12"]:
            key = f"FRACTAL_TASKS_PYTHON_{version}"
            value = values.get(key)
            if value is not None and not Path(value).is_absolute():
                raise FractalConfigurationError(
                    f"Non-absolute value {key}={value}"
                )

        default_version = values.get("FRACTAL_TASKS_PYTHON_DEFAULT_VERSION")

        if default_version is not None:
            # "production/slurm" branch
            # If a default version is set, then the corresponding interpreter
            # must also be set
            default_version_undescore = default_version.replace(".", "_")
            key = f"FRACTAL_TASKS_PYTHON_{default_version_undescore}"
            value = values.get(key)
            if value is None:
                msg = (
                    f"FRACTAL_TASKS_PYTHON_DEFAULT_VERSION={default_version} "
                    f"but {key}={value}."
                )
                logging.error(msg)
                raise FractalConfigurationError(msg)

        else:
            # If no default version is set, then only `sys.executable` is made
            # available
            _info = sys.version_info
            current_version = f"{_info.major}_{_info.minor}"
            current_version_dot = f"{_info.major}.{_info.minor}"
            values[
                "FRACTAL_TASKS_PYTHON_DEFAULT_VERSION"
            ] = current_version_dot
            logging.info(
                "Setting FRACTAL_TASKS_PYTHON_DEFAULT_VERSION to "
                f"{current_version_dot}"
            )

            # Unset all existing interpreters variable
            for _version in ["3_9", "3_10", "3_11", "3_12"]:
                key = f"FRACTAL_TASKS_PYTHON_{_version}"
                if _version == current_version:
                    values[key] = sys.executable
                    logging.info(f"Setting {key} to {sys.executable}.")
                else:
                    value = values.get(key)
                    if value is not None:
                        logging.info(
                            f"Setting {key} to None (given: {value}), "
                            "because FRACTAL_TASKS_PYTHON_DEFAULT_VERSION was "
                            "not set."
                        )
                    values[key] = None
        return values

    FRACTAL_SLURM_POLL_INTERVAL: int = 5
    """
    Interval to wait (in seconds) before checking whether unfinished job are
    still running on SLURM.
    """

    FRACTAL_SLURM_SBATCH_SLEEP: float = 0
    """
    Interval to wait (in seconds) between two subsequent `sbatch` calls, when
    running a task that produces multiple SLURM jobs.
    """

    FRACTAL_PIP_CACHE_DIR: AbsolutePathStr | None = None
    """
    Absolute path to the cache directory for `pip`; if unset,
    `--no-cache-dir` is used.
    """

    @property
    def PIP_CACHE_DIR_ARG(self) -> str:
        """
        Option for `pip install`, based on `FRACTAL_PIP_CACHE_DIR` value.

        If `FRACTAL_PIP_CACHE_DIR` is set, then return
        `--cache-dir /somewhere`; else return `--no-cache-dir`.
        """
        if self.FRACTAL_PIP_CACHE_DIR is not None:
            return f"--cache-dir {self.FRACTAL_PIP_CACHE_DIR}"
        else:
            return "--no-cache-dir"

    FRACTAL_MAX_PIP_VERSION: str = "24.0"
    """
    Maximum value at which to update `pip` before performing task collection.
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

    def check_runner(self) -> None:
        if not self.FRACTAL_RUNNER_WORKING_BASE_DIR:
            raise FractalConfigurationError(
                "FRACTAL_RUNNER_WORKING_BASE_DIR cannot be None."
            )

        info = f"FRACTAL_RUNNER_BACKEND={self.FRACTAL_RUNNER_BACKEND}"
        if self.FRACTAL_RUNNER_BACKEND == "slurm":
            from fractal_server.app.runner.executors.slurm_common._slurm_config import (  # noqa: E501
                load_slurm_config_file,
            )

            if not self.FRACTAL_SLURM_CONFIG_FILE:
                raise FractalConfigurationError(
                    f"Must set FRACTAL_SLURM_CONFIG_FILE when {info}"
                )
            else:
                if not self.FRACTAL_SLURM_CONFIG_FILE.exists():
                    raise FractalConfigurationError(
                        f"{info} but FRACTAL_SLURM_CONFIG_FILE="
                        f"{self.FRACTAL_SLURM_CONFIG_FILE} not found."
                    )

                load_slurm_config_file(self.FRACTAL_SLURM_CONFIG_FILE)
                if not shutil.which("sbatch"):
                    raise FractalConfigurationError(
                        f"{info} but `sbatch` command not found."
                    )
                if not shutil.which("squeue"):
                    raise FractalConfigurationError(
                        f"{info} but `squeue` command not found."
                    )
        elif self.FRACTAL_RUNNER_BACKEND == "slurm_ssh":
            if self.FRACTAL_SLURM_WORKER_PYTHON is None:
                raise FractalConfigurationError(
                    f"Must set FRACTAL_SLURM_WORKER_PYTHON when {info}"
                )

            from fractal_server.app.runner.executors.slurm_common._slurm_config import (  # noqa: E501
                load_slurm_config_file,
            )

            if not self.FRACTAL_SLURM_CONFIG_FILE:
                raise FractalConfigurationError(
                    f"Must set FRACTAL_SLURM_CONFIG_FILE when {info}"
                )
            else:
                if not self.FRACTAL_SLURM_CONFIG_FILE.exists():
                    raise FractalConfigurationError(
                        f"{info} but FRACTAL_SLURM_CONFIG_FILE="
                        f"{self.FRACTAL_SLURM_CONFIG_FILE} not found."
                    )

                load_slurm_config_file(self.FRACTAL_SLURM_CONFIG_FILE)
                if not shutil.which("ssh"):
                    raise FractalConfigurationError(
                        f"{info} but `ssh` command not found."
                    )
        else:  # i.e. self.FRACTAL_RUNNER_BACKEND == "local"
            if self.FRACTAL_LOCAL_CONFIG_FILE:
                if not self.FRACTAL_LOCAL_CONFIG_FILE.exists():
                    raise FractalConfigurationError(
                        f"{info} but FRACTAL_LOCAL_CONFIG_FILE="
                        f"{self.FRACTAL_LOCAL_CONFIG_FILE} not found."
                    )

    def check(self):
        """
        Make sure that required variables are set

        This method must be called before the server starts
        """

        if not self.JWT_SECRET_KEY:
            raise FractalConfigurationError("JWT_SECRET_KEY cannot be None")

        if not self.FRACTAL_TASKS_DIR:
            raise FractalConfigurationError("FRACTAL_TASKS_DIR cannot be None")

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
        self.check_runner()


def get_settings(settings=Settings()) -> Settings:
    return settings
