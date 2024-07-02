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
from os.path import abspath
from pathlib import Path
from typing import Literal
from typing import Optional
from typing import TypeVar

from dotenv import load_dotenv
from pydantic import BaseModel
from pydantic import BaseSettings
from pydantic import Field
from pydantic import root_validator
from pydantic import validator
from sqlalchemy.engine import URL

import fractal_server


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
    CLIENT_SECRET: str
    OIDC_CONFIGURATION_ENDPOINT: Optional[str]
    REDIRECT_URL: Optional[str] = None

    @root_validator
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

    The attributes of this class are set from the environtment.
    """

    class Config:
        case_sensitive = True

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

    JWT_SECRET_KEY: Optional[str]
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

    @root_validator(pre=True)
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
    DB_ENGINE: Literal["sqlite", "postgres", "postgres-psycopg"] = "sqlite"
    """
    Select which database engine to use (supported: `sqlite` and `postgres`).
    """
    DB_ECHO: bool = False
    """
    If `True`, make database operations verbose.
    """
    POSTGRES_USER: Optional[str]
    """
    User to use when connecting to the PostgreSQL database.
    """
    POSTGRES_PASSWORD: Optional[str]
    """
    Password to use when connecting to the PostgreSQL database.
    """
    POSTGRES_HOST: Optional[str] = "localhost"
    """
    URL to the PostgreSQL server or path to a UNIX domain socket.
    """
    POSTGRES_PORT: Optional[str] = "5432"
    """
    Port number to use when connecting to the PostgreSQL server.
    """
    POSTGRES_DB: Optional[str]
    """
    Name of the PostgreSQL database to connect to.
    """

    SQLITE_PATH: Optional[str]
    """
    File path where the SQLite database is located (or will be located).
    """

    @property
    def DATABASE_ASYNC_URL(self) -> URL:
        if self.DB_ENGINE == "postgres":
            url = URL.create(
                drivername="postgresql+asyncpg",
                username=self.POSTGRES_USER,
                password=self.POSTGRES_PASSWORD,
                host=self.POSTGRES_HOST,
                port=self.POSTGRES_PORT,
                database=self.POSTGRES_DB,
            )
        elif self.DB_ENGINE == "postgres-psycopg":
            url = URL.create(
                drivername="postgresql+psycopg",
                username=self.POSTGRES_USER,
                password=self.POSTGRES_PASSWORD,
                host=self.POSTGRES_HOST,
                port=self.POSTGRES_PORT,
                database=self.POSTGRES_DB,
            )
        else:
            if not self.SQLITE_PATH:
                raise FractalConfigurationError(
                    "SQLITE_PATH path cannot be None"
                )
            sqlite_path = abspath(self.SQLITE_PATH)
            url = URL.create(
                drivername="sqlite+aiosqlite",
                database=sqlite_path,
            )
        return url

    @property
    def DATABASE_SYNC_URL(self):
        if self.DB_ENGINE == "postgres":
            return self.DATABASE_ASYNC_URL.set(
                drivername="postgresql+psycopg2"
            )
        elif self.DB_ENGINE == "postgres-psycopg":
            return self.DATABASE_ASYNC_URL.set(drivername="postgresql+psycopg")
        else:
            if not self.SQLITE_PATH:
                raise FractalConfigurationError(
                    "SQLITE_PATH path cannot be None"
                )
            return self.DATABASE_ASYNC_URL.set(drivername="sqlite")

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

    FRACTAL_DEFAULT_ADMIN_PASSWORD: str = "1234"
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

    FRACTAL_TASKS_DIR: Optional[Path]
    """
    Directory under which all the tasks will be saved (either an absolute path
    or a path relative to current working directory).
    """

    @validator("FRACTAL_TASKS_DIR", always=True)
    def make_FRACTAL_TASKS_DIR_absolute(cls, v):
        """
        If `FRACTAL_TASKS_DIR` is a non-absolute path, make it absolute (based
        on the current working directory).
        """
        if v is None:
            return None
        FRACTAL_TASKS_DIR_path = Path(v)
        if not FRACTAL_TASKS_DIR_path.is_absolute():
            FRACTAL_TASKS_DIR_path = FRACTAL_TASKS_DIR_path.resolve()
            logging.warning(
                f'FRACTAL_TASKS_DIR="{v}" is not an absolute path; '
                f'converting it to "{str(FRACTAL_TASKS_DIR_path)}"'
            )
        return FRACTAL_TASKS_DIR_path

    @validator("FRACTAL_RUNNER_WORKING_BASE_DIR", always=True)
    def make_FRACTAL_RUNNER_WORKING_BASE_DIR_absolute(cls, v):
        """
        (Copy of make_FRACTAL_TASKS_DIR_absolute)
        If `FRACTAL_RUNNER_WORKING_BASE_DIR` is a non-absolute path,
        make it absolute (based on the current working directory).
        """
        if v is None:
            return None
        FRACTAL_RUNNER_WORKING_BASE_DIR_path = Path(v)
        if not FRACTAL_RUNNER_WORKING_BASE_DIR_path.is_absolute():
            FRACTAL_RUNNER_WORKING_BASE_DIR_path = (
                FRACTAL_RUNNER_WORKING_BASE_DIR_path.resolve()
            )
            logging.warning(
                f'FRACTAL_RUNNER_WORKING_BASE_DIR="{v}" is not an absolute '
                "path; converting it to "
                f'"{str(FRACTAL_RUNNER_WORKING_BASE_DIR_path)}"'
            )
        return FRACTAL_RUNNER_WORKING_BASE_DIR_path

    FRACTAL_RUNNER_BACKEND: Literal[
        "local",
        "local_experimental",
        "slurm",
        "slurm_ssh",
    ] = "local"
    """
    Select which runner backend to use.
    """

    FRACTAL_RUNNER_WORKING_BASE_DIR: Optional[Path]
    """
    Base directory for running jobs / workflows. All artifacts required to set
    up, run and tear down jobs are placed in subdirs of this directory.
    """

    FRACTAL_LOGGING_LEVEL: int = logging.INFO
    """
    Logging-level threshold for logging

    Only logs of with this level (or higher) will appear in the console logs;
    see details [here](../internals/logs/).
    """

    FRACTAL_LOCAL_CONFIG_FILE: Optional[Path]
    """
    Path of JSON file with configuration for the local backend.
    """

    FRACTAL_API_MAX_JOB_LIST_LENGTH: int = 50
    """
    Number of ids that can be stored in the `jobsV1` and `jobsV2` attributes of
    `app.state`.
    """

    FRACTAL_GRACEFUL_SHUTDOWN_TIME: int = 30
    """
    Waiting time for the shutdown phase of executors
    """

    FRACTAL_SLURM_CONFIG_FILE: Optional[Path]
    """
    Path of JSON file with configuration for the SLURM backend.
    """

    FRACTAL_SLURM_WORKER_PYTHON: Optional[str] = None
    """
    Absolute path to Python interpreter that will run the jobs on the SLURM
    nodes. If not specified, the same interpreter that runs the server is used.
    """

    @validator("FRACTAL_SLURM_WORKER_PYTHON", always=True)
    def absolute_FRACTAL_SLURM_WORKER_PYTHON(cls, v):
        """
        If `FRACTAL_SLURM_WORKER_PYTHON` is a relative path, fail.
        """
        if v is None:
            return None
        elif not Path(v).is_absolute():
            raise FractalConfigurationError(
                f"Non-absolute value for FRACTAL_SLURM_WORKER_PYTHON={v}"
            )
        else:
            return v

    FRACTAL_TASKS_PYTHON_DEFAULT_VERSION: Optional[
        Literal["3.9", "3.10", "3.11", "3.12"]
    ] = None
    """
    Default Python version to be used for task collection. Defaults to the
    current version. Requires the corresponding variable (e.g
    `FRACTAL_TASKS_PYTHON_3_10`) to be set.
    """

    FRACTAL_TASKS_PYTHON_3_9: Optional[str] = None
    """
    Absolute path to the Python 3.9 interpreter that serves as base for virtual
    environments tasks. Note that this interpreter must have the `venv` module
    installed. If set, this must be an absolute path. If the version specified
    in `FRACTAL_TASKS_PYTHON_DEFAULT_VERSION` is `"3.9"` and this attribute is
    unset, `sys.executable` is used as a default.
    """

    FRACTAL_TASKS_PYTHON_3_10: Optional[str] = None
    """
    Same as `FRACTAL_TASKS_PYTHON_3_9`, for Python 3.10.
    """

    FRACTAL_TASKS_PYTHON_3_11: Optional[str] = None
    """
    Same as `FRACTAL_TASKS_PYTHON_3_9`, for Python 3.11.
    """

    FRACTAL_TASKS_PYTHON_3_12: Optional[str] = None
    """
    Same as `FRACTAL_TASKS_PYTHON_3_9`, for Python 3.12.
    """

    @root_validator(pre=True)
    def check_tasks_python(cls, values) -> None:
        """
        Perform multiple checks of the Python-intepreter variables.

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

            # Unset all existing intepreters variable
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
    still running on SLURM (see `SlurmWaitThread` in
    [`clusterfutures`](https://github.com/sampsyo/clusterfutures/blob/master/cfut/__init__.py)).
    """

    FRACTAL_SLURM_SBATCH_SLEEP: int = 0
    """
    Interval to wait (in seconds) between two subsequent `sbatch` calls, when
    running a task that produces multiple SLURM jobs.
    """

    FRACTAL_SLURM_ERROR_HANDLING_INTERVAL: int = 5
    """
    Interval to wait (in seconds) when the SLURM backend does not find an
    output pickle file - which could be due to several reasons (e.g. the SLURM
    job was cancelled or failed, or writing the file is taking long). If the
    file is still missing after this time interval, this leads to a
    `JobExecutionError`.
    """

    FRACTAL_SLURM_SSH_HOST: Optional[str] = None
    """
    SSH-reachable host where a SLURM client is available.
    """
    FRACTAL_SLURM_SSH_USER: Optional[str] = None
    """
    User on `FRACTAL_SLURM_SSH_HOST`.
    """
    FRACTAL_SLURM_SSH_PRIVATE_KEY_PATH: Optional[str] = None
    """
    Private key for connecting to `FRACTAL_SLURM_SSH_HOST` as
    `FRACTAL_SLURM_SSH_USER`.
    """
    # FIXME SSH: Split this into two folders (for tasks and for jobs)
    FRACTAL_SLURM_SSH_WORKING_BASE_DIR: Optional[str] = None
    """
    Remote folder on `FRACTAL_SLURM_SSH_HOST`.
    """

    FRACTAL_API_SUBMIT_RATE_LIMIT: int = 2
    """
    Interval to wait (in seconds) to be allowed to call again
    `POST api/v1/{project_id}/workflow/{workflow_id}/apply/`
    with the same path and query parameters.
    """

    FRACTAL_RUNNER_TASKS_INCLUDE_IMAGE: str = (
        "Copy OME-Zarr structure;Convert Metadata Components from 2D to 3D"
    )
    """
    `;`-separated list of names for task that require the `metadata["image"]`
    attribute in their input-arguments JSON file.
    """

    FRACTAL_API_V1_MODE: Literal["include", "exclude"] = "include"
    """
    Whether to include the v1 API.
    """

    ###########################################################################
    # BUSINESS LOGIC
    ###########################################################################
    def check_db(self) -> None:
        """
        Checks that db environment variables are properly set.
        """
        if self.DB_ENGINE == "postgres":
            if not self.POSTGRES_DB:
                raise FractalConfigurationError(
                    "POSTGRES_DB cannot be None when DB_ENGINE=postgres."
                )
            try:
                import psycopg2  # noqa: F401
                import asyncpg  # noqa: F401
            except ModuleNotFoundError:
                raise FractalConfigurationError(
                    "DB engine is `postgres` but `psycopg2` or `asyncpg` "
                    "are not available"
                )
        elif self.DB_ENGINE == "postgres-psycopg":
            try:
                import psycopg  # noqa: F401
            except ModuleNotFoundError:
                raise FractalConfigurationError(
                    "DB engine is `postgres-psycopg` but `psycopg` is not "
                    "available"
                )
        else:
            if not self.SQLITE_PATH:
                raise FractalConfigurationError(
                    "SQLITE_PATH cannot be None when DB_ENGINE=sqlite."
                )

    def check_runner(self) -> None:

        if not self.FRACTAL_RUNNER_WORKING_BASE_DIR:
            raise FractalConfigurationError(
                "FRACTAL_RUNNER_WORKING_BASE_DIR cannot be None."
            )

        info = f"FRACTAL_RUNNER_BACKEND={self.FRACTAL_RUNNER_BACKEND}"
        if self.FRACTAL_RUNNER_BACKEND == "slurm":

            from fractal_server.app.runner.executors.slurm._slurm_config import (  # noqa: E501
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
            if self.FRACTAL_SLURM_SSH_USER is None:
                raise FractalConfigurationError(
                    f"Must set FRACTAL_SLURM_SSH_USER when {info}"
                )
            if self.FRACTAL_SLURM_SSH_HOST is None:
                raise FractalConfigurationError(
                    f"Must set FRACTAL_SLURM_SSH_HOST when {info}"
                )
            if self.FRACTAL_SLURM_SSH_PRIVATE_KEY_PATH is None:
                raise FractalConfigurationError(
                    f"Must set FRACTAL_SLURM_SSH_PRIVATE_KEY_PATH when {info}"
                )
            if self.FRACTAL_SLURM_SSH_WORKING_BASE_DIR is None:
                raise FractalConfigurationError(
                    f"Must set FRACTAL_SLURM_SSH_WORKING_BASE_DIR when {info}"
                )

            from fractal_server.app.runner.executors.slurm._slurm_config import (  # noqa: E501
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

        self.check_db()
        self.check_runner()


def get_settings(settings=Settings()) -> Settings:
    return settings
