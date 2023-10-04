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
    """

    CLIENT_NAME: str
    CLIENT_ID: str
    CLIENT_SECRET: str
    OIDC_CONFIGURATION_ENDPOINT: Optional[str]

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
            key for key in environ.keys() if "OAUTH" in key
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
            )
            values["OAUTH_CLIENTS_CONFIG"].append(oauth_client_config)
        return values

    ###########################################################################
    # DATABASE
    ###########################################################################
    DB_ENGINE: Literal["sqlite", "postgres"] = "sqlite"
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
    def DATABASE_URL(self) -> URL:
        if self.DB_ENGINE == "sqlite":
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
        elif "postgres":
            url = URL.create(
                drivername="postgresql+asyncpg",
                username=self.POSTGRES_USER,
                password=self.POSTGRES_PASSWORD,
                host=self.POSTGRES_HOST,
                port=self.POSTGRES_PORT,
                database=self.POSTGRES_DB,
            )
            return url

    @property
    def DATABASE_SYNC_URL(self):
        if self.DB_ENGINE == "sqlite":
            if not self.SQLITE_PATH:
                raise FractalConfigurationError(
                    "SQLITE_PATH path cannot be None"
                )
            return self.DATABASE_URL.set(drivername="sqlite")
        elif self.DB_ENGINE == "postgres":
            return self.DATABASE_URL.set(drivername="postgresql+psycopg2")

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

    FRACTAL_RUNNER_BACKEND: Literal["local", "slurm"] = "local"
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

    FRACTAL_SLURM_CONFIG_FILE: Optional[Path]
    """
    Path of JSON file with configuration for the SLURM backend.
    """

    FRACTAL_SLURM_WORKER_PYTHON: Optional[str] = None
    """
    Path to Python interpreter that will run the jobs on the SLURM nodes. If
    not specified, the same interpreter that runs the server is used.
    """

    FRACTAL_SLURM_POLL_INTERVAL: int = 5
    """
    Interval to wait (in seconds) before checking whether unfinished job are
    still running on SLURM (see `SlurmWaitThread` in
    [`clusterfutures`](https://github.com/sampsyo/clusterfutures/blob/master/cfut/__init__.py)).
    """

    FRACTAL_SLURM_ERROR_HANDLING_INTERVAL: int = 5
    """
    Interval to wait (in seconds) when the SLURM backend does not find an
    output pickle file - which could be due to several reasons (e.g. the SLURM
    job was cancelled or failed, or writing the file is taking long). If the
    file is still missing after this time interval, this leads to a
    `JobExecutionError`.
    """

    FRACTAL_CORS_ALLOW_ORIGIN: str = (
        "http://127.0.0.1:5173;http://localhost:5173"
    )
    """
    Allowed origins for CORS middleware configuration.
    Default values correspond to `vite` defaults.
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

                from fractal_server.app.runner._slurm._slurm_config import (
                    load_slurm_config_file,
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
        else:  # i.e. self.FRACTAL_RUNNER_BACKEND == "local"
            if self.FRACTAL_LOCAL_CONFIG_FILE:
                if not self.FRACTAL_LOCAL_CONFIG_FILE.exists():
                    raise FractalConfigurationError(
                        f"{info} but FRACTAL_LOCAL_CONFIG_FILE="
                        f"{self.FRACTAL_LOCAL_CONFIG_FILE} not found."
                    )

    def check(self):
        """
        Make sure that mandatory variables are set

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
