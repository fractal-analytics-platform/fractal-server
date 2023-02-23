# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
# Marco Franzon <marco.franzon@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
import shutil
from os import environ
from os import getenv
from os.path import abspath
from pathlib import Path
from typing import List
from typing import Literal
from typing import Optional
from typing import TypeVar

from dotenv import load_dotenv
from pydantic import BaseModel
from pydantic import BaseSettings
from pydantic import Field
from pydantic import root_validator

import fractal_server


class FractalConfigurationError(RuntimeError):
    pass


T = TypeVar("T")


load_dotenv(".fractal_server.env")


class OAuthClient(BaseModel):
    """
    OAuth Client Model

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

        AUTHORIZE_ENDPOINT:
            Authorization endpoint
        ACCESS_TOKEN_ENDPOINT:
            Token endpoint
        REFRESH_TOKEN_ENDPOINT:
            Refresh token endpoint
        REVOKE_TOKEN_ENDPOINT:
            Revoke token endpoint
    """

    CLIENT_NAME: str
    CLIENT_ID: str
    CLIENT_SECRET: str

    AUTHORIZE_ENDPOINT: Optional[str]
    ACCESS_TOKEN_ENDPOINT: Optional[str]
    REFRESH_TOKEN_ENDPOINT: Optional[str]
    REVOKE_TOKEN_ENDPOINT: Optional[str]


class Settings(BaseSettings):
    """
    Contains all the configuration variables for Fractal Server

    The attributes of this class are set from the environtment.
    """

    class Config:
        case_sensitive = True

    PROJECT_NAME: str = "Fractal Server"
    PROJECT_VERSION: str = fractal_server.__VERSION__
    DEPLOYMENT_TYPE: Optional[
        Literal["production", "staging", "testing", "development"]
    ]
    """
    The deployment type of the server installation. It is important that
    production deployments be marked as such to trigger server hardening.
    """

    ###########################################################################
    # AUTH
    ###########################################################################

    OAUTH_CLIENTS: List[OAuthClient] = Field(default_factory=list)

    # JWT TOKEN
    JWT_EXPIRE_SECONDS: int = 180
    JWT_SECRET_KEY: Optional[str]

    # COOKIE TOKEN
    COOKIE_EXPIRE_SECONDS: int = 86400

    @root_validator(pre=True)
    def collect_oauth_clients(cls, values):
        """
        Automatic collection of OAuth Clients

        This method collects the environment variables relative to a single
        OAuth client and saves them within the `Settings` object in the form
        of an `OAuthClient` instance.

        Fractal can support an arbitrary number of OAuth providers, which are
        automatically detected by parsing the environment variable names. In
        particular, to set the provider `FOO`, one must specify the variables

            OAUTH_FOO_CLIENT_ID
            OAUTH_FOO_CLIENT_SECRET
            ...

        etc (cf. OAuthClient).
        """
        oauth_env_variable_keys = [
            key for key in environ.keys() if "OAUTH" in key
        ]
        clients_available = {
            var.split("_")[1] for var in oauth_env_variable_keys
        }

        values["OAUTH_CLIENTS"] = []
        for client in clients_available:
            prefix = f"OAUTH_{client}"
            oauth_client = OAuthClient(
                CLIENT_NAME=client,
                CLIENT_ID=getenv(f"{prefix}_CLIENT_ID", None),
                CLIENT_SECRET=getenv(f"{prefix}_CLIENT_SECRET", None),
                AUTHORIZE_ENDPOINT=getenv(
                    f"{prefix}_AUTHORIZE_ENDPOINT", None
                ),
                ACCESS_TOKEN_ENDPOINT=getenv(
                    f"{prefix}_ACCESS_TOKEN_ENDPOINT", None
                ),
                REFRESH_TOKEN_ENDPOINT=getenv(
                    f"{prefix}_REFRESH_TOKEN_ENDPOINT", None
                ),
                REVOKE_TOKEN_ENDPOINT=getenv(
                    f"{prefix}_REVOKE_TOKEN_ENDPOINT", None
                ),
            )
            values["OAUTH_CLIENTS"].append(oauth_client)
        return values

    ###########################################################################
    # DATABASE
    ###########################################################################
    DB_ENGINE: Literal["sqlite", "postgres"] = "sqlite"
    DB_ECHO: bool = False

    POSTGRES_USER: Optional[str]
    POSTGRES_PASSWORD: Optional[str]
    POSTGRES_SERVER: Optional[str] = "localhost"
    POSTGRES_PORT: Optional[str] = "5432"
    POSTGRES_DB: Optional[str]

    SQLITE_PATH: Optional[str]

    @property
    def DATABASE_URL(self):
        if self.DB_ENGINE == "sqlite":
            if not self.SQLITE_PATH:
                raise ValueError("SQLITE_PATH path cannot be None")
            sqlite_path = (
                abspath(self.SQLITE_PATH)
                if self.SQLITE_PATH
                else self.SQLITE_PATH
            )
            return f"sqlite+aiosqlite:///{sqlite_path}"
        elif "postgres":
            pg_uri = (
                "postgresql+asyncpg://"
                f"{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
                f"@{self.POSTGRES_SERVER}:{self.POSTGRES_PORT}"
                f"/{self.POSTGRES_DB}"
            )
            return pg_uri

    @property
    def DATABASE_SYNC_URL(self):
        if self.DB_ENGINE == "sqlite":
            if not self.SQLITE_PATH:
                raise ValueError("SQLITE_PATH path cannot be None")
            return self.DATABASE_URL.replace("aiosqlite", "pysqlite")
        elif self.DB_ENGINE == "postgres":
            return self.DATABASE_URL.replace("asyncpg", "psycopg2")

    ###########################################################################
    # FRACTAL SPECIFIC
    ###########################################################################

    FRACTAL_DEFAULT_ADMIN_EMAIL: str = "admin@fractal.xy"
    """
    Admin default email, used upon creation of the first superuser during
    server startup.

    **IMPORTANT**: After the server startup, you should always edit the default
    admin credentials.
    """

    FRACTAL_DEFAULT_ADMIN_PASSWORD: str = "1234"
    """
    Admin default password, used upon creation of the first superuser during
    server startup.

    **IMPORTANT**: After the server startup, you should always edit the default
    admin credentials.
    """

    FRACTAL_TASKS_DIR: Optional[Path]
    """
    Directory under which all the tasks will be saved.
    """

    FRACTAL_RUNNER_BACKEND: Literal["local", "slurm"] = "local"
    """
    Select which runner backend to use.
    """

    FRACTAL_RUNNER_WORKING_BASE_DIR: Optional[Path]
    """
    Base directory for running jobs / workflows. All artifacts required to set
    up, run and tear down jobs are placed in subdirs of this directory.
    """

    FRACTAL_LOGGING_LEVEL: int = 30
    """
    Logging verbosity for main Fractal logger (`30` means `WARNING` - see
    [logging
    levels](https://docs.python.org/3/library/logging.html#logging-levels)).
    """

    FRACTAL_RUNNER_MAX_TASKS_PER_WORKFLOW: Optional[int] = None
    """
    Maximum number of components that a parallel task may process
    simultaneously. If `None`, no limit is set.

    Note: this limit concerns a single task in a single workflow execution, but
    it does **not** limit the global (i.e. across workflow executions) number
    of components processed simultaneously.

    Intended use cases:

    1. When using the [local backend](../internals/runners/local/), reduce
    memory requirements of a workflow by capping the number of tasks running in
    parallel.
    2. When using the [SLURM backend](../internals/runners/slurm/), reduce the
    number of SLURM jobs that are submitted at the same time for a given
    workflow; this is e.g. to avoid `AssocMaxSubmitJobLimit` errors related to
    the [`MaxSubmitJobs` SLURM
    limit](https://slurm.schedmd.com/resource_limits.html#assoc).
    """

    FRACTAL_SLURM_CONFIG_FILE: Optional[Path]
    """
    Path of JSON file with configuration for the SLURM backend.
    """

    FRACTAL_RUNNER_DEFAULT_EXECUTOR: str = "cpu-low"
    """
    Used by some runner backends to configure the parameters to run jobs with.
    """

    FRACTAL_SLURM_WORKER_PYTHON: Optional[str] = None
    """
    Path to Python interpreter that will run the jobs on the SLURM nodes. If
    not specified, the same interpreter that runs the server is used.
    """

    FRACTAL_SLURM_POLL_INTERVAL: Optional[int] = 60
    """
    Interval to wait (in seconds) before checking whether unfinished job are
    still running on SLURM (see `SlurmWaitThread` in
    [`clusterfutures`](https://github.com/sampsyo/clusterfutures/blob/master/cfut/__init__.py)).
    """

    FRACTAL_SLURM_KILLWAIT_INTERVAL: Optional[int] = 45
    """
    Interval to wait (in seconds) when the execution of a SLURM-backend job
    failed, before raising a `JobExecutionError`. Must be larger than [SLURM
    `KillWait` timer](https://slurm.schedmd.com/slurm.conf.html#OPT_KillWait),
    to make sure that stdout/stderr files have been written).
    """

    FRACTAL_SLURM_OUTPUT_FILE_GRACE_TIME: Optional[int] = 4
    """
    Interval to wait (in seconds) when the SLURM backend does not find an
    output pickle file, which could be for multiple reasons (the SLURM job was
    cancelled, or writing the file is taking long). After this interval, the
    file is considered as missing.
    """

    # NOTE: we currently set FRACTAL_PARSL_MONITORING to False, due to
    # https://github.com/fractal-analytics-platform/fractal-server/issues/148
    FRACTAL_PARSL_MONITORING: bool = False
    FRACTAL_PARSL_CONFIG: str = "local"

    ###########################################################################
    # BUSINESS LOGIC
    ###########################################################################

    def check(self):
        """
        Make sure that mandatory variables are set

        This method must be called before the server starts
        """

        class StrictSettings(BaseSettings):
            class Config:
                extra = "allow"

            DEPLOYMENT_TYPE: Literal[
                "production", "staging", "testing", "development"
            ]
            JWT_SECRET_KEY: str
            DB_ENGINE: str = Field()

            if DB_ENGINE == "postgres":
                POSTGRES_USER: str
                POSTGRES_PASSWORD: str
                POSTGRES_DB: str
            elif DB_ENGINE == "sqlite":
                SQLITE_PATH: str

            FRACTAL_TASKS_DIR: Path
            FRACTAL_RUNNER_WORKING_BASE_DIR: Path

            FRACTAL_RUNNER_BACKEND: str = Field()
            if FRACTAL_RUNNER_BACKEND == "slurm":
                FRACTAL_SLURM_CONFIG_FILE: Path

        StrictSettings(**self.dict())

        # Check that some variables are allowed
        if isinstance(self.FRACTAL_RUNNER_MAX_TASKS_PER_WORKFLOW, int):
            if self.FRACTAL_RUNNER_MAX_TASKS_PER_WORKFLOW < 1:
                raise FractalConfigurationError(
                    f"{self.FRACTAL_RUNNER_MAX_TASKS_PER_WORKFLOW=} "
                    "not allowed"
                )

        if self.FRACTAL_RUNNER_BACKEND == "slurm":
            info = f"FRACTAL_RUNNER_BACKEND={self.FRACTAL_RUNNER_BACKEND}"

            # Check that FRACTAL_SLURM_CONFIG_FILE exists
            if not self.FRACTAL_SLURM_CONFIG_FILE.exists():
                raise FractalConfigurationError(
                    f"{info} but {str(self.FRACTAL_SLURM_CONFIG_FILE)} "
                    "is missing"
                )
            # Check that sbatch command is available
            if not shutil.which("sbatch"):
                raise FractalConfigurationError(
                    f"{info} but sbatch command not found."
                )
            # Check that squeue command is available
            if not shutil.which("squeue"):
                raise FractalConfigurationError(
                    f"{info} but squeue command not found."
                )


def get_settings(settings=Settings()) -> Settings:
    return settings
