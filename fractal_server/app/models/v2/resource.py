from datetime import datetime
from typing import Any

from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import SQLModel

from fractal_server.utils import get_timestamp


class Resource(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)

    type: str
    # FIXME: db check: resource.type must be one of (slurm_sudo, slurm_ssh, local)
    # FIXME: runtime check: resource.type must be identical to settings.FRACTAL_RUNNER_BACKEND
    # FIXME: runtime check: resource.type must have a single value for all rows (obsolete, due to previous check)
    """
    FRACTAL_RUNNER_BACKEND
    """
    name: str
    # FIXME: add unique index

    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    host: str | None = None

    # runner_settings

    job_local_folder: str
    """
    # FRACTAL_RUNNER_WORKING_BASE_DIR_zzz
    # """

    job_remote_folder: str | None = None
    job_runner_config: dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False, server_default="{}")
    )
    """
    FRACTAL_LOCAL_CONFIG_FILE_zzz and FRACTAL_SLURM_CONFIG_FILE_zzz content
    """
    job_slurm_python_worker: str | None = None
    """
    FRACTAL_SLURM_WORKER_PYTHON_zzz
    check not (type is local and job_slurm_python_worker is None)
    """

    job_poll_interval: int = 5
    """
    FRACTAL_SLURM_POLL_INTERVAL_zzz
    """

    # task_settings
    tasks_local_folder: str
    """
    FRACTAL_TASKS_DIR_zzz
    """

    tasks_python_config: dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False, server_default="{}")
    )
    """
    Example:
    {
      "default_version": "3.10",
      "versions:{
        "3.10": "/somewhere/venv-3.10/bin/python",
        "3.11": "/somewhere/venv-3.11/bin/python",
        "3.12": "/somewhere/venv-3.12/bin/python",
       }
    }
    """

    tasks_pixi_config: dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False, server_default="{}")
    )

    tasks_pip_cache_dir: str | None = None
    """
    FRACTAL_PIP_CACHE_DIR_zzz + PIP_CACHE_DIR_ARG
    """

    @property
    def pip_cache_dir_arg(self) -> str:
        """
        Option for `pip install`, based on `tasks_pip_cache_dir` value.
        If `tasks_pip_cache_dir` is set, then return
        `--cache-dir /somewhere`; else return `--no-cache-dir`.
        """
        if self.tasks_pip_cache_dir is not None:
            return f"--cache-dir {self.tasks_pip_cache_dir}"
        else:
            return "--no-cache-dir"
