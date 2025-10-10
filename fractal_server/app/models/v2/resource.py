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
    """
    One of `local`, `slurm_sudo` or `slurm_ssh` - matching with
    `settings.FRACTAL_RUNNER_BACKEND`.
    """

    name: str
    """
    Resource name.
    """

    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    """
    Creation timestamp.
    """

    host: str | None = None
    """
    Address for ssh connections, when `type="slurm_ssh"`.
    """

    jobs_local_dir: str
    """
    Base local folder for job subfolders (containing artifacts and logs).
    """

    jobs_runner_config: dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False, server_default="{}")
    )
    """
    FIXME: point to appropriate schemas
    """

    jobs_slurm_python_worker: str | None = None
    """
    On SLURM deloyments, this is the Python interpreter that runs the
    `fractal-server` worker from within the SLURM jobs.
    """

    jobs_poll_interval: int = 5
    """
    On SLURM deployments, the interval to wait before new `squeue` calls.
    """

    # task_settings
    tasks_local_dir: str
    """
    Base local folder for task-package subfolders.
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
    """
    FIXME: describe
    """

    tasks_pip_cache_dir: str | None = None
    """
    FIXME: describe
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
