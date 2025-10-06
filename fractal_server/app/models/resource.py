from datetime import datetime

from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import SQLModel

from fractal_server.utils import get_timestamp


class Resource(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)

    resource_type: str  # + CHECK (slurm_sudo, slurm_ssh, local)
    # FIXME resource_type must have a single value for all rows
    """
    FRACTAL_RUNNER_BACKEND
    """
    name: str
    name_readable: str

    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    host: str | None = None

    # runner_settings

    job_local_folder: str
    """
    # FRACTAL_RUNNER_WORKING_BASE_DIR
    # """

    job_remote_folder: str | None = None
    job_runner_config: dict[str, str] = Field(
        sa_column=Column(JSONB, nullable=False, server_default="{}")
    )
    """
    FRACTAL_LOCAL_CONFIG_FILE and FRACTAL_SLURM_CONFIG_FILE content
    """
    job_slurm_python_worker: str | None = None
    """
    FRACTAL_SLURM_WORKER_PYTHON
    check not (resource_type is local and job_slurm_python_worker is None)
    """

    # task_settings
    tasks_local_folder: str
    """
    FRACTAL_TASKS_DIR
    """

    tasks_python_default_version: str
    tasks_python_3_9: str | None = None
    tasks_python_3_10: str | None = None
    tasks_python_3_11: str | None = None
    tasks_python_3_12: str | None = None
    tasks_python_3_13: str | None = None

    tasks_pixi_config: dict[str, str] = Field(
        sa_column=Column(JSONB, nullable=False, server_default="{}")
    )

    tasks_pip_cache_dir: str | None = None
    """
    FRACTAL_PIP_CACHE_DIR + PIP_CACHE_DIR_ARG
    """
