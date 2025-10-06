from datetime import datetime

from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field
from sqlmodel import SQLModel


class Resource(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)

    resource_type: str  # + CHECK (slurm_sudo, slurm_ssh, local)
    name: str
    name_readable: str

    creation_timestamp: datetime
    last_edit_timestamp: datetime
    host: str | None

    # runner_settings
    local_job_folder: str
    remote_job_folder: str | None
    slurm_config: dict[str, str] = Field(
        sa_column=Column(JSONB, nullable=False, server_default="{}")
    )
    python_worker: str

    # task_settings
    bas_remote_folder: str
    default_python_version: str
    base_python_3_9: str
    base_python_3_10: str
    base_python_3_11: str
    base_python_3_12: str
    base_python_3_13: str
    pixi_confg: dict[str, str] = Field(
        sa_column=Column(JSONB, nullable=False, server_default="{}")
    )
