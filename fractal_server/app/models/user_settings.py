from typing import Optional

from sqlalchemy import Column
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import SQLModel


class UserSettings(SQLModel, table=True):
    __tablename__ = "user_settings"

    id: Optional[int] = Field(default=None, primary_key=True)

    # SSH-SLURM
    ssh_host: Optional[str] = None
    ssh_username: Optional[str] = None
    ssh_private_key_path: Optional[str] = None
    ssh_tasks_dir: Optional[str] = None
    ssh_jobs_dir: Optional[str] = None

    # SUDO-SLURM
    slurm_user: Optional[str] = None
    slurm_accounts: list[str] = Field(
        sa_column=Column(JSON, server_default="[]", nullable=False)
    )
    cache_dir: Optional[str] = None
