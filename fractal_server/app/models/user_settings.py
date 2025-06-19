from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field
from sqlmodel import SQLModel


class UserSettings(SQLModel, table=True):
    """
    Comprehensive list of user settings.

    Attributes:
        id: ID of database object
        slurm_accounts:
            List of SLURM accounts, to be used upon Fractal job submission.
        ssh_host: SSH-reachable host where a SLURM client is available.
        ssh_username: User on `ssh_host`.
        ssh_private_key_path: Path of private SSH key for `ssh_username`.
        ssh_tasks_dir: Task-venvs base folder on `ssh_host`.
        ssh_jobs_dir: Jobs base folder on `ssh_host`.
        slurm_user: Local user, to be impersonated via `sudo -u`
        project_dir: Folder where `slurm_user` can write.
    """

    __tablename__ = "user_settings"

    id: int | None = Field(default=None, primary_key=True)
    slurm_accounts: list[str] = Field(
        sa_column=Column(JSONB, server_default="[]", nullable=False)
    )
    ssh_host: str | None = None
    ssh_username: str | None = None
    ssh_private_key_path: str | None = None
    ssh_tasks_dir: str | None = None
    ssh_jobs_dir: str | None = None
    slurm_user: str | None = None
    project_dir: str | None = None
