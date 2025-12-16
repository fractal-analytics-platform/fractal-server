from sqlmodel import Field
from sqlmodel import SQLModel


class Profile(SQLModel, table=True):
    """
    Profile table.
    """

    id: int | None = Field(default=None, primary_key=True)

    resource_id: int = Field(foreign_key="resource.id", ondelete="RESTRICT")

    resource_type: str
    """
    Type of resource (either `local`, `slurm_sudo` or `slurm_ssh`).
    """

    name: str = Field(unique=True)
    """
    Profile name.
    """

    username: str | None = None
    """
    Username to be impersonated, either via `sudo -u` or via `ssh`.
    """

    ssh_key_path: str | None = None
    """
    Path to the private SSH key of user `username` (only relevant if
    `resource_type="slurm_ssh"`).
    """

    jobs_remote_dir: str | None = None
    """
    Remote path of the job folder (only relevant if
    `resource_type="slurm_ssh"`).
    """

    tasks_remote_dir: str | None = None
    """
    Remote path of the task folder (only relevant if
    `resource_type="slurm_ssh"`).
    """
