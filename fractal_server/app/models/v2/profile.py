from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

from fractal_server.app.models.base import Base


class Profile(Base):
    """
    Profile table.
    """

    __tablename__ = "profile"

    id: Mapped[int] = mapped_column(primary_key=True)

    resource_id: Mapped[int] = mapped_column(
        ForeignKey("resource.id", ondelete="RESTRICT")
    )

    resource_type: Mapped[str]
    """
    Type of resource (either `local`, `slurm_sudo` or `slurm_ssh`).
    """

    name: Mapped[str] = mapped_column(unique=True)
    """
    Profile name.
    """

    username: Mapped[str | None] = mapped_column(default=lambda: None)
    """
    Username to be impersonated, either via `sudo -u` or via `ssh`.
    """

    ssh_key_path: Mapped[str | None] = mapped_column(default=lambda: None)
    """
    Path to the private SSH key of user `username` (only relevant if
    `resource_type="slurm_ssh"`).
    """

    jobs_remote_dir: Mapped[str | None] = mapped_column(default=lambda: None)
    """
    Remote path of the job folder (only relevant if
    `resource_type="slurm_ssh"`).
    """

    tasks_remote_dir: Mapped[str | None] = mapped_column(default=lambda: None)
    """
    Remote path of the task folder (only relevant if
    `resource_type="slurm_ssh"`).
    """

    pixi_cache_dir: Mapped[str | None] = mapped_column(default=lambda: None)
    """
    Override for the `PIXI_CACHE_DIR` variable, which would otherwise default
    to the `cache` subfolder of `PIXI_HOME` (only relevant if
    `resource_type="slurm_ssh"`)
    """
