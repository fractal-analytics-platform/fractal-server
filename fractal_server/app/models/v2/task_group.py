from datetime import datetime
from datetime import timezone

from sqlalchemy import Column
from sqlalchemy.types import DateTime
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from .task import TaskV2
from fractal_server.utils import get_timestamp


class TaskGroupV2(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    task_list: list[TaskV2] = Relationship(
        sa_relationship_kwargs=dict(
            lazy="selectin", cascade="all, delete-orphan"
        ),
    )

    user_id: int = Field(foreign_key="user_oauth.id")
    user_group_id: int | None = Field(
        foreign_key="usergroup.id", default=None, ondelete="SET NULL"
    )

    origin: str
    pkg_name: str
    version: str | None = None
    python_version: str | None = None
    path: str | None = None
    wheel_path: str | None = None
    pip_extras: str | None = None
    pinned_package_versions: dict[str, str] = Field(
        sa_column=Column(
            JSON,
            server_default="{}",
            default={},
            nullable=True,
        ),
    )
    pip_freeze: str | None = None
    venv_path: str | None = None
    venv_size_in_kB: int | None = None
    venv_file_number: int | None = None

    active: bool = True
    timestamp_created: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    timestamp_last_used: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(
            DateTime(timezone=True),
            nullable=False,
            server_default=(
                datetime(2024, 11, 20, tzinfo=timezone.utc).isoformat()
            ),
        ),
    )

    @property
    def pip_install_string(self) -> str:
        """
        Prepare string to be used in `python -m pip install`.
        """
        extras = f"[{self.pip_extras}]" if self.pip_extras is not None else ""

        if self.wheel_path is not None:
            return f"{self.wheel_path}{extras}"
        else:
            if self.version is None:
                raise ValueError(
                    "Cannot run `pip_install_string` with "
                    f"{self.pkg_name=}, {self.wheel_path=}, {self.version=}."
                )
            return f"{self.pkg_name}{extras}=={self.version}"

    @property
    def pinned_package_versions_string(self) -> str:
        """
        Prepare string to be used in `python -m pip install`.
        """
        if self.pinned_package_versions is None:
            return ""
        output = " ".join(
            [
                f"{key}=={value}"
                for key, value in self.pinned_package_versions.items()
            ]
        )
        return output


class TaskGroupActivityV2(SQLModel, table=True):

    id: int | None = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user_oauth.id")
    taskgroupv2_id: int | None = Field(
        default=None, foreign_key="taskgroupv2.id", ondelete="SET NULL"
    )
    timestamp_started: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    pkg_name: str
    version: str
    status: str
    action: str
    log: str | None = None
    timestamp_ended: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True)),
    )
