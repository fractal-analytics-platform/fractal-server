from datetime import datetime
from datetime import timezone
from typing import Optional

from sqlalchemy import Column
from sqlalchemy.types import DateTime
from sqlalchemy.types import JSON
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from .task import TaskV2
from fractal_server.utils import get_timestamp


class TaskGroupV2(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    task_list: list[TaskV2] = Relationship(
        sa_relationship_kwargs=dict(
            lazy="selectin", cascade="all, delete-orphan"
        ),
    )

    user_id: int = Field(foreign_key="user_oauth.id")
    user_group_id: Optional[int] = Field(foreign_key="usergroup.id")

    origin: str
    pkg_name: str
    version: Optional[str] = None
    python_version: Optional[str] = None
    path: Optional[str] = None
    wheel_path: Optional[str] = None
    pip_extras: Optional[str] = None
    pinned_package_versions: dict[str, str] = Field(
        sa_column=Column(
            JSON,
            server_default="{}",
            default={},
            nullable=True,
        ),
    )
    pip_freeze: Optional[str] = None
    venv_path: Optional[str] = None
    venv_size_in_kB: Optional[int] = None
    venv_file_number: Optional[int] = None

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

    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user_oauth.id")
    taskgroupv2_id: Optional[int] = Field(foreign_key="taskgroupv2.id")
    timestamp_started: datetime = Field(
        default_factory=get_timestamp,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    pkg_name: str
    version: str
    status: str
    action: str
    log: Optional[str] = None
    timestamp_ended: Optional[datetime] = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True)),
    )
