from datetime import datetime
from datetime import timezone

from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import DateTime
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from .task import TaskV2
from fractal_server.utils import get_timestamp


def _check_origin_not_pixi(origin: str):
    """
    Raise `ValueError` if `origin=="pixi"`
    """
    if origin == "pixi":
        raise ValueError(f"Cannot call 'pip_install_string' if {origin=}.")


def _create_dependency_string(pinned_versions: dict[str, str]) -> str:
    """
    Expand e.g. `{"a": "1.2", "b": "3"}` into `"a==1.2 b==3"`.
    """
    output = " ".join(
        [f"{key}=={value}" for key, value in pinned_versions.items()]
    )
    return output


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
    # TODO-2.17.1: make `resource_id` not nullable
    resource_id: int | None = Field(
        foreign_key="resource.id", default=None, ondelete="RESTRICT"
    )

    origin: str
    pkg_name: str
    version: str | None = None
    python_version: str | None = None
    pixi_version: str | None = None
    path: str | None = None
    archive_path: str | None = None
    pip_extras: str | None = None
    pinned_package_versions_pre: dict[str, str] = Field(
        sa_column=Column(
            JSONB,
            server_default="{}",
            default={},
            nullable=True,
        ),
    )
    pinned_package_versions_post: dict[str, str] = Field(
        sa_column=Column(
            JSONB,
            server_default="{}",
            default={},
            nullable=True,
        ),
    )
    env_info: str | None = None
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
        _check_origin_not_pixi(self.origin)

        extras = f"[{self.pip_extras}]" if self.pip_extras is not None else ""

        if self.archive_path is not None:
            return f"{self.archive_path}{extras}"
        else:
            if self.version is None:
                raise ValueError(
                    "Cannot run `pip_install_string` with "
                    f"{self.pkg_name=}, {self.archive_path=}, {self.version=}."
                )
            return f"{self.pkg_name}{extras}=={self.version}"

    @property
    def pinned_package_versions_pre_string(self) -> str:
        """
        Prepare string to be used in `python -m pip install`.
        """
        _check_origin_not_pixi(self.origin)

        if self.pinned_package_versions_pre is None:
            return ""
        output = _create_dependency_string(self.pinned_package_versions_pre)
        return output

    @property
    def pinned_package_versions_post_string(self) -> str:
        """
        Prepare string to be used in `python -m pip install`.
        """
        _check_origin_not_pixi(self.origin)

        if self.pinned_package_versions_post is None:
            return ""
        output = _create_dependency_string(self.pinned_package_versions_post)
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
