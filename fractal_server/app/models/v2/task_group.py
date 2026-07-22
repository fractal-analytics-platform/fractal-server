from datetime import datetime
from datetime import timezone

from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.types import DateTime
from sqlalchemy.types import String

from fractal_server.app.models.base import Base
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityAction
from fractal_server.utils import get_timestamp

from .task import TaskV2


def _check_origin_not_pixi(origin: str) -> None:
    """
    Raise `ValueError` if `origin=="pixi"`
    """
    if origin == "pixi":
        raise ValueError(f"Cannot call 'pip_install_string' if {origin=}.")


def _create_dependency_string(pinned_versions: dict[str, str]) -> str:
    """
    Expand e.g. `{"a": "1.2", "b": "3"}` into `'"a==1.2" "b==3"'`.
    """
    output = " ".join(
        [f'"{key}=={value}"' for key, value in pinned_versions.items()]
    )
    return output


class TaskGroupV2(Base):
    __tablename__ = "taskgroupv2"

    id: Mapped[int] = mapped_column(primary_key=True)
    task_list: Mapped[list["TaskV2"]] = relationship(
        lazy="selectin", cascade="all, delete-orphan"
    )

    user_id: Mapped[int] = mapped_column(ForeignKey("user_oauth.id"))
    user_group_id: Mapped[int | None] = mapped_column(
        ForeignKey("usergroup.id", ondelete="SET NULL"), default=lambda: None
    )
    resource_id: Mapped[int] = mapped_column(
        ForeignKey("resource.id", ondelete="RESTRICT")
    )

    origin: Mapped[str] = mapped_column()
    pkg_name: Mapped[str] = mapped_column()
    version: Mapped[str] = mapped_column()
    python_version: Mapped[str | None] = mapped_column(default=lambda: None)
    pixi_version: Mapped[str | None] = mapped_column(default=lambda: None)
    path: Mapped[str | None] = mapped_column(default=lambda: None)
    archive_path: Mapped[str | None] = mapped_column(default=lambda: None)
    pip_extras: Mapped[str | None] = mapped_column(default=lambda: None)
    pinned_package_versions_pre: Mapped[dict[str, str]] = mapped_column(
        JSONB,
        server_default="{}",
        default={},
        nullable=True,
    )
    pinned_package_versions_post: Mapped[dict[str, str]] = mapped_column(
        JSONB,
        server_default="{}",
        default={},
        nullable=True,
    )
    env_info: Mapped[str | None] = mapped_column(default=lambda: None)
    venv_path: Mapped[str | None] = mapped_column(default=lambda: None)

    active: Mapped[bool] = mapped_column(default=True)
    timestamp_created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=get_timestamp
    )
    timestamp_last_used: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=(
            datetime(2024, 11, 20, tzinfo=timezone.utc).isoformat()
        ),
        default=get_timestamp,
    )

    __table_args__ = (
        Index(
            "ix_taskgroupv2_path_unique_constraint",
            "path",
            "resource_id",
            unique=True,
        ),
    )

    @property
    def pip_install_string(self) -> str:
        """
        Prepare string to be used in `python -m pip install`.
        """
        _check_origin_not_pixi(self.origin)

        extras = (
            f"[{self.pip_extras}]"
            if (self.pip_extras is not None and self.pip_extras != "")
            else ""
        )

        if self.archive_path is not None:
            return f"{self.archive_path}{extras}"
        else:
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


class TaskGroupActivityV2(Base):
    __tablename__ = "taskgroupactivityv2"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("user_oauth.id"))
    taskgroupv2_id: Mapped[int | None] = mapped_column(
        ForeignKey("taskgroupv2.id", ondelete="SET NULL"), default=lambda: None
    )
    timestamp_started: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=get_timestamp
    )
    pkg_name: Mapped[str] = mapped_column()
    version: Mapped[str] = mapped_column()
    status: Mapped[str] = mapped_column()
    action: Mapped[str] = mapped_column()
    log: Mapped[str | None] = mapped_column(default=lambda: None)
    timestamp_ended: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=lambda: None
    )
    fractal_server_version: Mapped[str] = mapped_column(
        String, server_default="pre-2.19.0", nullable=False
    )

    __table_args__ = (
        Index(
            "ix_taskgroupactivityv2_collect_unique_constraint",
            "taskgroupv2_id",
            unique=True,
            postgresql_where=text(
                f"action = '{TaskGroupActivityAction.COLLECT}'"
            ),
        ),
    )
