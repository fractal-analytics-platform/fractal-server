from enum import StrEnum
from typing import Any
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import model_validator
from pydantic import ValidationError
from pydantic.types import AwareDatetime

from fractal_server.runner.config import JobRunnerConfigLocal
from fractal_server.runner.config import JobRunnerConfigSLURM
from fractal_server.tasks.config import TaskPythonSettings
from fractal_server.tasks.config import TasksPixiSettings
from fractal_server.types import AbsolutePathStr
from fractal_server.types import NonEmptyStr


class ResourceType(StrEnum):
    SLURM_SUDO = "slurm_sudo"
    SLURM_SSH = "slurm_ssh"
    LOCAL = "local"


def validate_resource(resource_data: dict[str, Any]) -> None:
    try:
        resource_type = resource_data["type"]
    except KeyError:
        raise ValueError("Missing `type` key.")
    match resource_type:
        case ResourceType.LOCAL:
            ValidResourceLocal(**resource_data)
        case ResourceType.SLURM_SUDO:
            ValidResourceSlurmSudo(**resource_data)
        case ResourceType.SLURM_SSH:
            ValidResourceSlurmSSH(**resource_data)


class _ValidResourceBase(BaseModel):
    type: ResourceType

    # Tasks
    tasks_python_config: dict[NonEmptyStr, Any]
    tasks_pixi_config: dict[NonEmptyStr, Any]
    tasks_local_dir: AbsolutePathStr
    tasks_pip_cache_dir: AbsolutePathStr | None

    # Jobs
    jobs_local_dir: AbsolutePathStr
    jobs_runner_config: dict[NonEmptyStr, Any]

    @model_validator(mode="after")
    def _tasks_configurations(self) -> Self:
        if self.tasks_python_config != {}:
            TaskPythonSettings(**self.tasks_python_config)
        if self.tasks_pixi_config != {}:
            pixi_settings = TasksPixiSettings(**self.tasks_pixi_config)
            if (
                self.type == ResourceType.SLURM_SSH
                and pixi_settings.SLURM_CONFIG is None
            ):
                raise ValidationError(
                    "`tasks_pixi_config` must include `SLURM_CONFIG`."
                )
        return self


class ValidResourceLocal(_ValidResourceBase):
    jobs_runner_config: JobRunnerConfigLocal
    jobs_slurm_python_worker: None = None


class ValidResourceSlurmSudo(_ValidResourceBase):
    type: Literal[ResourceType.SLURM_SUDO]
    jobs_slurm_python_worker: AbsolutePathStr
    jobs_runner_config: JobRunnerConfigSLURM
    jobs_poll_interval: int


class ValidResourceSlurmSSH(_ValidResourceBase):
    type: Literal[ResourceType.SLURM_SSH]
    host: NonEmptyStr
    jobs_slurm_python_worker: AbsolutePathStr
    jobs_runner_config: JobRunnerConfigSLURM
    jobs_poll_interval: int


class ResourceCreate(BaseModel):
    type: ResourceType

    name: NonEmptyStr
    host: NonEmptyStr | None = None

    jobs_local_dir: NonEmptyStr
    jobs_runner_config: dict[str, Any]
    jobs_slurm_python_worker: AbsolutePathStr | None = None
    jobs_poll_interval: int | None = None

    tasks_local_dir: AbsolutePathStr
    tasks_python_config: dict[str, Any]
    tasks_pixi_config: dict[str, Any]
    tasks_pip_cache_dir: AbsolutePathStr | None = None

    @model_validator(mode="after")
    def _validate_resource(self):
        data = self.model_dump()
        match self.type:
            case ResourceType.LOCAL:
                ValidResourceLocal(**data)
            case ResourceType.SLURM_SUDO:
                ValidResourceSlurmSudo(**data)
            case ResourceType.SLURM_SSH:
                ValidResourceSlurmSSH(**data)

        return self


class ResourceUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    # Non-nullable db columns
    name: NonEmptyStr = None
    jobs_local_dir: NonEmptyStr = None
    tasks_local_dir: AbsolutePathStr = None
    tasks_python_config: dict[str, Any] = None
    tasks_pixi_config: dict[str, Any] = None
    # Nullable db columns
    host: NonEmptyStr | None = None
    jobs_slurm_python_worker: AbsolutePathStr | None = None
    jobs_poll_interval: int | None = None
    tasks_pip_cache_dir: AbsolutePathStr | None = None


class ResourceRead(BaseModel):
    id: int

    type: ResourceType

    name: str
    timestamp_created: AwareDatetime

    host: str | None = None

    jobs_local_dir: str
    jobs_runner_config: dict[str, Any]
    jobs_slurm_python_worker: str | None = None
    jobs_poll_interval: int

    tasks_local_dir: str
    tasks_python_config: dict[str, Any]
    tasks_pixi_config: dict[str, Any]
    tasks_pip_cache_dir: str | None = None
