from enum import StrEnum
from typing import Any
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import model_validator
from pydantic import ValidationError

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


class ValidResourceSlurmSudo(_ValidResourceBase):
    type: Literal[ResourceType.SLURM_SUDO.value]
    jobs_slurm_python_worker: AbsolutePathStr
    jobs_runner_config: JobRunnerConfigSLURM


class ValidResourceSlurmSSH(_ValidResourceBase):
    type: Literal[ResourceType.SLURM_SSH.value]
    host: NonEmptyStr
    jobs_slurm_python_worker: AbsolutePathStr
    jobs_runner_config: JobRunnerConfigSLURM


class ResourceCreate(BaseModel):
    pass


class ResourceUpdate(BaseModel):
    pass


class ResourceRead(BaseModel):
    pass
