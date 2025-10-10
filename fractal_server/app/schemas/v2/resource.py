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


class _ValidResourceBase(BaseModel):
    type: Literal["slurm_sudo", "slurm_ssh", "local"]

    # Tasks
    tasks_python_config: dict[NonEmptyStr, Any]
    tasks_pixi_config: dict[NonEmptyStr, Any]
    tasks_local_folder: AbsolutePathStr
    tasks_pip_cache_dir: AbsolutePathStr | None

    # Jobs
    job_local_folder: AbsolutePathStr
    job_runner_config: dict[NonEmptyStr, Any]

    @model_validator(mode="after")
    def _tasks_configurations(self) -> Self:
        if self.tasks_python_config != {}:
            TaskPythonSettings(**self.tasks_python_config)
        if self.tasks_pixi_config != {}:
            pixi_settings = TasksPixiSettings(**self.tasks_pixi_config)
            if self.type == "slurm_ssh" and pixi_settings.SLURM_CONFIG is None:
                raise ValidationError(
                    "`tasks_pixi_config` must include `SLURM_CONFIG`."
                )
        return self


class ValidResourceLocal(_ValidResourceBase):
    job_runner_config: JobRunnerConfigLocal


class ValidResourceSlurmSudo(_ValidResourceBase):
    type: Literal["slurm_sudo"]
    job_slurm_python_worker: AbsolutePathStr
    job_runner_config: JobRunnerConfigSLURM


class ValidResourceSlurmSSH(_ValidResourceBase):
    type: Literal["slurm_ssh"]
    host: NonEmptyStr
    job_slurm_python_worker: AbsolutePathStr
    job_runner_config: JobRunnerConfigSLURM


class ResourceCreate(BaseModel):
    pass


class ResourceUpdate(BaseModel):
    pass


class ResourceRead(BaseModel):
    pass
