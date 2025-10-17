from enum import StrEnum
from typing import Annotated
from typing import Any
from typing import Literal
from typing import Self

from pydantic import AfterValidator
from pydantic import BaseModel
from pydantic import Discriminator
from pydantic import model_validator
from pydantic import Tag
from pydantic import validate_call
from pydantic.types import AwareDatetime

from fractal_server.runner.config import JobRunnerConfigLocal
from fractal_server.runner.config import JobRunnerConfigSLURM
from fractal_server.tasks.config import TasksPixiSettings
from fractal_server.tasks.config import TasksPythonSettings
from fractal_server.types import AbsolutePathStr
from fractal_server.types import NonEmptyStr


class ResourceType(StrEnum):
    SLURM_SUDO = "slurm_sudo"
    SLURM_SSH = "slurm_ssh"
    LOCAL = "local"


def cast_serialize_pixi_settings(
    v: dict[NonEmptyStr, Any],
) -> dict[NonEmptyStr, Any]:
    """
    Validate current value, and enrich it with default values.
    """
    if v != {}:
        v = TasksPixiSettings(**v).model_dump()
    return v


class _ValidResourceBase(BaseModel):
    type: ResourceType
    name: NonEmptyStr

    # Tasks
    tasks_python_config: TasksPythonSettings
    tasks_pixi_config: Annotated[
        dict[NonEmptyStr, Any],
        AfterValidator(cast_serialize_pixi_settings),
    ]
    tasks_local_dir: AbsolutePathStr

    # Jobs
    jobs_local_dir: AbsolutePathStr
    jobs_runner_config: dict[NonEmptyStr, Any]
    jobs_poll_interval: int = 5

    @model_validator(mode="after")
    def _pixi_slurm_config(self) -> Self:
        if (
            self.tasks_pixi_config != {}
            and self.type == ResourceType.SLURM_SSH
            and self.tasks_pixi_config["SLURM_CONFIG"] is None
        ):
            raise ValueError(
                "`tasks_pixi_config` must include `SLURM_CONFIG`."
            )
        return self


class ValidResourceLocal(_ValidResourceBase):
    type: Literal[ResourceType.LOCAL]
    jobs_runner_config: JobRunnerConfigLocal
    jobs_slurm_python_worker: None = None
    host: None = None


class ValidResourceSlurmSudo(_ValidResourceBase):
    type: Literal[ResourceType.SLURM_SUDO]
    jobs_slurm_python_worker: AbsolutePathStr
    jobs_runner_config: JobRunnerConfigSLURM
    host: None = None


class ValidResourceSlurmSSH(_ValidResourceBase):
    type: Literal[ResourceType.SLURM_SSH]
    host: NonEmptyStr
    jobs_slurm_python_worker: AbsolutePathStr
    jobs_runner_config: JobRunnerConfigSLURM


def get_discriminator_value(v: Any) -> str:
    # See https://github.com/fastapi/fastapi/discussions/12941
    if isinstance(v, dict):
        return v.get("type", None)
    return getattr(v, "type", None)


ResourceCreate = Annotated[
    Annotated[ValidResourceLocal, Tag(ResourceType.LOCAL)]
    | Annotated[ValidResourceSlurmSudo, Tag(ResourceType.SLURM_SUDO)]
    | Annotated[ValidResourceSlurmSSH, Tag(ResourceType.SLURM_SSH)],
    Discriminator(get_discriminator_value),
]


class ResourceRead(BaseModel):
    id: int

    type: str

    name: str
    timestamp_created: AwareDatetime

    host: str | None

    jobs_local_dir: str
    jobs_runner_config: dict[str, Any]
    jobs_slurm_python_worker: str | None
    jobs_poll_interval: int

    tasks_local_dir: str
    tasks_python_config: dict[str, Any]
    tasks_pixi_config: dict[str, Any]


@validate_call
def cast_serialize_resource(_data: ResourceCreate) -> dict[str, Any]:
    """
    Cast/serialize round-trip for `Resource` data.

    We use `@validate_call` because `ResourceCreate` is a `Union` type and it
    cannot be instantiated directly.

    Return:
        Serialized version of a valid resource object.
    """
    return _data.model_dump()
