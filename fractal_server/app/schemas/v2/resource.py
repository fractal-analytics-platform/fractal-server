from enum import StrEnum
from typing import Annotated
from typing import Any
from typing import Literal
from typing import Self

from pydantic import AfterValidator
from pydantic import BaseModel
from pydantic import Discriminator
from pydantic import Tag
from pydantic import model_validator
from pydantic import validate_call
from pydantic.types import AwareDatetime

from fractal_server.runner.config import JobRunnerConfigLocal
from fractal_server.runner.config import JobRunnerConfigSLURM
from fractal_server.runner.executors.slurm_rest.client import (
    SUPPORTED_API_VERSIONS,
)
from fractal_server.tasks.config import TasksPixiSettings
from fractal_server.tasks.config import TasksPythonSettings
from fractal_server.types import AbsolutePathStr
from fractal_server.types import HttpUrlStr
from fractal_server.types import NonEmptyStr


class ResourceType(StrEnum):
    """
    Enum for the possible resource types.
    """

    SLURM_SUDO = "slurm_sudo"
    """
    Enum entry for resource type `slurm_sudo`.
    """

    SLURM_SSH = "slurm_ssh"
    """
    Enum entry for resource type `slurm_ssh`.
    """

    SLURM_REST = "slurm_rest"
    """
    Enum entry for resource type `slurm_rest`.
    """

    LOCAL = "local"
    """
    Enum entry for resource type `local`.
    """


def is_ssh_resource_type(resource_type: str) -> bool:
    """
    Whether the cluster of a resource type is (also) accessed over SSH.

    Arguments:
        resource_type: Resource type.
    """
    return resource_type in (ResourceType.SLURM_SSH, ResourceType.SLURM_REST)


def val_rest_url(value: str) -> str:
    # Tokens are sent in request headers
    if not value.startswith("https://"):
        raise ValueError(f"Invalid `rest_url` (https only): '{value}'.")
    return value.rstrip("/")


def val_rest_api_version(value: str) -> str:
    if value not in SUPPORTED_API_VERSIONS:
        raise ValueError(
            f"Unsupported `rest_api_version` '{value}' "
            f"(supported: {list(SUPPORTED_API_VERSIONS)})."
        )
    return value


def cast_serialize_pixi_settings(
    value: dict[NonEmptyStr, Any],
) -> dict[NonEmptyStr, Any]:
    """
    Cast/serialize round trip for `tasks_pixi_config` through the
    `TasksPixiSettings` schema.

    Arguments:
        value: Current `tasks_pixi_config` value.
    """
    if value != {}:
        value = TasksPixiSettings(**value).model_dump()
    return value


class ValidResourceBase(BaseModel):
    """
    Base resource schema.
    """

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

    prevent_new_submissions: bool = False

    @model_validator(mode="after")
    def _pixi_slurm_config(self) -> Self:
        if (
            self.tasks_pixi_config != {}
            and is_ssh_resource_type(self.type)
            and self.tasks_pixi_config["SLURM_CONFIG"] is None
        ):
            raise ValueError("`tasks_pixi_config` must include `SLURM_CONFIG`.")
        return self


class ValidResourceLocal(ValidResourceBase):
    """
    Valid local resource.

    Attributes:
        name: Resource name.
        type: Resource type.
        prevent_new_submissions:
            When set to true: Prevent new job submissions and stop execution of
            ongoing jobs as soon as the current task is complete.
        tasks_python_config:
            Configuration of Python interpreters used for task collection.
        tasks_pixi_config:
            Configuration of `pixi` interpreters used for task collection.
        tasks_local_dir:
            Local base folder for task environments.
        jobs_local_dir:
            Local base folder for job folders.
        jobs_runner_config:
            Runner configuration.
    """

    type: Literal[ResourceType.LOCAL]
    jobs_runner_config: JobRunnerConfigLocal
    jobs_slurm_python_worker: None = None
    host: None = None
    rest_url: None = None
    rest_ca_bundle: None = None
    rest_api_version: None = None


class ValidResourceSlurmSudo(ValidResourceBase):
    """
    Valid SLURM-sudo resource.

    Attributes:
        name: Resource name.
        type: Resource type.
        prevent_new_submissions:
            When set to true: Prevent new job submissions and stop execution of
            ongoing jobs as soon as the current task is complete.
        tasks_python_config:
            Configuration of Python interpreters used for task collection.
        tasks_pixi_config:
            Configuration of `pixi` interpreters used for task collection.
        tasks_local_dir:
            Local base folder for task environments.
        jobs_local_dir:
            Local base folder for job folders.
        jobs_runner_config:
            Runner configuration.
        jobs_poll_interval:
            `squeue` polling interval.
        jobs_slurm_python_worker:
            Python worker to be used in SLURM jobs.
    """

    type: Literal[ResourceType.SLURM_SUDO]
    jobs_slurm_python_worker: AbsolutePathStr
    jobs_runner_config: JobRunnerConfigSLURM
    host: None = None
    rest_url: None = None
    rest_ca_bundle: None = None
    rest_api_version: None = None


class ValidResourceSlurmSSH(ValidResourceBase):
    """
    Valid SLURM-SSH resource.

    Attributes:
        name: Resource name
        type: Resource type.
        prevent_new_submissions:
            When set to true: Prevent new job submissions and stop execution of
            ongoing jobs as soon as the current task is complete.
        tasks_python_config:
            Configuration of Python interpreters used for task collection.
        tasks_pixi_config:
            Configuration of `pixi` interpreters used for task collection.
        tasks_local_dir:
            Local base folder for task environments.
        jobs_local_dir:
            Local base folder for job folders.
        jobs_runner_config:
            Runner configuration.
        jobs_poll_interval:
            `squeue` polling interval.
        jobs_slurm_python_worker:
            Python worker to be used in SLURM jobs.
        host:
            Hostname or IP address of remote SLURM cluster.
    """

    type: Literal[ResourceType.SLURM_SSH]
    host: NonEmptyStr
    jobs_slurm_python_worker: AbsolutePathStr
    jobs_runner_config: JobRunnerConfigSLURM
    rest_url: None = None
    rest_ca_bundle: None = None
    rest_api_version: None = None


class ValidResourceSlurmRest(ValidResourceBase):
    """
    Valid SLURM-REST resource.

    SLURM jobs are submitted, polled and cancelled through `slurmrestd`,
    while everything else goes over SSH, as for `slurm_ssh` resources.

    Attributes:
        name: Resource name
        type: Resource type.
        prevent_new_submissions:
            When set to true: Prevent new job submissions and stop execution of
            ongoing jobs as soon as the current task is complete.
        tasks_python_config:
            Configuration of Python interpreters used for task collection.
        tasks_pixi_config:
            Configuration of `pixi` interpreters used for task collection.
        tasks_local_dir:
            Local base folder for task environments.
        jobs_local_dir:
            Local base folder for job folders.
        jobs_runner_config:
            Runner configuration.
        jobs_poll_interval:
            Polling interval of SLURM job states.
        jobs_slurm_python_worker:
            Python worker to be used in SLURM jobs.
        host:
            Hostname or IP address of remote SLURM cluster (for SSH).
        rest_url:
            HTTPS base URL of `slurmrestd` (without `/slurm/<version>`).
        rest_ca_bundle:
            Optional CA file (PEM) on the `fractal-server` machine, to verify
            the TLS certificate of `slurmrestd` (e.g. for an internal CA).
            If unset, the system CAs are used.
        rest_api_version:
            Optional `slurmrestd` OpenAPI version (e.g. `v0.0.44`).
    """

    type: Literal[ResourceType.SLURM_REST]
    host: NonEmptyStr
    jobs_slurm_python_worker: AbsolutePathStr
    jobs_runner_config: JobRunnerConfigSLURM
    rest_url: Annotated[HttpUrlStr, AfterValidator(val_rest_url)]
    rest_ca_bundle: AbsolutePathStr | None = None
    rest_api_version: (
        Annotated[NonEmptyStr, AfterValidator(val_rest_api_version)] | None
    ) = None


def get_discriminator_value(v: Any) -> str:
    # See https://github.com/fastapi/fastapi/discussions/12941
    if isinstance(v, dict):
        return v.get("type", None)
    return getattr(v, "type", None)


ResourceCreate = Annotated[
    Annotated[ValidResourceLocal, Tag(ResourceType.LOCAL)]
    | Annotated[ValidResourceSlurmSudo, Tag(ResourceType.SLURM_SUDO)]
    | Annotated[ValidResourceSlurmSSH, Tag(ResourceType.SLURM_SSH)]
    | Annotated[ValidResourceSlurmRest, Tag(ResourceType.SLURM_REST)],
    Discriminator(get_discriminator_value),
]
"""
Schema for resources in API request bodies.
"""


class ResourceRead(BaseModel):
    """
    Schema for resources in API response bodies.
    """

    id: int
    name: str
    type: str
    prevent_new_submissions: bool
    timestamp_created: AwareDatetime

    host: str | None

    rest_url: str | None = None
    rest_ca_bundle: str | None = None
    rest_api_version: str | None = None

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

    Args:
        _data:

    Return:
        Serialized version of a valid resource object.
    """
    return _data.model_dump()
