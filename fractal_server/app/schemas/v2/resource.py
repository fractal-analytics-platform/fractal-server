from pathlib import Path
from typing import Annotated
from typing import Any
from typing import Literal
from typing import Self

from pydantic import AfterValidator
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import model_validator
from pydantic import PositiveInt

from fractal_server.types import AbsolutePathStr
from fractal_server.types import DictStrStr
from fractal_server.types import NonEmptyStr


class TaskPythonSettings(BaseModel):
    """
    FIXME
    """

    versions: dict[
        Literal[
            "3.9",
            "3.10",
            "3.11",
            "3.12",
            "3.13",
            "3.14",
        ],
        AbsolutePathStr,
    ]
    """
    Dictionary mapping Python versions to the corresponding interpreters.
    """
    default_version: NonEmptyStr
    """
    Default task-collection Python version (must be a key of `self.versions`).
    """

    @model_validator(mode="after")
    def check_python_version(self) -> Self:
        if self.default_version not in self.versions:
            raise ValueError(
                f"Default Python version '{self.default_version}' not in "
                f"available version {list(self.versions.keys())}."
            )

        return self


def _check_pixi_slurm_memory(mem: str) -> str:
    if mem[-1] not in ["K", "M", "G", "T"]:
        raise ValueError(
            f"Invalid memory requirement {mem=} for `pixi`, "
            "please set a K/M/G/T units suffix."
        )
    return mem


class PixiSLURMConfig(BaseModel):
    """
    Parameters that are passed directly to a `sbatch` command.

    See https://slurm.schedmd.com/sbatch.html.
    """

    partition: NonEmptyStr
    """
    `-p, --partition=<partition_names>`
    """
    cpus: PositiveInt
    """
    `-c, --cpus-per-task=<ncpus>
    """
    mem: Annotated[NonEmptyStr, AfterValidator(_check_pixi_slurm_memory)]
    """
    `--mem=<size>[units]` (examples: `"10M"`, `"10G"`).
    From `sbatch` docs: Specify the real memory required per node. Default
    units are megabytes. Different units can be specified using the suffix
    [K|M|G|T].
    """
    time: NonEmptyStr
    """
    `-t, --time=<time>`.
    From `sbatch` docs: "A time limit of zero requests that no time limit be
    imposed. Acceptable time formats include "minutes", "minutes:seconds",
    "hours:minutes:seconds", "days-hours", "days-hours:minutes" and
    "days-hours:minutes:seconds".
    """


class PixiSettings(BaseModel):
    """
    Configuration for Pixi Task collection.

    In order to use Pixi for Task collection, you must have one or more Pixi
    binaries in your machine
    (see
    [example/get_pixi.sh](https://github.com/fractal-analytics-platform/fractal-server/blob/main/example/get_pixi.sh)
    for installation example).

    To let Fractal Server use these binaries for Task collection, a JSON file
    must be prepared with the data to populate `PixiSettings` (arguments with
    default values may be omitted).

    The path to this JSON file must then be provided to Fractal via the
    environment variable `FRACTAL_PIXI_CONFIG_FILE_zzz`.
    """

    versions: DictStrStr
    """
    A dictionary with Pixi versions as keys and paths to the corresponding
    folder as values.

    E.g. let's assume that you have Pixi v0.47.0 at
    `/pixi-path/0.47.0/bin/pixi` and Pixi v0.48.2 at
    `/pixi-path/0.48.2/bin/pixi`, then
    ```json
    "versions": {
        "0.47.0": "/pixi-path/0.47.0",
        "0.48.2": "/pixi-path/0.48.2"
    }
    ```
    """
    default_version: str
    """
    Default Pixi version to be used for Task collection.

    Must be a key of the `versions` dictionary.
    """
    PIXI_CONCURRENT_SOLVES: int = 4
    """
    Value of
    [`--concurrent-solves`](https://pixi.sh/latest/reference/cli/pixi/install/#arg---concurrent-solves)
    for `pixi install`.
    """
    PIXI_CONCURRENT_DOWNLOADS: int = 4
    """
    Value of
    [`--concurrent-downloads`](https://pixi.sh/latest/reference/cli/pixi/install/#arg---concurrent-downloads)
    for `pixi install`.
    """
    TOKIO_WORKER_THREADS: int = 2
    """
    From
    [Tokio documentation](
    https://docs.rs/tokio/latest/tokio/#cpu-bound-tasks-and-blocking-code
    )
    :

        The core threads are where all asynchronous code runs,
        and Tokio will by default spawn one for each CPU core.
        You can use the environment variable `TOKIO_WORKER_THREADS` to override
        the default value.
    """
    DEFAULT_ENVIRONMENT: str = "default"
    """
    Default pixi environment name.
    """
    DEFAULT_PLATFORM: str = "linux-64"
    """
    Default platform for pixi.
    """
    SLURM_CONFIG: PixiSLURMConfig | None = None
    """
    Required when using pixi in a SSH/SLURM deployment.
    """

    @model_validator(mode="after")
    def check_pixi_settings(self):
        if self.default_version not in self.versions:
            raise ValueError(
                f"Default version '{self.default_version}' not in "
                f"available version {list(self.versions.keys())}."
            )

        pixi_base_dir = Path(self.versions[self.default_version]).parent

        for key, value in self.versions.items():
            pixi_path = Path(value)

            if pixi_path.parent != pixi_base_dir:
                raise ValueError(
                    f"{pixi_path=} is not located within the {pixi_base_dir=}."
                )
            if pixi_path.name != key:
                raise ValueError(f"{pixi_path.name=} is not equal to {key=}")

        return self


class _ValidResourceBase(BaseModel):
    resource_type: Literal["slurm_sudo", "slurm_ssh", "local"]

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
            PixiSettings(**self.tasks_pixi_config)
        return self


class JobRunnerConfigLocal(BaseModel):
    """
    Specifications of the local-backend configuration

    Attributes:
        parallel_tasks_per_job:
            Maximum number of tasks to be run in parallel as part of a call to
            `FractalThreadPoolExecutor.map`; if `None`, then all tasks will
            start at the same time.
    """

    model_config = ConfigDict(extra="forbid")
    parallel_tasks_per_job: int | None = None


class JobRunnerConfigSLURM(BaseModel):
    """
    Specifications of the SLURM-backend configuration - FIXME
    """


class ValidResourceLocal(_ValidResourceBase):
    job_runner_config: JobRunnerConfigLocal


class ValidResourceSlurmSudo(_ValidResourceBase):
    resource_type: Literal["slurm_sudo"]
    job_slurm_python_worker: AbsolutePathStr
    job_runner_config: JobRunnerConfigSLURM


class ValidResourceSlurmSSH(_ValidResourceBase):
    resource_type: Literal["slurm_ssh"]
    hostname: NonEmptyStr
    job_slurm_python_worker: AbsolutePathStr
    job_remote_folder: NonEmptyStr
    job_runner_config: JobRunnerConfigSLURM


class ProfileValidationModel(BaseModel):  # FIXME: Move to another module
    resource_type: Literal["slurm_sudo", "slurm_ssh", "local"]


class _ValidProfileBase(BaseModel):
    pass


class ValidProfileLocal(_ValidProfileBase):
    pass


class ValidProfileSlurmSudo(_ValidProfileBase):
    username: NonEmptyStr


class ValidProfileSlurmSSH(_ValidProfileBase):
    username: NonEmptyStr
    ssh_key_path: AbsolutePathStr


class ResourceCreate(BaseModel):
    pass


class ResourceUpdate(BaseModel):
    pass


class ResourceRead(BaseModel):
    pass
