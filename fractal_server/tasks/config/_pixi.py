from pathlib import Path
from typing import Annotated

from pydantic import AfterValidator
from pydantic import BaseModel
from pydantic import model_validator
from pydantic import PositiveInt

from fractal_server.types import DictStrStr
from fractal_server.types import NonEmptyStr


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


class TasksPixiSettings(BaseModel):
    """
    Configuration for `pixi` Task collection.
    """

    versions: DictStrStr
    """
    Dictionary mapping `pixi` versions (e.g. `0.47.0`) to the corresponding
    folders (e.g. `/somewhere/pixi/0.47.0` - if the binary is
    `/somewhere/pixi/0.47.0/bin/pixi`).
    """
    default_version: str
    """
    Default task-collection `pixi` version.
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
    Required when using `pixi` in a SSH/SLURM deployment.
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
