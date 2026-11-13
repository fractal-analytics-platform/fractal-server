from pathlib import Path
from typing import Self

from pydantic import BaseModel
from pydantic import model_validator

from fractal_server.tasks.config._slurm import SLURMConfig
from fractal_server.types import DictStrStr


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
    [`--concurrent-solves`](https://pixi.sh/latest/code_reference/cli/pixi/install/#arg---concurrent-solves)
    for `pixi install`.
    """
    PIXI_CONCURRENT_DOWNLOADS: int = 4
    """
    Value of
    [`--concurrent-downloads`](https://pixi.sh/latest/code_reference/cli/pixi/install/#arg---concurrent-downloads)
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
    SLURM_CONFIG: SLURMConfig | None = None
    """
    Required when using `pixi` in a SSH/SLURM deployment.
    """

    @model_validator(mode="after")
    def check_pixi_settings(self) -> Self:
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
