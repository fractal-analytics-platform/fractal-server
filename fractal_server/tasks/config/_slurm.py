from typing import Annotated
from typing import Self

from pydantic import AfterValidator
from pydantic import BaseModel
from pydantic import Field
from pydantic import PositiveInt
from pydantic import model_validator

from fractal_server.types import NonEmptyStr


def _check_slurm_memory(mem: str) -> str:
    if mem[-1] not in ["K", "M", "G", "T"]:
        raise ValueError(
            f"Invalid memory requirement '{mem}', please set a K/M/G/T units "
            "suffix."
        )
    return mem


MemoryStr = Annotated[NonEmptyStr, AfterValidator(_check_slurm_memory)]


class SLURMConfig(BaseModel):
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
    mem: MemoryStr | None = None
    """
    `--mem=<size>[units]` (examples: `"10M"`, `"10G"`).
    From `sbatch` docs: Specify the real memory required per node. Default
    units are megabytes. Different units can be specified using the suffix
    [K|M|G|T].
    """
    mem_per_cpu: MemoryStr | None = None
    """
    `--mem-per-cpu=<size>[units]` (examples: `"10M"`, `"10G"`).
    From `sbatch` docs: Minimum memory required per usable allocated CPU.
    Default units are megabytes. [..] The --mem, --mem-per-cpu and
    --mem-per-gpu options are mutually exclusive.
    """
    time: NonEmptyStr
    """
    `-t, --time=<time>`.
    From `sbatch` docs: "A time limit of zero requests that no time limit be
    imposed. Acceptable time formats include "minutes", "minutes:seconds",
    "hours:minutes:seconds", "days-hours", "days-hours:minutes" and
    "days-hours:minutes:seconds".
    """
    preamble: list[str] = Field(default_factory=list)
    """
    Lines to be included at the beginning of the SLURM submission script.
    """

    @model_validator(mode="after")
    def _memory_validator(self: Self) -> Self:
        if (self.mem is None and self.mem_per_cpu is None) or (
            self.mem is not None and self.mem_per_cpu is not None
        ):
            raise ValueError("You must set either `mem` or `mem_per_cpu`.")
        return self
