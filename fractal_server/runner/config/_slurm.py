from typing import Annotated

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic.types import PositiveInt

from fractal_server.logger import set_logger
from fractal_server.types import DictStrStr
from fractal_server.types import NonEmptyStr

logger = set_logger(__name__)


class SlurmConfigError(ValueError):
    """
    Slurm configuration error
    """

    pass


def _mem_to_MB(raw_mem: str | int) -> int:
    """
    Convert a memory-specification string into an integer (in MB units), or
    simply return the input if it is already an integer.

    Supported units are `"M", "G", "T"`, with `"M"` being the default; some
    parsing examples are: `"10M" -> 10000`, `"3G" -> 3000000`.

    Arguments:
        raw_mem:
            A string (e.g. `"100M"`) or an integer (in MB).

    Returns:
        Integer value of memory in MB units.
    """

    info = f"[_parse_mem_value] {raw_mem=}"
    error_msg = (
        f"{info}, invalid specification of memory requirements "
        "(valid examples: 93, 71M, 93G, 71T)."
    )

    # Handle integer argument
    if type(raw_mem) is int:
        return raw_mem

    # Handle string argument
    if not raw_mem[0].isdigit():  # fail e.g. for raw_mem="M100"
        logger.error(error_msg)
        raise SlurmConfigError(error_msg)
    if raw_mem.isdigit():
        mem_MB = int(raw_mem)
    elif raw_mem.endswith("M"):
        stripped_raw_mem = raw_mem.strip("M")
        if not stripped_raw_mem.isdigit():
            logger.error(error_msg)
            raise SlurmConfigError(error_msg)
        mem_MB = int(stripped_raw_mem)
    elif raw_mem.endswith("G"):
        stripped_raw_mem = raw_mem.strip("G")
        if not stripped_raw_mem.isdigit():
            logger.error(error_msg)
            raise SlurmConfigError(error_msg)
        mem_MB = int(stripped_raw_mem) * 10**3
    elif raw_mem.endswith("T"):
        stripped_raw_mem = raw_mem.strip("T")
        if not stripped_raw_mem.isdigit():
            logger.error(error_msg)
            raise SlurmConfigError(error_msg)
        mem_MB = int(stripped_raw_mem) * 10**6
    else:
        logger.error(error_msg)
        raise SlurmConfigError(error_msg)

    logger.debug(f"{info}, return {mem_MB}")
    return mem_MB


MemMBType = Annotated[PositiveInt | NonEmptyStr, _mem_to_MB]


class _SlurmConfigSet(BaseModel):
    """
    Options for the default or gpu SLURM config.

    Attributes:
        partition:
        cpus_per_task:
        mem:
            See `_parse_mem_value` for details on allowed values.
        constraint:
        gres:
        time:
        exclude:
        nodelist:
        account:
        extra_lines:
    """

    model_config = ConfigDict(extra="forbid")

    partition: NonEmptyStr | None = None
    cpus_per_task: PositiveInt | None = None
    mem: MemMBType | None = None
    constraint: NonEmptyStr | None = None
    gres: NonEmptyStr | None = None
    exclude: NonEmptyStr | None = None
    nodelist: NonEmptyStr | None = None
    time: NonEmptyStr | None = None
    account: NonEmptyStr | None = None
    extra_lines: list[NonEmptyStr] | None = None
    gpus: NonEmptyStr | None = None


class _BatchingConfigSet(BaseModel):
    """
    Options to configure the batching strategy (that is, how to combine
    several tasks in a single SLURM job).

    Attributes:
        target_cpus_per_job:
        max_cpus_per_job:
        target_mem_per_job:
            (see `_parse_mem_value` for details on allowed values)
        max_mem_per_job:
            (see `_parse_mem_value` for details on allowed values)
        target_num_jobs:
        max_num_jobs:
    """

    model_config = ConfigDict(extra="forbid")

    target_num_jobs: PositiveInt
    max_num_jobs: PositiveInt
    target_cpus_per_job: PositiveInt
    max_cpus_per_job: PositiveInt
    target_mem_per_job: MemMBType
    max_mem_per_job: MemMBType


class JobRunnerConfigSLURM(BaseModel):
    """
    Common SLURM configuration.

    Note: this is a common and abstract class, which gets transformed into
    more specific configuration objects during job execution.

    Valid JSON example
    ```JSON
    {
      "default_slurm_config": {
          "partition": "main",
          "cpus_per_task": 1
      },
      "gpu_slurm_config": {
          "partition": "gpu",
          "extra_lines": ["#SBATCH --gres=gpu:v100:1"]
      },
      "batching_config": {
          "target_cpus_per_job": 1,
          "max_cpus_per_job": 1,
          "target_mem_per_job": 200,
          "max_mem_per_job": 500,
          "target_num_jobs": 2,
          "max_num_jobs": 4
      },
      "user_local_exports": {
          "CELLPOSE_LOCAL_MODELS_PATH": "CELLPOSE_LOCAL_MODELS_PATH",
          "NAPARI_CONFIG": "napari_config.json"
      }
    }
    ```

    Attributes:
        default_slurm_config:
            Common default options for all tasks.
        gpu_slurm_config:
            Default configuration for all GPU tasks.
        batching_config:
            Configuration of the batching strategy.
        user_local_exports:
            Key-value pairs to be included as `export`-ed variables in SLURM
            submission script, after prepending values with the user's cache
            directory.
    """

    model_config = ConfigDict(extra="forbid")

    default_slurm_config: _SlurmConfigSet
    gpu_slurm_config: _SlurmConfigSet | None = None
    batching_config: _BatchingConfigSet
    user_local_exports: DictStrStr | None = None
