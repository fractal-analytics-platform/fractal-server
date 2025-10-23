from typing import Annotated

from pydantic import AfterValidator
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic.types import PositiveInt

from fractal_server.runner.config.slurm_mem_to_MB import slurm_mem_to_MB
from fractal_server.types import DictStrStr
from fractal_server.types import NonEmptyStr


MemMBType = Annotated[
    PositiveInt | NonEmptyStr, AfterValidator(slurm_mem_to_MB)
]


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
    extra_lines: list[NonEmptyStr] = Field(default_factory=list)
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
    user_local_exports: DictStrStr = Field(default_factory=dict)
