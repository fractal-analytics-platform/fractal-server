# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
"""
Submodule to handle the SLURM configuration for a WorkflowTask
"""
import json
from pathlib import Path
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic.error_wrappers import ValidationError

from .....config import get_settings
from .....logger import set_logger
from .....syringe import Inject

logger = set_logger(__name__)


class SlurmConfigError(ValueError):
    """
    Slurm configuration error
    """

    pass


class _SlurmConfigSet(BaseModel, extra=Extra.forbid):
    """
    Options that can be set in `FRACTAL_SLURM_CONFIG_FILE` for the default/gpu
    SLURM config. Only used as part of `SlurmConfigFile`.

    Attributes:
        partition:
        cpus_per_task:
        mem:
            See `_parse_mem_value` for details on allowed values.
        constraint:
        gres:
        time:
        account:
        extra_lines:
    """

    partition: Optional[str]
    cpus_per_task: Optional[int]
    mem: Optional[Union[int, str]]
    constraint: Optional[str]
    gres: Optional[str]
    time: Optional[str]
    account: Optional[str]
    extra_lines: Optional[list[str]]


class _BatchingConfigSet(BaseModel, extra=Extra.forbid):
    """
    Options that can be set in `FRACTAL_SLURM_CONFIG_FILE` to configure the
    batching strategy (that is, how to combine several tasks in a single SLURM
    job). Only used as part of `SlurmConfigFile`.

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

    target_cpus_per_job: int
    max_cpus_per_job: int
    target_mem_per_job: Union[int, str]
    max_mem_per_job: Union[int, str]
    target_num_jobs: int
    max_num_jobs: int


class SlurmConfigFile(BaseModel, extra=Extra.forbid):
    """
    Specifications for the content of `FRACTAL_SLURM_CONFIG_FILE`

    This must be a JSON file, and a valid example is
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

    See `_SlurmConfigSet` and `_BatchingConfigSet` for more details.

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

    default_slurm_config: _SlurmConfigSet
    gpu_slurm_config: Optional[_SlurmConfigSet]
    batching_config: _BatchingConfigSet
    user_local_exports: Optional[dict[str, str]]


def load_slurm_config_file(
    config_path: Optional[Path] = None,
) -> SlurmConfigFile:
    """
    Load a SLURM configuration file and validate its content with
    `SlurmConfigFile`.

    Arguments:
        config_path:
    """

    if not config_path:
        settings = Inject(get_settings)
        config_path = settings.FRACTAL_SLURM_CONFIG_FILE

    # Load file
    logger.debug(f"[get_slurm_config] Now loading {config_path=}")
    try:
        with config_path.open("r") as f:
            slurm_env = json.load(f)
    except Exception as e:
        raise SlurmConfigError(
            f"Error while loading {config_path=}. "
            f"Original error:\n{str(e)}"
        )

    # Validate file content
    logger.debug(f"[load_slurm_config_file] Now validating {config_path=}")
    logger.debug(f"[load_slurm_config_file] {slurm_env=}")
    try:
        obj = SlurmConfigFile(**slurm_env)
    except ValidationError as e:
        raise SlurmConfigError(
            f"Error while loading {config_path=}. "
            f"Original error:\n{str(e)}"
        )

    # Convert memory to MB units, in all relevant attributes
    if obj.default_slurm_config.mem:
        obj.default_slurm_config.mem = _parse_mem_value(
            obj.default_slurm_config.mem
        )
    if obj.gpu_slurm_config and obj.gpu_slurm_config.mem:
        obj.gpu_slurm_config.mem = _parse_mem_value(obj.gpu_slurm_config.mem)
    obj.batching_config.target_mem_per_job = _parse_mem_value(
        obj.batching_config.target_mem_per_job
    )
    obj.batching_config.max_mem_per_job = _parse_mem_value(
        obj.batching_config.max_mem_per_job
    )

    return obj


class SlurmConfig(BaseModel, extra=Extra.forbid):
    """
    Abstraction for SLURM parameters

    **NOTE**: `SlurmConfig` objects are created internally in `fractal-server`,
    and they are not meant to be initialized by the user; the same holds for
    `SlurmConfig` attributes (e.g. `mem_per_task_MB`), which are not meant to
    be part of the `FRACTAL_SLURM_CONFIG_FILE` JSON file (details on the
    expected file content are defined in
    [`SlurmConfigFile`](./#fractal_server.app.runner._slurm._slurm_config.SlurmConfigFile)).

    Part of the attributes map directly to some of the SLURM attribues (see
    https://slurm.schedmd.com/sbatch.html), e.g. `partition`. Other attributes
    are metaparameters which are needed in fractal-server to combine multiple
    tasks in the same SLURM job (e.g. `parallel_tasks_per_job` or
    `max_num_jobs`).

    Attributes:
        partition: Corresponds to SLURM option.
        cpus_per_task: Corresponds to SLURM option.
        mem_per_task_MB: Corresponds to `mem` SLURM option.
        job_name: Corresponds to `name` SLURM option.
        constraint: Corresponds to SLURM option.
        gres: Corresponds to SLURM option.
        account: Corresponds to SLURM option.
        time: Corresponds to SLURM option (WARNING: not fully supported).
        prefix: Prefix of configuration lines in SLURM submission scripts.
        shebang_line: Shebang line for SLURM submission scripts.
        extra_lines: Additional lines to include in SLURM submission scripts.
        tasks_per_job: Number of tasks for each SLURM job.
        parallel_tasks_per_job: Number of tasks to run in parallel for
                                each SLURM job.
        target_cpus_per_job: Optimal number of CPUs to be requested in each
                             SLURM job.
        max_cpus_per_job: Maximum number of CPUs that can be requested in each
                          SLURM job.
        target_mem_per_job: Optimal amount of memory (in MB) to be requested in
                            each SLURM job.
        max_mem_per_job: Maximum amount of memory (in MB) that can be requested
                         in each SLURM job.
        target_num_jobs: Optimal number of SLURM jobs for a given WorkflowTask.
        max_num_jobs: Maximum number of SLURM jobs for a given WorkflowTask.
        user_local_exports:
            Key-value pairs to be included as `export`-ed variables in SLURM
            submission script, after prepending values with the user's cache
            directory.
    """

    # Required SLURM parameters (note that the integer attributes are those
    # that will need to scale up with the number of parallel tasks per job)
    partition: str
    cpus_per_task: int
    mem_per_task_MB: int
    prefix: str = "#SBATCH"
    shebang_line: str = "#!/bin/sh"

    # Optional SLURM parameters
    job_name: Optional[str] = None
    constraint: Optional[str] = None
    gres: Optional[str] = None
    time: Optional[str] = None
    account: Optional[str] = None

    # Free-field attribute for extra lines to be added to the SLURM job
    # preamble
    extra_lines: Optional[list[str]] = Field(default_factory=list)

    # Variables that will be `export`ed in the SLURM submission script
    user_local_exports: Optional[dict[str, str]] = None

    # Metaparameters needed to combine multiple tasks in each SLURM job
    tasks_per_job: Optional[int] = None
    parallel_tasks_per_job: Optional[int] = None
    target_cpus_per_job: int
    max_cpus_per_job: int
    target_mem_per_job: int
    max_mem_per_job: int
    target_num_jobs: int
    max_num_jobs: int

    def _sorted_extra_lines(self) -> list[str]:
        """
        Return a copy of `self.extra_lines`, where lines starting with
        `self.prefix` are listed first.
        """

        def _no_prefix(_line):
            if _line.startswith(self.prefix):
                return 0
            else:
                return 1

        return sorted(self.extra_lines, key=_no_prefix)

    def sort_script_lines(self, script_lines: list[str]) -> list[str]:
        """
        Return a copy of `script_lines`, where lines are sorted as in:

        1. `self.shebang_line` (if present);
        2. Lines starting with `self.prefix`;
        3. Other lines.

        Arguments:
            script_lines:
        """

        def _sorting_function(_line):
            if _line == self.shebang_line:
                return 0
            elif _line.startswith(self.prefix):
                return 1
            else:
                return 2

        return sorted(script_lines, key=_sorting_function)

    def to_sbatch_preamble(
        self,
        remote_export_dir: Optional[str] = None,
    ) -> list[str]:
        """
        Compile `SlurmConfig` object into the preamble of a SLURM submission
        script.

        Arguments:
            user_cache_dir:
                Base directory for exports defined in
                `self.user_local_exports`.
        """
        if self.parallel_tasks_per_job is None:
            raise ValueError(
                "SlurmConfig.sbatch_preamble requires that "
                f"{self.parallel_tasks_per_job=} is not None."
            )
        if self.extra_lines:
            if len(self.extra_lines) != len(set(self.extra_lines)):
                raise ValueError(f"{self.extra_lines=} contains repetitions")

        mem_per_job_MB = self.parallel_tasks_per_job * self.mem_per_task_MB
        lines = [
            self.shebang_line,
            f"{self.prefix} --partition={self.partition}",
            f"{self.prefix} --ntasks={self.parallel_tasks_per_job}",
            f"{self.prefix} --cpus-per-task={self.cpus_per_task}",
            f"{self.prefix} --mem={mem_per_job_MB}M",
        ]
        for key in ["job_name", "constraint", "gres", "time", "account"]:
            value = getattr(self, key)
            if value is not None:
                # Handle the `time` parameter
                if key == "time" and self.parallel_tasks_per_job > 1:
                    # FIXME SSH: time setting must be handled better. Right now
                    # we simply propagate `time`, but this is not enough when
                    # several `srun` are combined in a single script.
                    logger.warning(
                        f"`time` SLURM parameter is set to {self.time}, "
                        "but this does not take into account the number of "
                        f"SLURM tasks ({self.parallel_tasks_per_job})."
                    )
                option = key.replace("_", "-")
                lines.append(f"{self.prefix} --{option}={value}")

        if self.extra_lines:
            for line in self._sorted_extra_lines():
                lines.append(line)

        if self.user_local_exports:
            if remote_export_dir is None:
                raise ValueError(
                    f"user_cache_dir=None but {self.user_local_exports=}"
                )
            for key, value in self.user_local_exports.items():
                tmp_value = str(Path(remote_export_dir) / value)
                lines.append(f"export {key}={tmp_value}")

        """
        FIXME export SRUN_CPUS_PER_TASK
        # From https://slurm.schedmd.com/sbatch.html: Beginning with 22.05,
        # srun will not inherit the --cpus-per-task value requested by salloc
        # or sbatch.  It must be requested again with the call to srun or set
        # with the SRUN_CPUS_PER_TASK environment variable if desired for the
        # task(s).
        if config.cpus_per_task:
            #additional_setup_lines.append(
                f"export SRUN_CPUS_PER_TASK={config.cpus_per_task}"
            )
        """

        return lines


def _parse_mem_value(raw_mem: Union[str, int]) -> int:
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
    if isinstance(raw_mem, int):
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


def get_default_slurm_config():
    """
    Return a default `SlurmConfig` configuration object
    """
    return SlurmConfig(
        partition="main",
        cpus_per_task=1,
        mem_per_task_MB=100,
        target_cpus_per_job=1,
        max_cpus_per_job=2,
        target_mem_per_job=100,
        max_mem_per_job=500,
        target_num_jobs=2,
        max_num_jobs=4,
    )
