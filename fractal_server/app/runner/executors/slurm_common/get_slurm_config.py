from pathlib import Path
from typing import Literal

from ._batching import heuristics
from ._slurm_config import _parse_mem_value
from ._slurm_config import load_slurm_config_file
from ._slurm_config import logger
from ._slurm_config import SlurmConfig
from ._slurm_config import SlurmConfigError
from fractal_server.app.models.v2 import WorkflowTaskV2


def get_slurm_config_internal(
    wftask: WorkflowTaskV2,
    which_type: Literal["non_parallel", "parallel"],
    config_path: Path | None = None,
) -> SlurmConfig:
    """
    Prepare a `SlurmConfig` configuration object

    The argument `which_type` determines whether we use `wftask.meta_parallel`
    or `wftask.meta_non_parallel`. In the following description, let us assume
    that `which_type="parallel"`.

    The sources for `SlurmConfig` attributes, in increasing priority order, are

    1. The general content of the Fractal SLURM configuration file.
    2. The GPU-specific content of the Fractal SLURM configuration file, if
        appropriate.
    3. Properties in `wftask.meta_parallel` (which typically include those in
       `wftask.task.meta_parallel`). Note that `wftask.meta_parallel` may be
       `None`.

    Arguments:
        wftask:
            WorkflowTask for which the SLURM configuration is is to be
            prepared.
        config_path:
            Path of a Fractal SLURM configuration file; if `None`, use
            `FRACTAL_SLURM_CONFIG_FILE` variable from settings.
        which_type:
            Determines whether to use `meta_parallel` or `meta_non_parallel`.

    Returns:
        slurm_config:
            The SlurmConfig object
    """

    if which_type == "non_parallel":
        wftask_meta = wftask.meta_non_parallel
    elif which_type == "parallel":
        wftask_meta = wftask.meta_parallel
    else:
        raise ValueError(
            f"get_slurm_config received invalid argument {which_type=}."
        )

    logger.debug(
        f"[get_slurm_config] WorkflowTask meta attribute: {wftask_meta=}"
    )

    # Incorporate slurm_env.default_slurm_config
    slurm_env = load_slurm_config_file(config_path=config_path)
    slurm_dict = slurm_env.default_slurm_config.model_dump(
        exclude_unset=True, exclude={"mem"}
    )
    if slurm_env.default_slurm_config.mem:
        slurm_dict["mem_per_task_MB"] = slurm_env.default_slurm_config.mem

    # Incorporate slurm_env.batching_config
    for key, value in slurm_env.batching_config.model_dump().items():
        slurm_dict[key] = value

    # Incorporate slurm_env.user_local_exports
    slurm_dict["user_local_exports"] = slurm_env.user_local_exports

    logger.debug(
        "[get_slurm_config] Fractal SLURM configuration file: "
        f"{slurm_env.model_dump()=}"
    )

    # GPU-related options
    # Notes about priority:
    # 1. This block of definitions takes priority over other definitions from
    #    slurm_env which are not under the `needs_gpu` subgroup
    # 2. This block of definitions has lower priority than whatever comes next
    #    (i.e. from WorkflowTask.meta_parallel).
    if wftask_meta is not None:
        needs_gpu = wftask_meta.get("needs_gpu", False)
    else:
        needs_gpu = False
    logger.debug(f"[get_slurm_config] {needs_gpu=}")
    if needs_gpu:
        for key, value in slurm_env.gpu_slurm_config.model_dump(
            exclude_unset=True, exclude={"mem"}
        ).items():
            slurm_dict[key] = value
        if slurm_env.gpu_slurm_config.mem:
            slurm_dict["mem_per_task_MB"] = slurm_env.gpu_slurm_config.mem

    # Number of CPUs per task, for multithreading
    if wftask_meta is not None and "cpus_per_task" in wftask_meta:
        cpus_per_task = int(wftask_meta["cpus_per_task"])
        slurm_dict["cpus_per_task"] = cpus_per_task

    # Required memory per task, in MB
    if wftask_meta is not None and "mem" in wftask_meta:
        raw_mem = wftask_meta["mem"]
        mem_per_task_MB = _parse_mem_value(raw_mem)
        slurm_dict["mem_per_task_MB"] = mem_per_task_MB

    # Job name
    job_name = wftask.task.name.replace(" ", "_")
    slurm_dict["job_name"] = job_name

    # Optional SLURM arguments and extra lines
    if wftask_meta is not None:
        account = wftask_meta.get("account", None)
        if account is not None:
            error_msg = (
                f"Invalid {account=} property in WorkflowTask `meta` "
                "attribute.\n"
                "SLURM account must be set in the request body of the "
                "apply-workflow endpoint, or by modifying the user properties."
            )
            logger.error(error_msg)
            raise SlurmConfigError(error_msg)
        for key in [
            "time",
            "gres",
            "gpus",
            "constraint",
            "nodelist",
            "exclude",
        ]:
            value = wftask_meta.get(key, None)
            if value is not None:
                slurm_dict[key] = value
    if wftask_meta is not None:
        extra_lines = wftask_meta.get("extra_lines", [])
    else:
        extra_lines = []
    extra_lines = slurm_dict.get("extra_lines", []) + extra_lines
    if len(set(extra_lines)) != len(extra_lines):
        logger.debug(
            "[get_slurm_config] Removing repeated elements from "
            f"{extra_lines=}."
        )
        extra_lines = list(set(extra_lines))
    slurm_dict["extra_lines"] = extra_lines

    # Job-batching parameters (if None, they will be determined heuristically)
    if wftask_meta is not None:
        tasks_per_job = wftask_meta.get("tasks_per_job", None)
        parallel_tasks_per_job = wftask_meta.get(
            "parallel_tasks_per_job", None
        )
    else:
        tasks_per_job = None
        parallel_tasks_per_job = None
    slurm_dict["tasks_per_job"] = tasks_per_job
    slurm_dict["parallel_tasks_per_job"] = parallel_tasks_per_job

    # Put everything together
    logger.debug(
        "[get_slurm_config] Now create a SlurmConfig object based on "
        f"{slurm_dict=}"
    )
    slurm_config = SlurmConfig(**slurm_dict)

    return slurm_config


def get_slurm_config(
    wftask: WorkflowTaskV2,
    which_type: Literal["non_parallel", "parallel"],
    config_path: Path | None = None,
    tot_tasks: int = 1,
) -> SlurmConfig:
    config = get_slurm_config_internal(
        wftask,
        which_type,
        config_path,
    )

    # Set/validate parameters for task batching
    tasks_per_job, parallel_tasks_per_job = heuristics(
        # Number of parallel components (always known)
        tot_tasks=tot_tasks,
        # Optional WorkflowTask attributes:
        tasks_per_job=config.tasks_per_job,
        parallel_tasks_per_job=config.parallel_tasks_per_job,  # noqa
        # Task requirements (multiple possible sources):
        cpus_per_task=config.cpus_per_task,
        mem_per_task=config.mem_per_task_MB,
        # Fractal configuration variables (soft/hard limits):
        target_cpus_per_job=config.target_cpus_per_job,
        target_mem_per_job=config.target_mem_per_job,
        target_num_jobs=config.target_num_jobs,
        max_cpus_per_job=config.max_cpus_per_job,
        max_mem_per_job=config.max_mem_per_job,
        max_num_jobs=config.max_num_jobs,
    )
    config.parallel_tasks_per_job = parallel_tasks_per_job
    config.tasks_per_job = tasks_per_job

    return config
