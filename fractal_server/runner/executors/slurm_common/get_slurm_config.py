from typing import Literal

from ._batching import heuristics
from .slurm_config import logger
from .slurm_config import SlurmConfig
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.runner.config import JobRunnerConfigSLURM
from fractal_server.runner.config.slurm_mem_to_MB import slurm_mem_to_MB
from fractal_server.runner.exceptions import SlurmConfigError
from fractal_server.string_tools import interpret_as_bool


def _get_slurm_config_internal(
    shared_config: JobRunnerConfigSLURM,
    wftask: WorkflowTaskV2,
    which_type: Literal["non_parallel", "parallel"],
) -> SlurmConfig:
    """

    Prepare a specific `SlurmConfig` configuration.

    The base configuration is the runner-level `shared_config` object, based
    on `resource.jobs_runner_config` (note that GPU-specific properties take
    priority, when `needs_gpu=True`). We then incorporate attributes from
    `wftask.meta_{non_parallel,parallel}` - with higher priority.

    Args:
        shared_config:
            Configuration object based on `resource.jobs_runner_config`.
        wftask:
            WorkflowTaskV2 for which the backend configuration should
            be prepared.
        which_type:
            Whether we should look at the non-parallel or parallel part
            of `wftask`.
        tot_tasks: Not used here, only present as a common interface.

    Returns:
        A ready-to-use `SlurmConfig` object.
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

    # Start from `shared_config`
    slurm_dict = shared_config.default_slurm_config.model_dump(
        exclude_unset=True, exclude={"mem"}
    )
    if shared_config.default_slurm_config.mem:
        slurm_dict["mem_per_task_MB"] = shared_config.default_slurm_config.mem

    # Incorporate slurm_env.batching_config
    for key, value in shared_config.batching_config.model_dump().items():
        slurm_dict[key] = value

    # Incorporate slurm_env.user_local_exports
    slurm_dict["user_local_exports"] = shared_config.user_local_exports

    # GPU-related options
    # Notes about priority:
    # 1. This block of definitions takes priority over other definitions from
    #    slurm_env which are not under the `needs_gpu` subgroup
    # 2. This block of definitions has lower priority than whatever comes next
    #    (i.e. from WorkflowTask.meta_parallel).
    if wftask_meta is not None:
        needs_gpu = interpret_as_bool(wftask_meta.get("needs_gpu", False))
    else:
        needs_gpu = False
    logger.debug(f"[get_slurm_config] {needs_gpu=}")
    if needs_gpu and shared_config.gpu_slurm_config is not None:
        for key, value in shared_config.gpu_slurm_config.model_dump(
            exclude_unset=True, exclude={"mem"}
        ).items():
            slurm_dict[key] = value
        if shared_config.gpu_slurm_config.mem:
            slurm_dict["mem_per_task_MB"] = shared_config.gpu_slurm_config.mem

    # Number of CPUs per task, for multithreading
    if wftask_meta is not None and "cpus_per_task" in wftask_meta:
        cpus_per_task = int(wftask_meta["cpus_per_task"])
        slurm_dict["cpus_per_task"] = cpus_per_task

    # Required memory per task, in MB
    if wftask_meta is not None and "mem" in wftask_meta:
        raw_mem = wftask_meta["mem"]
        mem_per_task_MB = slurm_mem_to_MB(raw_mem)
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
            f"[get_slurm_config] Removing repeated elements in {extra_lines=}."
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
        f"[get_slurm_config] Create SlurmConfig object based on {slurm_dict=}"
    )
    slurm_config = SlurmConfig(**slurm_dict)

    return slurm_config


def get_slurm_config(
    shared_config: JobRunnerConfigSLURM,
    wftask: WorkflowTaskV2,
    which_type: Literal["non_parallel", "parallel"],
    tot_tasks: int = 1,
) -> SlurmConfig:
    config = _get_slurm_config_internal(
        shared_config=shared_config,
        wftask=wftask,
        which_type=which_type,
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
