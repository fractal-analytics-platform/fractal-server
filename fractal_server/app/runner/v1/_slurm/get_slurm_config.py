from pathlib import Path
from typing import Optional

from fractal_server.app.models.v1 import WorkflowTask
from fractal_server.app.runner.executors.slurm._slurm_config import (
    _parse_mem_value,
)
from fractal_server.app.runner.executors.slurm._slurm_config import (
    load_slurm_config_file,
)
from fractal_server.app.runner.executors.slurm._slurm_config import logger
from fractal_server.app.runner.executors.slurm._slurm_config import SlurmConfig
from fractal_server.app.runner.executors.slurm._slurm_config import (
    SlurmConfigError,
)


def get_slurm_config(
    wftask: WorkflowTask,
    workflow_dir_local: Path,
    workflow_dir_remote: Path,
    config_path: Optional[Path] = None,
) -> SlurmConfig:
    """
    Prepare a `SlurmConfig` configuration object

    The sources for `SlurmConfig` attributes, in increasing priority order, are

    1. The general content of the Fractal SLURM configuration file.
    2. The GPU-specific content of the Fractal SLURM configuration file, if
        appropriate.
    3. Properties in `wftask.meta` (which, for `WorkflowTask`s added through
       `Workflow.insert_task`, also includes `wftask.task.meta`);

    Note: `wftask.meta` may be `None`.

    Arguments:
        wftask:
            WorkflowTask for which the SLURM configuration is is to be
            prepared.
        workflow_dir_local:
            Server-owned directory to store all task-execution-related relevant
            files (inputs, outputs, errors, and all meta files related to the
            job execution). Note: users cannot write directly to this folder.
        workflow_dir_remote:
            User-side directory with the same scope as `workflow_dir_local`,
            and where a user can write.
        config_path:
            Path of aFractal  SLURM configuration file; if `None`, use
            `FRACTAL_SLURM_CONFIG_FILE` variable from settings.

    Returns:
        slurm_config:
            The SlurmConfig object
    """

    logger.debug(
        "[get_slurm_config] WorkflowTask meta attribute: {wftask.meta=}"
    )

    # Incorporate slurm_env.default_slurm_config
    slurm_env = load_slurm_config_file(config_path=config_path)
    slurm_dict = slurm_env.default_slurm_config.dict(
        exclude_unset=True, exclude={"mem"}
    )
    if slurm_env.default_slurm_config.mem:
        slurm_dict["mem_per_task_MB"] = slurm_env.default_slurm_config.mem

    # Incorporate slurm_env.batching_config
    for key, value in slurm_env.batching_config.dict().items():
        slurm_dict[key] = value

    # Incorporate slurm_env.user_local_exports
    slurm_dict["user_local_exports"] = slurm_env.user_local_exports

    logger.debug(
        "[get_slurm_config] Fractal SLURM configuration file: "
        f"{slurm_env.dict()=}"
    )

    # GPU-related options
    # Notes about priority:
    # 1. This block of definitions takes priority over other definitions from
    #    slurm_env which are not under the `needs_gpu` subgroup
    # 2. This block of definitions has lower priority than whatever comes next
    #    (i.e. from WorkflowTask.meta).
    if wftask.meta is not None:
        needs_gpu = wftask.meta.get("needs_gpu", False)
    else:
        needs_gpu = False
    logger.debug(f"[get_slurm_config] {needs_gpu=}")
    if needs_gpu:
        for key, value in slurm_env.gpu_slurm_config.dict(
            exclude_unset=True, exclude={"mem"}
        ).items():
            slurm_dict[key] = value
        if slurm_env.gpu_slurm_config.mem:
            slurm_dict["mem_per_task_MB"] = slurm_env.gpu_slurm_config.mem

    # Number of CPUs per task, for multithreading
    if wftask.meta is not None and "cpus_per_task" in wftask.meta:
        cpus_per_task = int(wftask.meta["cpus_per_task"])
        slurm_dict["cpus_per_task"] = cpus_per_task

    # Required memory per task, in MB
    if wftask.meta is not None and "mem" in wftask.meta:
        raw_mem = wftask.meta["mem"]
        mem_per_task_MB = _parse_mem_value(raw_mem)
        slurm_dict["mem_per_task_MB"] = mem_per_task_MB

    # Job name
    job_name = wftask.task.name.replace(" ", "_")
    slurm_dict["job_name"] = job_name

    # Optional SLURM arguments and extra lines
    if wftask.meta is not None:
        account = wftask.meta.get("account", None)
        if account is not None:
            error_msg = (
                f"Invalid {account=} property in WorkflowTask `meta` "
                "attribute.\n"
                "SLURM account must be set in the request body of the "
                "apply-workflow endpoint, or by modifying the user properties."
            )
            logger.error(error_msg)
            raise SlurmConfigError(error_msg)
        for key in ["time", "gres", "constraint"]:
            value = wftask.meta.get(key, None)
            if value:
                slurm_dict[key] = value
    if wftask.meta is not None:
        extra_lines = wftask.meta.get("extra_lines", [])
    else:
        extra_lines = []
    extra_lines = slurm_dict.get("extra_lines", []) + extra_lines
    if len(set(extra_lines)) != len(extra_lines):
        logger.debug(
            "[get_slurm_config] Removing repeated elements "
            f"from {extra_lines=}."
        )
        extra_lines = list(set(extra_lines))
    slurm_dict["extra_lines"] = extra_lines

    # Job-batching parameters (if None, they will be determined heuristically)
    if wftask.meta is not None:
        tasks_per_job = wftask.meta.get("tasks_per_job", None)
        parallel_tasks_per_job = wftask.meta.get(
            "parallel_tasks_per_job", None
        )
    else:
        tasks_per_job = None
        parallel_tasks_per_job = None
    slurm_dict["tasks_per_job"] = tasks_per_job
    slurm_dict["parallel_tasks_per_job"] = parallel_tasks_per_job

    # Put everything together
    logger.debug(
        "[get_slurm_config] Now create a SlurmConfig object based "
        f"on {slurm_dict=}"
    )
    slurm_config = SlurmConfig(**slurm_dict)

    return slurm_config
