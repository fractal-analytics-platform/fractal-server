# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
# Marco Franzon <marco.franzon@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
"""
Slurm Bakend

This backend runs fractal workflows in a SLURM cluster using Clusterfutures
Executor objects.
"""
from pathlib import Path
from typing import Any
from typing import Optional
from typing import Union

from ...async_wrap import async_wrap
from ...executors.slurm.sudo.executor import FractalSlurmExecutor
from ...set_start_and_last_task_index import set_start_and_last_task_index
from .._common import execute_tasks
from ..common import TaskParameters
from ._submit_setup import _slurm_submit_setup
from fractal_server.app.models.v1 import Workflow
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


def _process_workflow(
    *,
    workflow: Workflow,
    input_paths: list[Path],
    output_path: Path,
    input_metadata: dict[str, Any],
    input_history: list[dict[str, Any]],
    logger_name: str,
    workflow_dir_local: Path,
    workflow_dir_remote: Path,
    first_task_index: int,
    last_task_index: int,
    slurm_user: Optional[str] = None,
    slurm_account: Optional[str] = None,
    user_cache_dir: str,
    worker_init: Optional[Union[str, list[str]]] = None,
) -> dict[str, Any]:
    """
    Internal processing routine for the SLURM backend

    This function initialises the a FractalSlurmExecutor, setting logging,
    workflow working dir and user to impersonate. It then schedules the
    workflow tasks and returns the output dataset metadata.

    Cf.
    [process_workflow][fractal_server.app.runner.v1._local.process_workflow]

    Returns:
        output_dataset_metadata: Metadata of the output dataset
    """

    if not slurm_user:
        raise RuntimeError(
            "slurm_user argument is required, for slurm backend"
        )

    if isinstance(worker_init, str):
        worker_init = worker_init.split("\n")

    with FractalSlurmExecutor(
        debug=True,
        keep_logs=True,
        slurm_user=slurm_user,
        user_cache_dir=user_cache_dir,
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        common_script_lines=worker_init,
        slurm_account=slurm_account,
    ) as executor:
        output_task_pars = execute_tasks(
            executor=executor,
            task_list=workflow.task_list[
                first_task_index : (last_task_index + 1)  # noqa
            ],  # noqa
            task_pars=TaskParameters(
                input_paths=input_paths,
                output_path=output_path,
                metadata=input_metadata,
                history=input_history,
            ),
            workflow_dir_local=workflow_dir_local,
            workflow_dir_remote=workflow_dir_remote,
            submit_setup_call=_slurm_submit_setup,
            logger_name=logger_name,
        )
    output_dataset_metadata_history = dict(
        metadata=output_task_pars.metadata, history=output_task_pars.history
    )
    return output_dataset_metadata_history


async def process_workflow(
    *,
    workflow: Workflow,
    input_paths: list[Path],
    output_path: Path,
    input_metadata: dict[str, Any],
    input_history: list[dict[str, Any]],
    logger_name: str,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    user_cache_dir: Optional[str] = None,
    slurm_user: Optional[str] = None,
    slurm_account: Optional[str] = None,
    worker_init: Optional[str] = None,
    first_task_index: Optional[int] = None,
    last_task_index: Optional[int] = None,
) -> dict[str, Any]:
    """
    Process workflow (SLURM backend public interface)

    Cf.
    [process_workflow][fractal_server.app.runner.v1._local.process_workflow]
    """

    # Set values of first_task_index and last_task_index
    num_tasks = len(workflow.task_list)
    first_task_index, last_task_index = set_start_and_last_task_index(
        num_tasks,
        first_task_index=first_task_index,
        last_task_index=last_task_index,
    )

    output_dataset_metadata_history = await async_wrap(_process_workflow)(
        workflow=workflow,
        input_paths=input_paths,
        output_path=output_path,
        input_metadata=input_metadata,
        input_history=input_history,
        logger_name=logger_name,
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        slurm_user=slurm_user,
        slurm_account=slurm_account,
        user_cache_dir=user_cache_dir,
        worker_init=worker_init,
        first_task_index=first_task_index,
        last_task_index=last_task_index,
    )
    return output_dataset_metadata_history


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
