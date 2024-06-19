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

from ....models.v2 import DatasetV2
from ....models.v2 import WorkflowV2
from ...async_wrap import async_wrap
from ...executors.slurm.sudo.executor import FractalSlurmExecutor
from ...set_start_and_last_task_index import set_start_and_last_task_index
from ..runner import execute_tasks_v2
from ._submit_setup import _slurm_submit_setup


def _process_workflow(
    *,
    workflow: WorkflowV2,
    dataset: DatasetV2,
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
    workflow tasks and returns the new dataset attributes

    Cf.
    [process_workflow][fractal_server.app.runner.v2._local.process_workflow]

    Returns:
        new_dataset_attributes:
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
        new_dataset_attributes = execute_tasks_v2(
            wf_task_list=workflow.task_list[
                first_task_index : (last_task_index + 1)  # noqa
            ],  # noqa
            dataset=dataset,
            executor=executor,
            workflow_dir_local=workflow_dir_local,
            workflow_dir_remote=workflow_dir_remote,
            logger_name=logger_name,
            submit_setup_call=_slurm_submit_setup,
        )
    return new_dataset_attributes


async def process_workflow(
    *,
    workflow: WorkflowV2,
    dataset: DatasetV2,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    first_task_index: Optional[int] = None,
    last_task_index: Optional[int] = None,
    logger_name: str,
    # Slurm-specific
    user_cache_dir: Optional[str] = None,
    slurm_user: Optional[str] = None,
    slurm_account: Optional[str] = None,
    worker_init: Optional[str] = None,
) -> dict:
    """
    Process workflow (SLURM backend public interface)

    Cf.
    [process_workflow][fractal_server.app.runner.v2._local.process_workflow]
    """

    # Set values of first_task_index and last_task_index
    num_tasks = len(workflow.task_list)
    first_task_index, last_task_index = set_start_and_last_task_index(
        num_tasks,
        first_task_index=first_task_index,
        last_task_index=last_task_index,
    )

    new_dataset_attributes = await async_wrap(_process_workflow)(
        workflow=workflow,
        dataset=dataset,
        logger_name=logger_name,
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        first_task_index=first_task_index,
        last_task_index=last_task_index,
        user_cache_dir=user_cache_dir,
        slurm_user=slurm_user,
        slurm_account=slurm_account,
        worker_init=worker_init,
    )
    return new_dataset_attributes
