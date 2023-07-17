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
Slurm Bakend

This backend runs fractal workflows in a SLURM cluster using Clusterfutures
Executor objects.
"""
from pathlib import Path
from typing import Any
from typing import Optional
from typing import Union

from ...models import Workflow
from .._common import execute_tasks
from ..common import async_wrap
from ..common import set_start_and_last_task_index
from ..common import TaskParameters
from ._submit_setup import _slurm_submit_setup
from .executor import FractalSlurmExecutor


def _process_workflow(
    *,
    workflow: Workflow,
    input_paths: list[Path],
    output_path: Path,
    input_metadata: dict[str, Any],
    logger_name: str,
    workflow_dir: Path,
    workflow_dir_user: Path,
    first_task_index: int,
    last_task_index: int,
    slurm_user: Optional[str] = None,
    user_cache_dir: str,
    worker_init: Optional[Union[str, list[str]]] = None,
) -> dict[str, Any]:
    """
    Internal processing routine for the SLURM backend

    This function initialises the a FractalSlurmExecutor, setting logging,
    workflow working dir and user to impersonate. It then schedules the
    workflow tasks and returns the output dataset metadata.

    Cf. [process_workflow][fractal_server.app.runner._local.process_workflow]

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
        working_dir=workflow_dir,
        working_dir_user=workflow_dir_user,
        common_script_lines=worker_init,
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
            ),
            workflow_dir=workflow_dir,
            workflow_dir_user=workflow_dir_user,
            submit_setup_call=_slurm_submit_setup,
            logger_name=logger_name,
        )
    output_dataset_metadata = output_task_pars.metadata
    return output_dataset_metadata


async def process_workflow(
    *,
    workflow: Workflow,
    input_paths: list[Path],
    output_path: Path,
    input_metadata: dict[str, Any],
    logger_name: str,
    workflow_dir: Path,
    workflow_dir_user: Optional[Path] = None,
    user_cache_dir: Optional[str] = None,
    slurm_user: Optional[str] = None,
    worker_init: Optional[str] = None,
    first_task_index: Optional[int] = None,
    last_task_index: Optional[int] = None,
) -> dict[str, Any]:
    """
    Process workflow (SLURM backend public interface)

    Cf. [process_workflow][fractal_server.app.runner._local.process_workflow]
    """

    # Set values of first_task_index and last_task_index
    num_tasks = len(workflow.task_list)
    first_task_index, last_task_index = set_start_and_last_task_index(
        num_tasks,
        first_task_index=first_task_index,
        last_task_index=last_task_index,
    )

    output_dataset_metadata = await async_wrap(_process_workflow)(
        workflow=workflow,
        input_paths=input_paths,
        output_path=output_path,
        input_metadata=input_metadata,
        logger_name=logger_name,
        workflow_dir=workflow_dir,
        workflow_dir_user=workflow_dir_user,
        slurm_user=slurm_user,
        user_cache_dir=user_cache_dir,
        worker_init=worker_init,
        first_task_index=first_task_index,
        last_task_index=last_task_index,
    )
    return output_dataset_metadata
