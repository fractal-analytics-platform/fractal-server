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
Local Bakend

This backend runs Fractal workflows using `FractalThreadPoolExecutor` (a custom
version of Python
[ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor))
to run tasks in several threads.
Incidentally, it also represents the reference implementation for a backend.
"""
from pathlib import Path
from typing import Any
from typing import Optional

from ...models import Workflow
from .._common import execute_tasks
from ..common import async_wrap
from ..common import set_start_and_last_task_index
from ..common import TaskParameters
from ._submit_setup import _local_submit_setup
from .executor import FractalThreadPoolExecutor


def _process_workflow(
    *,
    workflow: Workflow,
    input_paths: list[Path],
    output_path: Path,
    input_metadata: dict[str, Any],
    logger_name: str,
    workflow_dir: Path,
    first_task_index: int,
    last_task_index: int,
) -> dict[str, Any]:
    """
    Internal processing routine

    Schedules the workflow using a `FractalThreadPoolExecutor`.

    Cf. [process_workflow][fractal_server.app.runner._local.process_workflow]
    for the call signature.
    """

    with FractalThreadPoolExecutor() as executor:
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
            workflow_dir_user=workflow_dir,
            logger_name=logger_name,
            submit_setup_call=_local_submit_setup,
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
    slurm_user: Optional[str] = None,
    user_cache_dir: Optional[str] = None,
    worker_init: Optional[str] = None,
    first_task_index: Optional[int] = None,
    last_task_index: Optional[int] = None,
) -> dict[str, Any]:
    """
    Run a workflow

    This function is responsible for running a workflow on some input data,
    saving the output and taking care of any exception raised during the run.

    NOTE: This is the `local` backend's public interface, which also works as
    a reference implementation for other backends.

    Args:
        workflow:
            The workflow to be run
        input_paths:
            The paths to the input files to pass to the first task of the
            workflow
        output_path:
            The destination path for the last task of the workflow
        input_metadata:
            Initial metadata, passed to the first task
        logger_name:
            Name of the logger to log information on the run to
        workflow_dir:
            Working directory for this run.
        workflow_dir_user:
            Working directory for this run, on the user side. This argument is
            present for compatibility with the standard backend interface, but
            for the `local` backend it cannot be different from `workflow_dir`.
        slurm_user:
            Username to impersonate to run the workflow. This argument is
            present for compatibility with the standard backend interface, but
            is ignored in the `local` backend.
        user_cache_dir:
            Cache directory of the user who will run the workflow. This
            argument is present for compatibility with the standard backend
            interface, but is ignored in the `local` backend.
        worker_init:
            Any additional, usually backend specific, information to be passed
            to the backend executor. This argument is present for compatibility
            with the standard backend interface, but is ignored in the `local`
            backend.
        first_task_index:
            Positional index of the first task to execute; if `None`, start
            from `0`.
        last_task_index:
            Positional index of the last task to execute; if `None`, proceed
            until the last task.

    Raises:
        TaskExecutionError: wrapper for errors raised during tasks' execution
                            (positive exit codes).
        JobExecutionError: wrapper for errors raised by the tasks' executors
                           (negative exit codes).

    Returns:
        output_dataset_metadata:
            The updated metadata for the dataset, as returned by the last task
            of the workflow
    """

    if workflow_dir_user and (workflow_dir_user != workflow_dir):
        raise NotImplementedError(
            "Local backend does not support different directories "
            f"{workflow_dir=} and {workflow_dir_user=}"
        )

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
        first_task_index=first_task_index,
        last_task_index=last_task_index,
    )
    return output_dataset_metadata
