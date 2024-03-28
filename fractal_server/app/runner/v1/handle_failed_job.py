# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
# Marco Franzon <marco.franzon@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
"""
Helper functions to handle Dataset history.
"""
import json
import logging
from copy import deepcopy
from pathlib import Path
from typing import Any
from typing import Optional

from ...models.v1 import ApplyWorkflow
from ...models.v1 import Dataset
from ...models.v1 import Workflow
from ...models.v1 import WorkflowTask
from ...schemas.v1 import WorkflowTaskStatusTypeV1
from ..filenames import HISTORY_FILENAME
from ..filenames import METADATA_FILENAME


def assemble_history_failed_job(
    job: ApplyWorkflow,
    output_dataset: Dataset,
    workflow: Workflow,
    logger: logging.Logger,
    failed_wftask: Optional[WorkflowTask] = None,
) -> list[dict[str, Any]]:
    """
    Assemble `history` after a workflow-execution job fails.

    Args:
        job:
            The failed `ApplyWorkflow` object.
        output_dataset:
            The `output_dataset` associated to `job`.
        workflow:
            The `workflow` associated to `job`.
        logger: A logger instance.
        failed_wftask:
            If set, append it to `history` during step 3; if `None`, infer
            it by comparing the job task list and the one in
            `tmp_metadata_file`.

    Returns:
        The new value of `history`, to be merged into
        `output_dataset.meta`.
    """

    # The final value of the history attribute should include up to three
    # parts, coming from: the database, the temporary file, the failed-task
    # information.

    # Part 1: Read exising history from DB
    new_history = output_dataset.history

    # Part 2: Extend history based on tmp_metadata_file
    tmp_history_file = Path(job.working_dir) / HISTORY_FILENAME
    try:
        with tmp_history_file.open("r") as f:
            tmp_file_history = json.load(f)
            new_history.extend(tmp_file_history)
    except FileNotFoundError:
        tmp_file_history = []

    # Part 3/A: Identify failed task, if needed
    if failed_wftask is None:
        job_wftasks = workflow.task_list[
            job.first_task_index : (job.last_task_index + 1)  # noqa
        ]
        tmp_file_wftasks = [
            history_item["workflowtask"] for history_item in tmp_file_history
        ]
        if len(job_wftasks) <= len(tmp_file_wftasks):
            n_tasks_job = len(job_wftasks)
            n_tasks_tmp = len(tmp_file_wftasks)
            logger.error(
                "Cannot identify the failed task based on job task list "
                f"(length {n_tasks_job}) and temporary-file task list "
                f"(length {n_tasks_tmp})."
            )
            logger.error("Failed task not appended to history.")
        else:
            failed_wftask = job_wftasks[len(tmp_file_wftasks)]

    # Part 3/B: Append failed task to history
    if failed_wftask is not None:
        failed_wftask_dump = failed_wftask.model_dump(exclude={"task"})
        failed_wftask_dump["task"] = failed_wftask.task.model_dump()
        new_history_item = dict(
            workflowtask=failed_wftask_dump,
            status=WorkflowTaskStatusTypeV1.FAILED,
            parallelization=dict(
                parallelization_level=failed_wftask.parallelization_level,
            ),
        )
        new_history.append(new_history_item)

    return new_history


def assemble_meta_failed_job(
    job: ApplyWorkflow,
    output_dataset: Dataset,
) -> dict[str, Any]:
    """
    Assemble `Dataset.meta` (history excluded) for a failed workflow-execution.

    Assemble new value of `output_dataset.meta` based on the last successful
    task, i.e. based on the content of the temporary `METADATA_FILENAME` file.

    Args:
        job:
            The failed `ApplyWorkflow` object.
        output_dataset:
            The `output_dataset` associated to `job`.

    Returns:
        The new value of `output_dataset.meta`, apart from its `history` key.
    """

    new_meta = deepcopy(output_dataset.meta)
    metadata_file = Path(job.working_dir) / METADATA_FILENAME
    try:
        with metadata_file.open("r") as f:
            metadata_update = json.load(f)
        for key, value in metadata_update.items():
            new_meta[key] = value
    except FileNotFoundError:
        pass

    return new_meta
