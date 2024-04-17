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
from pathlib import Path
from typing import Any
from typing import Optional

from ...models.v2 import DatasetV2
from ...models.v2 import JobV2
from ...models.v2 import WorkflowTaskV2
from ...models.v2 import WorkflowV2
from ...schemas.v2 import WorkflowTaskStatusTypeV2
from ..filenames import FILTERS_FILENAME
from ..filenames import HISTORY_FILENAME
from ..filenames import IMAGES_FILENAME


def assemble_history_failed_job(
    job: JobV2,
    dataset: DatasetV2,
    workflow: WorkflowV2,
    logger_name: Optional[str] = None,
    failed_wftask: Optional[WorkflowTaskV2] = None,
) -> list[dict[str, Any]]:
    """
    Assemble `history` after a workflow-execution job fails.

    Args:
        job:
            The failed `JobV2` object.
        dataset:
            The `DatasetV2` object associated to `job`.
        workflow:
            The `WorkflowV2` object associated to `job`.
        logger_name: A logger name.
        failed_wftask:
            If set, append it to `history` during step 3; if `None`, infer
            it by comparing the job task list and the one in
            `HISTORY_FILENAME`.

    Returns:
        The new value of `history`, to be merged into
        `dataset.meta`.
    """

    logger = logging.getLogger(logger_name)

    # The final value of the history attribute should include up to three
    # parts, coming from: the database, the temporary file, the failed-task
    # information.

    # Part 1: Read exising history from DB
    new_history = dataset.history

    # Part 2: Extend history based on temporary-file contents
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
        failed_wftask_dump = failed_wftask.model_dump(
            exclude={"task", "task_legacy"}
        )
        if failed_wftask.is_legacy_task:
            failed_wftask_dump[
                "task_legacy"
            ] = failed_wftask.task_legacy.model_dump()
        else:
            failed_wftask_dump["task"] = failed_wftask.task.model_dump()
        new_history_item = dict(
            workflowtask=failed_wftask_dump,
            status=WorkflowTaskStatusTypeV2.FAILED,
            parallelization=dict(),  # FIXME: re-include parallelization
        )
        new_history.append(new_history_item)

    return new_history


def assemble_images_failed_job(job: JobV2) -> Optional[dict[str, Any]]:
    """
    Assemble `DatasetV2.images` for a failed workflow-execution.

    Assemble new value of `images` based on the last successful task, i.e.
    based on the content of the temporary `IMAGES_FILENAME` file. If the file
    is missing, return `None`.

    Argumentss:
        job:
            The failed `JobV2` object.

    Returns:
        The new value of `dataset.images`, or `None` if `IMAGES_FILENAME`
        is missing.
    """
    tmp_file = Path(job.working_dir) / IMAGES_FILENAME
    try:
        with tmp_file.open("r") as f:
            new_images = json.load(f)
        return new_images
    except FileNotFoundError:
        return None


def assemble_filters_failed_job(job: JobV2) -> Optional[dict[str, Any]]:
    """
    Assemble `DatasetV2.filters` for a failed workflow-execution.

    Assemble new value of `filters` based on the last successful task, i.e.
    based on the content of the temporary `FILTERS_FILENAME` file. If the file
    is missing, return `None`.

    Argumentss:
        job:
            The failed `JobV2` object.

    Returns:
        The new value of `dataset.filters`, or `None` if `FILTERS_FILENAME`
        is missing.
    """
    tmp_file = Path(job.working_dir) / FILTERS_FILENAME
    try:
        with tmp_file.open("r") as f:
            new_filters = json.load(f)
        return new_filters
    except FileNotFoundError:
        return None
