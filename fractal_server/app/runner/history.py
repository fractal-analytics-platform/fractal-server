# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
"""
Helper functions to handle Dataset history
"""
import json
import logging
from pathlib import Path
from typing import Any

from ..models import Dataset
from ..models import Workflow
from ..models import WorkflowTaskStatusType


def handle_history_failed_job(
    output_dataset: Dataset,
    tmp_metadata_file: Path,
    workflow: Workflow,
    first_task_index: int,
    last_task_index: int,
    logger: logging.Logger,
) -> list[dict[str, Any]]:
    """
    FIXME

    FIXME: define model for output
    """

    # The final value of the history_next attribute should include up to three
    # parts, coming from: the database, the temporary file, the failed-task
    # information.

    new_history_next = output_dataset.meta.get("history_next", [])

    # Part 2: Extract history_next from METADATA_FILENAME
    try:
        with tmp_metadata_file.open("r") as f:
            tmp_file_meta = json.load(f)
            tmp_file_history_next = tmp_file_meta.get("history_next", [])
            new_history_next.extend(tmp_file_history_next)
    except FileNotFoundError:
        tmp_file_history_next = []

    # Part 3: Append failed task (identified by comparison with job
    # task_list
    job_wftasks = workflow.task_list[
        first_task_index : (last_task_index + 1)  # type: ignore  # noqa
    ]
    tmp_file_wftasks = [
        history_item["workflowtask"] for history_item in tmp_file_history_next
    ]
    if len(job_wftasks) < len(tmp_file_wftasks):
        logger.error(
            "SOMETHING WENT WRONG AND HISTORY WAS NOT UPDATED CORRECTLY"  # FIXME # noqa
        )
    else:
        failed_wftask = job_wftasks[len(tmp_file_wftasks)]
        failed_wftask_dump = failed_wftask.dict(exclude={"task"})
        failed_wftask_dump["task"] = failed_wftask.task.dict()
        new_history_item = dict(
            workflowtask=failed_wftask_dump,
            status=WorkflowTaskStatusType.FAILED,
            parallelization=dict(
                parallelization_level=failed_wftask.parallelization_level,
            ),
        )
        new_history_next.append(new_history_item)

    return new_history_next
