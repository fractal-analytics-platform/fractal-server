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
import logging
from typing import Optional

from sqlalchemy.orm.attributes import flag_modified

from ...models.v2 import DatasetV2
from ...models.v2 import JobV2
from ...models.v2 import WorkflowTaskV2
from ...models.v2 import WorkflowV2
from ...schemas.v2 import WorkflowTaskStatusTypeV2
from fractal_server.app.db import get_sync_db


def assemble_history_failed_job(
    job: JobV2,
    dataset: DatasetV2,
    workflow: WorkflowV2,
    logger_name: Optional[str] = None,
    failed_wftask: Optional[WorkflowTaskV2] = None,
) -> None:
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

    with next(get_sync_db()) as db:
        db_dataset = db.get(DatasetV2, dataset.id)

        job_wftasks = workflow.task_list[
            job.first_task_index : (job.last_task_index + 1)  # noqa
        ]
        # Part 1/A: Identify failed task, if needed
        if failed_wftask is None:
            tmp_file_wftasks = [
                history_item["workflowtask"]
                for history_item in db_dataset.history
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

        # Part 1/B: Append failed task to history
        if failed_wftask is not None:
            failed_wftask_dump = failed_wftask.model_dump(exclude={"task"})
            failed_wftask_dump["task"] = failed_wftask.task.model_dump()

            for ind, history_item in enumerate(db_dataset.history):

                if (
                    history_item["workflowtask"]["task"]["id"]
                    == failed_wftask_dump["task"]["id"]
                ):
                    history_item["status"] = WorkflowTaskStatusTypeV2.FAILED
                    db_dataset.history[ind] = history_item
                    flag_modified(db_dataset, "history")
                    db.merge(db_dataset)
                    db.commit()
                    break

                else:
                    pass
