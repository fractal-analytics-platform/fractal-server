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

from sqlalchemy.orm.attributes import flag_modified

from ...models.v2 import DatasetV2
from ...schemas.v2 import WorkflowTaskStatusTypeV2
from fractal_server.app.db import get_sync_db


def mark_last_wftask_as_failed(
    dataset_id: int,
    logger_name: str,
) -> None:
    """
    Edit dataset history, by marking last item as failed.

    Args:
        dataset: The `DatasetV2` object
        logger_name: A logger name.
    """

    logger = logging.getLogger(logger_name)
    with next(get_sync_db()) as db:
        db_dataset = db.get(DatasetV2, dataset_id)
        if len(db_dataset.history) == 0:
            logger.warning(
                f"History for {dataset_id=} is empty. Likely reason: the job "
                "failed before its first task was marked as SUBMITTED. "
                "Continue."
            )
            return
        workflowtask_id = db_dataset.history[-1]["workflowtask"]["id"]
        last_item_status = db_dataset.history[-1]["status"]
        if last_item_status != WorkflowTaskStatusTypeV2.SUBMITTED:
            logger.warning(
                "Unexpected branch: "
                f"Last history item, for {workflowtask_id=}, "
                f"has status {last_item_status}. Skip."
            )
            return
        logger.info(f"Setting history item for {workflowtask_id=} to failed.")
        db_dataset.history[-1]["status"] = WorkflowTaskStatusTypeV2.FAILED
        flag_modified(db_dataset, "history")
        db.merge(db_dataset)
        db.commit()
