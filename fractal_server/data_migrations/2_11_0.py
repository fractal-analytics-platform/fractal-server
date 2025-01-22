import logging
from typing import Union

from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import DatasetV2
from fractal_server.app.models import JobV2
from fractal_server.app.models import WorkflowTaskV2
from fractal_server.app.schemas.v2 import DatasetReadV2
from fractal_server.app.schemas.v2 import JobReadV2
from fractal_server.app.schemas.v2 import ProjectReadV2
from fractal_server.app.schemas.v2 import TaskReadV2
from fractal_server.app.schemas.v2 import WorkflowTaskReadV2

logger = logging.getLogger("fix_db")
logger.setLevel(logging.INFO)


def dict_values_to_list(
    input_dict: dict[str, Union[int, float, bool, str, None]], identifier: str
) -> dict[str, list[Union[int, float, bool, str]]]:
    for k, v in input_dict.items():
        if not isinstance(v, (int, float, bool, str, type(None))):
            error_msg = (
                f"Attribute '{k}' from '{identifier}' has type '{type(v)}'."
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        elif v is None:
            logger.warning(
                f"Attribute '{k}' from '{identifier}' is None and it will be "
                "removed."
            )
        else:
            input_dict[k] = [v]
    return input_dict


def fix_db():

    logger.info("START execution of fix_db function")

    with next(get_sync_db()) as db:

        # DatasetV2.filters
        # DatasetV2.history[].workflowtask.input_filters
        stm = select(DatasetV2).order_by(DatasetV2.id)
        datasets = db.execute(stm).scalars().all()
        for ds in datasets:
            try:
                ds.attribute_filters = dict_values_to_list(
                    ds.filters["attributes"],
                    f"Dataset[{ds.id}].filters.attributes",
                )
                ds.type_filters = ds.filters["types"]
                ds.filters = None
                for i, h in enumerate(ds.history):
                    ds.history[i]["workflowtask"]["type_filters"] = h[
                        "workflowtask"
                    ]["input_filters"]["types"]
                flag_modified(ds, "history")
                DatasetReadV2(
                    project=ProjectReadV2(**ds.project.model_dump()),
                    **ds.model_dump(),
                )
                db.add(ds)
                logger.info(f"Fixed filters in DatasetV2[{ds.id}]")
            except RuntimeError as e:
                logger.info(f"Skipping DatasetV2[{ds.id}]. Error: '{e}'.")

        # WorkflowTaskV2.input_filters
        stm = select(WorkflowTaskV2).order_by(WorkflowTaskV2.id)
        wftasks = db.execute(stm).scalars().all()
        for wft in wftasks:
            wft.type_filters = wft.input_filters["types"]
            if wft.input_filters["attributes"]:
                logger.warning(
                    f"Removing WorkflowTaskV2[{wft.id}].input_filters"
                    f"['attributes'] = {wft.input_filters['attributes']}"
                )
            wft.input_filters = None
            flag_modified(wft, "input_filters")
            WorkflowTaskReadV2(
                task=TaskReadV2(**wft.task.model_dump()), **wft.model_dump()
            )
            db.add(wft)
            logger.info(f"Fixed filters in WorkflowTaskV2[{wft.id}]")

        # JOBS V2
        stm = select(JobV2).order_by(JobV2.id)
        jobs = db.execute(stm).scalars().all()
        for job in jobs:
            job.dataset_dump["type_filters"] = job.dataset_dump["filters"][
                "types"
            ]
            job.dataset_dump["attribute_filters"] = dict_values_to_list(
                job.dataset_dump["filters"]["attributes"],
                f"JobV2[{job.id}].dataset_dump.filters.attributes",
            )
            job.dataset_dump.pop("filters")
            flag_modified(job, "dataset_dump")
            JobReadV2(**job.model_dump())
            db.add(job)
            logger.info(f"Fixed filters in JobV2[{job.id}].datasetdump")

        db.commit()
        logger.info("Changes committed.")

    logger.info("END execution of fix_db function")
