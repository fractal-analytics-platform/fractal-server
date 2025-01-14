import logging

from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import DatasetV2
from fractal_server.app.models import JobV2
from fractal_server.app.models import WorkflowTaskV2

logger = logging.getLogger("fix_db")
logger.setLevel(logging.INFO)


def fix_db():

    logger.info("START execution of fix_db function")

    with next(get_sync_db()) as db:

        # DatasetV2.filters
        # DatasetV2.history[].workflowtask.input_filters
        stm = select(DatasetV2).order_by(DatasetV2.id)
        datasets = db.execute(stm).scalars().all()
        for ds in datasets:
            ds.attribute_filters = ds.filters["attributes"]
            ds.type_filters = ds.filters["types"]
            ds.filters = None
            for i, h in enumerate(ds.history):
                ds.history[i]["workflowtask"]["type_filters"] = h[
                    "workflowtask"
                ]["input_filters"]["types"]
            flag_modified(ds, "history")
            db.add(ds)
            logger.info(f"Fixed filters in DatasetV2[{ds.id}]")

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
            db.add(wft)
            logger.info(f"Fixed filters in WorkflowTaskV2[{wft.id}]")

        # JOBS V2
        stm = select(JobV2).order_by(JobV2.id)
        jobs = db.execute(stm).scalars().all()
        for job in jobs:
            job.dataset_dump["type_filters"] = job.dataset_dump["filters"][
                "types"
            ]
            job.dataset_dump["attribute_filters"] = job.dataset_dump[
                "filters"
            ]["attributes"]
            job.dataset_dump.pop("filters")
            flag_modified(job, "dataset_dump")
            logger.info(f"Fixed filters in JobV2[{job.id}].datasetdump")

        db.commit()
        logger.info("Changes committed.")
