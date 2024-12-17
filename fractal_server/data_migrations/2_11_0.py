import logging

from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import DatasetV2
from fractal_server.app.models import JobV2
from fractal_server.app.models import WorkflowTaskV2
from fractal_server.images import Filters

logger = logging.getLogger("fix_db")


def fix_db():

    logger.warning("START execution of fix_db function")

    with next(get_sync_db()) as db:

        # DatasetV2.filters
        # DatasetV2.history[].workflowtask.input_filters
        stm = select(DatasetV2).order_by(DatasetV2.id)
        datasets = db.execute(stm).scalars().all()
        for ds in datasets:
            ds.filters = Filters(
                attributes_include={
                    k: [v] for k, v in ds.filters["attributes"].items()
                },
                types=ds.filters["types"],
            ).dict()
            for i, h in enumerate(ds.history):
                ds.history[i]["workflowtask"]["input_filters"] = Filters(
                    attributes_include={
                        k: [v]
                        for k, v in h["workflowtask"]["input_filters"][
                            "attributes"
                        ].items()
                    },
                    types=h["workflowtask"]["input_filters"]["types"],
                ).dict()
            flag_modified(ds, "history")
            db.add(ds)
            logger.warning(f"Fixed filters in DatasetV2[{ds.id}]")

        # WorkflowTaskV2.input_filters
        stm = select(WorkflowTaskV2).order_by(WorkflowTaskV2.id)
        wftasks = db.execute(stm).scalars().all()
        for wft in wftasks:
            wft.input_filters = Filters(
                attributes_include={
                    k: [v] for k, v in wft.input_filters["attributes"].items()
                },
                types=wft.input_filters["types"],
            ).dict()
            db.add(wft)
            logger.warning(f"Fixed filters in WorkflowTaskV2[{ds.id}]")

        # JOBS V2
        stm = select(JobV2).order_by(JobV2.id)
        jobs = db.execute(stm).scalars().all()
        for job in jobs:
            job.dataset_dump["filters"] = Filters(
                attributes_include={
                    k: [v]
                    for k, v in job.dataset_dump["filters"][
                        "attributes"
                    ].items()
                },
                types=job.dataset_dump["filters"]["types"],
            ).dict()
            flag_modified(job, "dataset_dump")
            logger.warning(f"Fixed filters in JobV2[{ds.id}].datasetdump")

        db.commit()
        logger.warning("Changes committed.")
