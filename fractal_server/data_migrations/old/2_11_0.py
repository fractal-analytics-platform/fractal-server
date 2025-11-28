import logging

from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import DatasetV2
from fractal_server.app.models import JobV2
from fractal_server.app.models import ProjectV2
from fractal_server.app.models import WorkflowTaskV2
from fractal_server.app.models import WorkflowV2
from fractal_server.app.schemas.v2 import DatasetRead
from fractal_server.app.schemas.v2 import JobRead
from fractal_server.app.schemas.v2 import ProjectRead
from fractal_server.app.schemas.v2 import TaskRead
from fractal_server.app.schemas.v2 import WorkflowTaskRead
from fractal_server.types import AttributeFilters

logger = logging.getLogger("fix_db")
logger.setLevel(logging.INFO)


def dict_values_to_list(
    input_dict: dict[str, int | float | bool | str | None],
    identifier: str,
) -> tuple[AttributeFilters, bool]:
    was_there_a_warning = False
    for k, v in input_dict.items():
        if not isinstance(v, (int, float, bool, str, type(None))):
            error_msg = (
                f"Attribute '{k}' from '{identifier}' "
                "has invalid type '{type(v)}'."
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        elif v is None:
            logger.warning(
                f"Attribute '{k}' from '{identifier}' is "
                "None and it will be removed."
            )
            was_there_a_warning = True
        else:
            input_dict[k] = [v]
    return input_dict, was_there_a_warning


def fix_db():
    logger.info("START execution of fix_db function")

    with next(get_sync_db()) as db:
        # DatasetV2.filters
        stm = select(DatasetV2).order_by(DatasetV2.id)
        datasets = db.execute(stm).scalars().all()
        for ds in datasets:
            logger.info(f"DatasetV2[{ds.id}] START")
            if ds.filters is None:
                logger.info(f"DatasetV2[{ds.id}] SKIP")
                continue

            ds.attribute_filters, warning = dict_values_to_list(
                ds.filters["attributes"],
                f"Dataset[{ds.id}].filters.attributes",
            )
            if warning:
                proj = db.get(ProjectV2, ds.project_id)
                logger.warning(
                    "Additional information: "
                    f"{proj.id=}, "
                    f"{proj.name=}, "
                    f"{proj.user_list[0].email=}, "
                    f"{ds.id=}, "
                    f"{ds.name=}"
                )
            ds.type_filters = ds.filters["types"]
            ds.filters = None
            for i, h in enumerate(ds.history):
                ds.history[i]["workflowtask"]["type_filters"] = h[
                    "workflowtask"
                ]["input_filters"]["types"]
                ds.history[i]["workflowtask"].pop("input_filters")
            flag_modified(ds, "history")
            DatasetRead(
                **ds.model_dump(),
                project=ProjectRead(**ds.project.model_dump()),
            )
            db.add(ds)
            logger.info(f"DatasetV2[{ds.id}] END - fixed filters")

        logger.info("------ switch from dataset to workflowtasks ------")

        # WorkflowTaskV2.input_filters
        stm = select(WorkflowTaskV2).order_by(WorkflowTaskV2.id)
        wftasks = db.execute(stm).scalars().all()
        for wft in wftasks:
            logger.info(f"WorkflowTaskV2[{wft.id}] START")
            if wft.input_filters is None:
                logger.info(f"WorkflowTaskV2[{wft.id}] SKIP")
                continue
            wft.type_filters = wft.input_filters["types"]
            if wft.input_filters["attributes"]:
                logger.warning(
                    "Removing input_filters['attributes']. "
                    f"(previous value: {wft.input_filters['attributes']})"
                )
                wf = db.get(WorkflowV2, wft.workflow_id)
                proj = db.get(ProjectV2, wf.project_id)
                logger.warning(
                    "Additional information: "
                    f"{proj.id=}, "
                    f"{proj.name=}, "
                    f"{proj.user_list[0].email=}, "
                    f"{wf.id=}, "
                    f"{wf.name=}, "
                    f"{wft.task.name=}"
                )
            wft.input_filters = None
            flag_modified(wft, "input_filters")
            WorkflowTaskRead(
                **wft.model_dump(),
                task=TaskRead(**wft.task.model_dump()),
            )
            db.add(wft)
            logger.info(f"WorkflowTaskV2[{wft.id}] END - fixed filters")

        logger.info("------ switch from workflowtasks to jobs ------")

        # JOBS V2
        stm = select(JobV2).order_by(JobV2.id)
        jobs = db.execute(stm).scalars().all()
        for job in jobs:
            logger.info(f"JobV2[{job.id}] START")
            if "filters" not in job.dataset_dump.keys():
                logger.info(f"JobV2[{job.id}] SKIP")
                continue
            job.dataset_dump["type_filters"] = job.dataset_dump["filters"][
                "types"
            ]
            (
                job.dataset_dump["attribute_filters"],
                warning,
            ) = dict_values_to_list(
                job.dataset_dump["filters"]["attributes"],
                f"JobV2[{job.id}].dataset_dump.filters.attributes",
            )
            if warning and job.project_id is not None:
                proj = db.get(ProjectV2, job.project_id)
                logger.warning(
                    "Additional information: "
                    f"{proj.id=}, "
                    f"{proj.name=}, "
                    f"{proj.user_list[0].email=}, "
                    f"{job.id=}, "
                    f"{job.start_timestamp=}, "
                    f"{job.end_timestamp=}, "
                    f"{job.dataset_id=}, "
                    f"{job.workflow_id=}."
                )
            job.dataset_dump.pop("filters")
            flag_modified(job, "dataset_dump")
            JobRead(**job.model_dump())
            db.add(job)
            logger.info(f"JobV2[{job.id}] END - fixed filters")

        db.commit()
        logger.info("Changes committed.")

    logger.info("END execution of fix_db function")
