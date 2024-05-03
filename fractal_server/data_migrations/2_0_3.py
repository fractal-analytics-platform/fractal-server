import logging

from packaging.version import parse
from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified

import fractal_server
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v1 import ApplyWorkflow
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.schemas.v1 import ApplyWorkflowReadV1
from fractal_server.app.schemas.v2 import JobReadV2


def fix_db():
    logger = logging.getLogger("fix_db")
    logger.warning("START execution of fix_db function")

    # Check that this module matches with the current version
    module_version = parse("2.0.3")
    current_version = parse(fractal_server.__VERSION__)
    if (
        current_version.major != module_version.major
        or current_version.minor != module_version.minor
        or current_version.micro != module_version.micro
    ):
        raise RuntimeError(
            f"{fractal_server.__VERSION__=} not matching with {__file__=}"
        )

    with next(get_sync_db()) as db:

        # V1 jobs
        stm = select(ApplyWorkflow)
        jobs_v1 = db.execute(stm).scalars().all()
        for job_v1 in sorted(jobs_v1, key=lambda x: x.id):
            for KEY in ["history"]:
                logger.warning(
                    f"Now removing {KEY} from `input/output_dataset_dump`, "
                    f"for appplyworkflow.id={job_v1.id}."
                )
                if KEY in job_v1.input_dataset_dump.keys():
                    job_v1.input_dataset_dump.pop(KEY)
                if KEY in job_v1.output_dataset_dump.keys():
                    job_v1.output_dataset_dump.pop(KEY)
            flag_modified(job_v1, "input_dataset_dump")
            flag_modified(job_v1, "output_dataset_dump")
            db.add(job_v1)
            db.commit()
            db.refresh(job_v1)
            db.expunge(job_v1)
            logger.warning(
                f"Now validating applyworkflow.id={job_v1.id} with "
                "ApplyWorkflowReadV1."
            )
            ApplyWorkflowReadV1(**job_v1.model_dump())

        # V2 jobs
        stm = select(JobV2)
        jobs_v2 = db.execute(stm).scalars().all()
        for job_v2 in sorted(jobs_v2, key=lambda x: x.id):
            for KEY in ["history", "images"]:
                logger.warning(
                    f"Now removing {KEY} from `dataset_dump`, "
                    f"for jobv2.id={job_v2.id}."
                )
                if KEY in job_v2.dataset_dump.keys():
                    job_v2.dataset_dump.pop(KEY)
            flag_modified(job_v2, "dataset_dump")
            db.add(job_v2)
            db.commit()
            db.refresh(job_v2)
            db.expunge(job_v2)
            logger.warning(
                f"Now validating jobv2.id={job_v2.id} with JobReadV2."
            )
            JobReadV2(**job_v2.model_dump())

    logger.warning("END of execution of fix_db function")
