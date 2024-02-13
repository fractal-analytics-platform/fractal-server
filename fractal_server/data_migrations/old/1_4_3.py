import logging
from datetime import datetime
from datetime import timezone

from packaging.version import parse
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

import fractal_server
from fractal_server.app.db import get_sync_db
from fractal_server.app.models import ApplyWorkflow
from fractal_server.app.models import Dataset
from fractal_server.app.models import Project
from fractal_server.app.models import Workflow
from fractal_server.app.schemas import ApplyWorkflowRead
from fractal_server.app.schemas import WorkflowRead
from fractal_server.app.schemas.dataset import DatasetRead
from fractal_server.app.schemas.dumps import DatasetDump
from fractal_server.app.schemas.dumps import WorkflowDump


REFERENCE_TIMESTAMP = datetime(2000, 1, 1, tzinfo=timezone.utc)


def fix_db():
    logger = logging.getLogger("fix_db")
    logger.warning("START execution of fix_db function")

    # Check that this module matches with the current version
    module_version = parse("1.4.3")
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

        # add timestamp_created to Workflows
        stm = select(Workflow)
        workflows = db.execute(stm).scalars().all()
        for workflow in sorted(workflows, key=lambda x: x.id):
            # add timestamp_created to Workflows
            timestamp_created = workflow.timestamp_created
            if timestamp_created != REFERENCE_TIMESTAMP:
                logger.warning(
                    f"[Workflow {workflow.id:4d}] {timestamp_created=} -> skip"
                )
            else:
                logger.warning(
                    f"[Workflow {workflow.id:4d}] {timestamp_created=} -> "
                    "replace with project timestamp."
                )
                project = db.get(Project, workflow.project_id)
                if project is None:
                    raise IntegrityError(
                        f"[Workflow {workflow.id:4d}] "
                        f"project_id={workflow.project_id}, "
                        f"but Project {workflow.project_id} does not exist"
                    )
                new_timestamp = project.timestamp_created
                logger.warning(
                    f"[Workflow {workflow.id:4d}] New value: {new_timestamp=}"
                )
                workflow.timestamp_created = new_timestamp
                db.add(workflow)
                db.commit()
                db.refresh(workflow)
                db.expunge(workflow)
                WorkflowRead(
                    **workflow.model_dump(exclude={"task_list", "project"}),
                    task_list=workflow.task_list,
                    project=workflow.project,
                )

        # add timestamp_created to Dataset
        stm = select(Dataset)
        datasets = db.execute(stm).scalars().all()
        for dataset in sorted(datasets, key=lambda x: x.id):
            # add timestamp_created to Datasets
            timestamp_created = dataset.timestamp_created
            if timestamp_created != REFERENCE_TIMESTAMP:
                logger.warning(
                    f"[Dataset {dataset.id:4d}] {timestamp_created=} -> skip."
                )
            else:
                logger.warning(
                    f"[Dataset {dataset.id:4d}] {timestamp_created=} -> "
                    "replace with project timestamp."
                )
                project = db.get(Project, dataset.project_id)
                if project is None:
                    raise IntegrityError(
                        f"[Dataset {dataset.id:4d}] "
                        f"project_id={dataset.project_id}, "
                        f"but Project {dataset.project_id} does not exist"
                    )
                new_timestamp = project.timestamp_created
                logger.warning(
                    f"[Dataset {dataset.id:4d}] New value: {new_timestamp=}"
                )
                dataset.timestamp_created = new_timestamp
                db.add(dataset)
                db.commit()
                db.refresh(dataset)
                db.expunge(dataset)
                DatasetRead(
                    **dataset.model_dump(exclude={"resource_list", "project"}),
                    resource_list=dataset.resource_list,
                    project=dataset.project,
                )

        # Add timestamp_created to job attributes workflow_dump,
        # input_dataset_dump, output_dataset_dump
        stm = select(ApplyWorkflow)
        jobs = db.execute(stm).scalars().all()
        for job in sorted(jobs, key=lambda x: x.id):
            project_timestamp = None
            if job.project_id is not None:
                project = db.get(Project, job.project_id)
                if project is None:
                    raise IntegrityError(
                        f"[Job {job.id:4d}] "
                        f"project_id={job.project_id}, "
                        f"but Project {job.project_id} does not exist"
                    )
                project_timestamp = project.timestamp_created

            # WORKFLOW DUMP
            workflow_dump_timestamp = job.workflow_dump.get(
                "timestamp_created"
            )
            if workflow_dump_timestamp is not None:
                logger.warning(
                    f"[Job {job.id:4d}] -> workflow_dump['timestamp_created'] "
                    f" = {workflow_dump_timestamp} -> SKIP"
                )
            else:  # workflow_dump_timestamp is None
                if project_timestamp is not None:
                    # if Job.Project exists
                    new_timestamp = project_timestamp
                    logger.warning(
                        f"[Job {job.id:4d}] workflow_dump['timestamp_created']"
                        f"={workflow_dump_timestamp} -> replace it with "
                        "Project "
                        f"{job.project_id} timestamp -> {new_timestamp}"
                    )
                else:
                    # if Job.Project doesn't exist
                    logger.warning(
                        f"[Job {job.id:4d}] workflow_dump['timestamp_created']"
                        f"={workflow_dump_timestamp} AND project_id is None "
                        "-> replace it with reference timestamp "
                        f"{REFERENCE_TIMESTAMP}"
                    )
                    new_timestamp = REFERENCE_TIMESTAMP
                # add Job.workflow_dump.timestamp_created
                new_workflow_dump = job.workflow_dump.copy()
                new_workflow_dump.update(
                    {"timestamp_created": str(new_timestamp)}
                )
                job.workflow_dump = new_workflow_dump
                db.add(job)
                db.commit()
                db.refresh(job)

            # INPUT DATASET DUMP
            ids_dump_timestamp = job.input_dataset_dump.get(
                "timestamp_created"
            )
            if ids_dump_timestamp is not None:
                logger.warning(
                    f"[Job {job.id:4d}] -> "
                    "input_dataset_dump['timestamp_created'] "
                    f" = {ids_dump_timestamp} -> SKIP"
                )
            else:  # ids_dump_timestamp is None
                if project_timestamp is not None:
                    # if Job.Project exists
                    new_timestamp = project_timestamp
                    logger.warning(
                        f"[Job {job.id:4d}] "
                        "input_dataset_dump['timestamp_created']="
                        f"{ids_dump_timestamp} -> replace it with "
                        f"Project {job.project_id} timestamp -> "
                        f"{new_timestamp}"
                    )
                else:
                    # if Job.Project doesn't exist
                    logger.warning(
                        f"[Job {job.id:4d}] "
                        f"input_dataset_dump['timestamp_created']="
                        f"{ids_dump_timestamp} AND project_id is None -> "
                        "replace it with reference timestamp "
                        f"{REFERENCE_TIMESTAMP}"
                    )
                    new_timestamp = REFERENCE_TIMESTAMP
                # add Job.input_dataset_dump.timestamp_created
                new_ids_dump = job.input_dataset_dump.copy()
                new_ids_dump.update({"timestamp_created": str(new_timestamp)})
                job.input_dataset_dump = new_ids_dump
                db.add(job)
                db.commit()
                db.refresh(job)

            # OUTPUT DATASET DUMP
            ods_dump_timestamp = job.output_dataset_dump.get(
                "timestamp_created"
            )
            if ods_dump_timestamp is not None:
                logger.warning(
                    f"[Job {job.id:4d}] -> "
                    "output_dataset_dump['timestamp_created'] "
                    f" = {ods_dump_timestamp} -> SKIP"
                )
            else:  # ods_dump_timestamp is None
                if project_timestamp is not None:
                    # if Job.Project exists
                    new_timestamp = project_timestamp
                    logger.warning(
                        f"[Job {job.id:4d}] "
                        "output_dataset_dump['timestamp_created']="
                        f"{ods_dump_timestamp} -> replace it with "
                        f"Project {job.project_id} timestamp -> "
                        f"{new_timestamp}"
                    )
                else:
                    # if Job.Project doesn't exist
                    logger.warning(
                        f"[Job {job.id:4d}] "
                        f"output_dataset_dump['timestamp_created']="
                        f"{ods_dump_timestamp} AND project_id is None -> "
                        "replace it with reference timestamp "
                        f"{REFERENCE_TIMESTAMP}"
                    )
                    new_timestamp = REFERENCE_TIMESTAMP
                # add Job.output_dataset_dump.timestamp_created
                new_ods_dump = job.output_dataset_dump.copy()
                new_ods_dump.update({"timestamp_created": str(new_timestamp)})
                job.output_dataset_dump = new_ods_dump
                db.add(job)
                db.commit()
                db.refresh(job)
            db.expunge(job)
            WorkflowDump(**job.workflow_dump)
            DatasetDump(**job.input_dataset_dump)
            DatasetDump(**job.output_dataset_dump)
            ApplyWorkflowRead(**job.model_dump())

    logger.warning("END of execution of fix_db function")
