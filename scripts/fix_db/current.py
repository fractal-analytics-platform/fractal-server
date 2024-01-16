import logging
from datetime import datetime
from datetime import timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import ApplyWorkflow
from fractal_server.app.models import Dataset
from fractal_server.app.models import Project
from fractal_server.app.models import Workflow
from fractal_server.app.schemas import ApplyWorkflowRead
from fractal_server.app.schemas.dumps import DatasetDump
from fractal_server.app.schemas.dumps import WorkflowDump

# from fractal_server.app.schemas import WorkflowRead
# from fractal_server.app.schemas.dataset import DatasetRead


REFERENCE_TIMESTAMP = datetime(2000, 1, 1, tzinfo=timezone.utc)

with next(get_sync_db()) as db:

    # add timestamp_created to Workflows
    stm = select(Workflow)
    workflows = db.execute(stm).scalars().all()
    for workflow in workflows:
        # add timestamp_created to Workflows
        timestamp_created = workflow.timestamp_created
        if timestamp_created != REFERENCE_TIMESTAMP:
            logging.warning(
                f"[Workflow {workflow.id:4d}] {timestamp_created=} -> skip."
            )
        else:
            logging.warning(
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
            logging.warning(
                f"[Workflow {workflow.id:4d}] New value: {new_timestamp=}"
            )
            workflow.timestamp_created = new_timestamp
            db.add(workflow)
            db.commit()
            db.refresh(workflow)
            db.expunge(workflow)
            # WorkflowRead(**workflow.model_dump())

    # add timestamp_created to Dataset
    stm = select(Dataset)
    datasets = db.execute(stm).scalars().all()
    for dataset in datasets:
        # add timestamp_created to Datasets
        timestamp_created = dataset.timestamp_created
        if timestamp_created != REFERENCE_TIMESTAMP:
            logging.warning(
                f"[Dataset {dataset.id:4d}] {timestamp_created=} -> skip."
            )
        else:
            logging.warning(
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
            logging.warning(
                f"[Dataset {dataset.id:4d}] New value: {new_timestamp=}"
            )
            dataset.timestamp_created = new_timestamp
            db.add(dataset)
            db.commit()
            db.refresh(dataset)
            db.expunge(dataset)
            # DatasetRead(**dataset.model_dump())

    # add timestamp_created to Job.workflow_dump and Job.in/output_dataset_dump
    stm = select(ApplyWorkflow)
    jobs = db.execute(stm).scalars().all()
    for job in jobs:

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
        workflow_dump_timestamp = job.workflow_dump.get("timestamp_created")
        if workflow_dump_timestamp is not None:
            logging.warning(
                f"[Job {job.id:4d}] -> Job.workflow_dump['timestamp_created'] "
                f" = {workflow_dump_timestamp} -> SKIP"
            )
        else:  # workflow_dump_timestamp is None
            if project_timestamp is not None:
                # if Job.Project exists
                new_timestamp = project_timestamp
                logging.warning(
                    f"[Job {job.id:4d}] Job.workflow_dump['timestamp_created']"
                    f"={workflow_dump_timestamp} -> replace it with Project "
                    f"{job.project_id} timestamp -> {new_timestamp}"
                )
            else:
                # if Job.Project doesn't exist
                logging.warning(
                    f"[Job {job.id:4d}] Job.workflow_dump['timestamp_created']"
                    f"={workflow_dump_timestamp} AND Job.project_id is None "
                    "-> replace it with reference timestamp "
                    f"{REFERENCE_TIMESTAMP}"
                )
                new_timestamp = REFERENCE_TIMESTAMP
            # add Job.workflow_dump.timestamp_created
            new_workflow_dump = job.workflow_dump.copy()
            new_workflow_dump.update({"timestamp_created": str(new_timestamp)})
            job.workflow_dump = new_workflow_dump
            db.add(job)
            db.commit()
            db.refresh(job)

        # INPUT DATASET DUMP
        ids_dump_timestamp = job.input_dataset_dump.get("timestamp_created")
        if ids_dump_timestamp is not None:
            logging.warning(
                f"[Job {job.id:4d}] -> "
                "Job.input_dataset_dump['timestamp_created'] "
                f" = {ids_dump_timestamp} -> SKIP"
            )
        else:  # ids_dump_timestamp is None
            if project_timestamp is not None:
                # if Job.Project exists
                new_timestamp = project_timestamp
                logging.warning(
                    f"[Job {job.id:4d}] "
                    "Job.input_dataset_dump['timestamp_created']="
                    f"{ids_dump_timestamp} -> replace it with "
                    f"Project {job.project_id} timestamp -> {new_timestamp}"
                )
            else:
                # if Job.Project doesn't exist
                logging.warning(
                    f"[Job {job.id:4d}] "
                    f"Job.input_dataset_dump['timestamp_created']="
                    f"{ids_dump_timestamp} AND Job.project_id is None -> "
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
        ods_dump_timestamp = job.output_dataset_dump.get("timestamp_created")
        if ods_dump_timestamp is not None:
            logging.warning(
                f"[Job {job.id:4d}] -> "
                "Job.output_dataset_dump['timestamp_created'] "
                f" = {ods_dump_timestamp} -> SKIP"
            )
        else:  # ods_dump_timestamp is None
            if project_timestamp is not None:
                # if Job.Project exists
                new_timestamp = project_timestamp
                logging.warning(
                    f"[Job {job.id:4d}] "
                    "Job.output_dataset_dump['timestamp_created']="
                    f"{ods_dump_timestamp} -> replace it with "
                    f"Project {job.project_id} timestamp -> {new_timestamp}"
                )
            else:
                # if Job.Project doesn't exist
                logging.warning(
                    f"[Job {job.id:4d}] "
                    f"Job.output_dataset_dump['timestamp_created']="
                    f"{ods_dump_timestamp} AND Job.project_id is None -> "
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
