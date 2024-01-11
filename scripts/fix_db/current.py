"""
Loop over jobs.
If the corresponding project still exists, set the project_dump.
"""
import json
import logging
from datetime import datetime
from datetime import timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from fractal_server.app.db import get_sync_db
from fractal_server.app.models.dataset import Dataset
from fractal_server.app.models.job import ApplyWorkflow
from fractal_server.app.models.project import Project
from fractal_server.app.models.workflow import Workflow
from fractal_server.app.schemas.applyworkflow import ApplyWorkflowRead
from fractal_server.app.schemas.dataset import DatasetRead
from fractal_server.app.schemas.dumps import DatasetDump
from fractal_server.app.schemas.dumps import ProjectDump
from fractal_server.app.schemas.dumps import WorkflowDump
from fractal_server.app.schemas.project import ProjectRead
from fractal_server.app.schemas.workflow import WorkflowRead


REFERENCE_TIMESTAMP = datetime(2000, 1, 1, tzinfo=timezone.utc)
REFERENCE_TIMESTAMP_STRING = str(REFERENCE_TIMESTAMP)


with next(get_sync_db()) as db:
    # Get list of all projects with their related job
    stm = select(Project)
    projects = db.execute(stm).scalars().all()
    for project in projects:
        timestamp_created = project.timestamp_created
        if timestamp_created != REFERENCE_TIMESTAMP:
            logging.warning(
                f"[Project {project.id:4d}] {timestamp_created=} -> skip."
            )
        else:
            logging.warning(
                f"[Project {project.id:4d}] {timestamp_created=} -> "
                "replace with job timestamps."
            )
            stm = select(ApplyWorkflow).where(
                ApplyWorkflow.project_id == project.id
            )
            jobs = db.execute(stm).scalars().all()
            if len(jobs) == 0:
                logging.warning(
                    f"[Project {project.id:4d}] No jobs found, skip."
                )
                continue
            timestamp_created = min([job.start_timestamp for job in jobs])
            logging.warning(
                f"[Project {project.id:4d}] New value: {timestamp_created=}"
            )
            project.timestamp_created = timestamp_created
            db.add(project)
            db.commit()
            db.refresh(project)
            db.expunge(project)
            ProjectRead(**project.model_dump())

    # Workflow.timestamp_created
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
            timestamp_created = workflow.project.timestamp_created
            logging.warning(
                f"[Workflow {workflow.id:4d}] New value: {timestamp_created=}"
            )
            workflow.timestamp_created = timestamp_created
            db.add(workflow)
            db.commit()
            db.refresh(workflow)
            db.expunge(workflow)
            WorkflowRead(**workflow.model_dump())
        # add timestamp_created to Jobs.workflow_dump
        stm = select(ApplyWorkflow).where(
            ApplyWorkflow.workflow_dump["id"] == workflow.id
        )
        jobs = db.execute(stm).scalars().all()
        for job in jobs:
            job.workflow_dump["timestamp_created"] = str(timestamp_created)
            db.add(job)
            db.commit()
            db.refresh(job)
            db.expunge(job)
            WorkflowDump(**job.workflow_dump)

    # Dataset.timestamp_created
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
            timestamp_created = dataset.project.timestamp_created
            logging.warning(
                f"[Dataset {dataset.id:4d}] New value: {timestamp_created=}"
            )
            dataset.timestamp_created = timestamp_created
            db.add(dataset)
            db.commit()
            db.refresh(dataset)
            db.expunge(dataset)
            DatasetRead(**dataset.model_dump())

        # add timestamp_created to Jobs.input_dataset_dump
        stm = select(ApplyWorkflow).where(
            ApplyWorkflow.input_dataset_dump["id"] == dataset.id
        )
        jobs = db.execute(stm).scalars().all()
        for job in jobs:
            job.input_dataset_dump["timestamp_created"] = str(
                timestamp_created
            )
            db.add(job)
            db.commit()
            db.refresh(job)
            db.expunge(job)
            DatasetDump(**job.input_dataset_dump)
        # add timestamp_created to Jobs.output_dataset_dump
        stm = select(ApplyWorkflow).where(
            ApplyWorkflow.output_dataset_dump["id"] == dataset.id
        )
        jobs = db.execute(stm).scalars().all()
        for job in jobs:
            job.output_dataset_dump["timestamp_created"] = str(
                timestamp_created
            )
            db.add(job)
            db.commit()
            db.refresh(job)
            db.expunge(job)
            DatasetDump(**job.output_dataset_dump)

    # Get list of all jobs
    stm = select(ApplyWorkflow)
    res = db.execute(stm)
    jobs = res.scalars().all()

    # Loop over jobs
    for job in sorted(jobs, key=lambda x: x.id):
        if job.project_dump != {}:
            # Do not overwrite existing data
            logging.warning(
                f"[Job {job.id:4d}] project_dump attribute non-empty, skip"
            )
        else:
            if job.project_id is None:
                logging.warning(
                    f"[Job {job.id:4d}] project_id=None, use dummy data"
                )
                project_dump = dict(
                    id=-1,
                    name="__UNDEFINED__",
                    read_only=True,
                    timestamp_created=REFERENCE_TIMESTAMP_STRING,
                )
            else:
                project = db.get(Project, job.project_id)
                if project is None:
                    raise IntegrityError(
                        f"[Job {job.id:4d}] "
                        f"project_id={job.project_id}, "
                        f"but Project {job.project_id} does not exist"
                    )
                project_dump = json.loads(project.json(exclude={"user_list"}))

            logging.warning(f"[Job {job.id:4d}] setting {project_dump=}")
            ProjectDump(**project_dump)
            job.project_dump = project_dump
            db.add(job)
            db.commit()

            # Also validate that the row can be cast into ApplyWorkflowRead
            db.refresh(job)
            db.expunge(job)

            ApplyWorkflowRead(**job.model_dump())
