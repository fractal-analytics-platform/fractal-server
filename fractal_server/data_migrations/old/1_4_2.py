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
from fractal_server.app.models.v1.job import ApplyWorkflow
from fractal_server.app.models.v1.project import Project
from fractal_server.app.schemas.v1.applyworkflow import ApplyWorkflowReadV1
from fractal_server.app.schemas.v1.dumps import ProjectDumpV1
from fractal_server.app.schemas.v1.project import ProjectReadV1


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
            ProjectReadV1(**project.model_dump())

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
            ProjectDumpV1(**project_dump)
            job.project_dump = project_dump
            db.add(job)
            db.commit()

            # Also validate that the row can be cast into ApplyWorkflowRead
            db.refresh(job)
            db.expunge(job)
            ApplyWorkflowReadV1(**job.model_dump())
