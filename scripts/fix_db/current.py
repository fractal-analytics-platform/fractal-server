"""
Loop over jobs.
If the corresponding project still exists, set the project_dump.
"""
import json
import logging
from datetime import datetime

from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from fractal_server.app.db import get_sync_db
from fractal_server.app.models.job import ApplyWorkflow
from fractal_server.app.models.project import Project
from fractal_server.app.schemas.applyworkflow import ApplyWorkflowRead
from fractal_server.app.schemas.applyworkflow import ProjectDump
from fractal_server.app.schemas.project import ProjectRead

with next(get_sync_db()) as db:
    # Get list of all projects with their related job
    stm = (
        select(Project, func.min(ApplyWorkflow.start_timestamp))
        .select_from(Project)
        .join(ApplyWorkflow)
        .where(Project.id == ApplyWorkflow.project_id)
        .group_by(Project.id)
    )
    projects_timestamps = db.execute(stm).scalars().all()
    for project, min_job_timestamp in projects_timestamps:
        if project.timestamp_created == str(datetime(1, 1, 1, 0, 0, 0, 0)):
            project.timestamp_created = min_job_timestamp
            db.add(project)
    db.commit()
    for project, _ in projects_timestamps:
        db.refresh(project)
        db.expunge(project)
        ProjectRead(**project.dict())

    # Get list of jobs
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
                    timestamp_created=datetime(1, 1, 1, 0, 0, 0),
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
            ApplyWorkflowRead(**job.dict())
