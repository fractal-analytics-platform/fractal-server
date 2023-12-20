"""
Loop over jobs.
If the corresponing project still exists, set the project_dump.
"""
import logging

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from fractal_server.app.db import get_sync_db
from fractal_server.app.models.job import ApplyWorkflow
from fractal_server.app.models.project import Project
from fractal_server.app.schemas.applyworkflow import ApplyWorkflowRead
from fractal_server.app.schemas.applyworkflow import ProjectDump

with next(get_sync_db()) as db:

    # Get list of jobs
    stm = select(ApplyWorkflow)
    applyworkflows = db.execute(stm)
    rows = applyworkflows.scalars().all()

    # Loop over jobs
    for row in sorted(rows, key=lambda x: x.id):
        if row.project_dump != {}:
            logging.warning(
                f"[Job {row.id:4d}] project_dump attribute non-empty, skip"
            )
        else:
            # protects from overriding
            if row.project_id is None:
                logging.warning(
                    f"[Job {row.id:4d}] project_id=None, use dummy data"
                )
                project_dump = dict(
                    id=-1, name="__UNDEFINED__", read_only=True
                )
            else:
                project = db.get(Project, row.project_id)
                if project is None:
                    raise IntegrityError(
                        f"[Job {row.id:4d}] "
                        f"project_id={row.project_id}, "
                        f"but Project {row.project_id} does not exist"
                    )
                project_dump = project.dict(exclude={"user_list"})

            logging.warning(f"[Job {row.id:4d}] setting {project_dump=}")
            ProjectDump(**project_dump)
            row.project_dump = project_dump
            db.add(row)
            db.commit()

            # Also validate that the row can be cast into ApplyWorkflowRead
            db.refresh(row)
            db.expunge(row)
            ApplyWorkflowRead(**row.dict())
