"""
Loop over jobs.
If the corresponing project still exists, set the project_dump.
"""
import logging

from sqlalchemy import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models.job import ApplyWorkflow
from fractal_server.app.models.project import Project

with next(get_sync_db()) as db:

    # Get list of jobs
    stm = select(ApplyWorkflow)
    applyworkflows = db.execute(stm)
    rows = applyworkflows.scalars().all()

    # Loop over jobs
    for row in sorted(rows, key=lambda x: x.id):
        # Check that job is linked to a project
        if row.project_id is None:
            logging.warning(f"Skip job {row.id}, since it has project_id=None")
            continue
        project = db.get(Project, row.project_id)
        if project is None:
            logging.error(
                f"Skip job {row.id}, since project with ID "
                f"{row.project_id} is None"
            )

        # Set job's project_dump attribute
        project_dump = project.dict(
            exclude={
                "user_list",
                "dataset_list",
                "workflow_list",
                "job_list",
            }
        )

        logging.warning(
            f"Handling job with ID {row.id:4d}, setting {project_dump=}"
        )
        row.project_dump = project_dump
        db.add(row)
        db.commit()
