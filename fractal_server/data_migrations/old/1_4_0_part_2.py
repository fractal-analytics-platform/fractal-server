"""
Loop over jobs, check that the corresponding project exists and is linked to a
single user, then set the user_email job attribute.
"""
import logging

from sqlalchemy import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v1.job import ApplyWorkflow
from fractal_server.app.models.v1.project import Project


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

        # Check that project is linked to a single user
        n_users = len(project.user_list)
        if n_users != 1:
            logging.warning(
                f"Skip job {row.id}, since project {row.project_id} "
                f"is linked to {n_users} users"
            )
            continue

        # Set job's user_email attribute
        user_email = project.user_list[0].email
        logging.warning(
            f"Handling job with ID {row.id:4d}, setting {user_email=}"
        )
        row.user_email = user_email
        db.add(row)
        db.commit()
