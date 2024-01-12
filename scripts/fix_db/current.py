import logging
from datetime import datetime
from datetime import timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import ApplyWorkflow
from fractal_server.app.models import Project
from fractal_server.app.models import Workflow
from fractal_server.app.schemas.dumps import WorkflowDump

# from fractal_server.app.schemas import WorkflowRead

REFERENCE_TIMESTAMP = datetime(2000, 1, 1, tzinfo=timezone.utc)

with next(get_sync_db()) as db:

    # add timestamp_created to Workflow
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
            # Missing `task_list` and `project``
            # WorkflowRead(**workflow.model_dump())

        # add timestamp_created to Job.workflow_dump
        stm = select(ApplyWorkflow)
        jobs = db.execute(stm).scalars().all()
        for job in jobs:
            workflow_dump_timestamp = job.workflow_dump.get(
                "timestamp_created"
            )
            logging.warning(f"[Job {job.id:4d}] -> {workflow_dump_timestamp=}")
            if job.workflow_dump.get("timestamp_created") is None:
                # if Job.workflow_dump has no timestamp_created
                if job.project_id is not None:
                    # if Job.Project exists
                    project = db.get(Project, job.project_id)
                    if project is None:
                        raise IntegrityError(
                            f"[Job {job.id:4d}] "
                            f"project_id={job.project_id}, "
                            f"but Project {job.project_id} does not exist"
                        )
                    logging.warning(
                        f"[Job {job.id:4d}] "
                        f"{job.workflow_dump.get('timestamp_created')=} -> "
                        "replace with project timestamp."
                    )
                    new_timestamp = project.timestamp_created
                else:
                    # if Job.Project doesn't exist
                    logging.warning(
                        f"[Job {job.id:4d}] "
                        f"{job.workflow_dump.get('timestamp_created')=} "
                        f" and {job.project_id=} -> "
                        f"replace with {REFERENCE_TIMESTAMP=}."
                    )
                    new_timestamp = REFERENCE_TIMESTAMP
                # add Job.workflow_dump.timestamp_created
                new_workflow_dump = job.workflow_dump.copy()
                new_workflow_dump.update(
                    {"timestamp_created": str(timestamp_created)}
                )
                job.workflow_dump = new_workflow_dump
                db.add(job)
                db.commit()
                db.refresh(job)
                db.expunge(job)
                WorkflowDump(**job.workflow_dump)
