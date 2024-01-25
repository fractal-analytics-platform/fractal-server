from sqlalchemy import select
from sqlalchemy.orm import joinedload

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import Project
from fractal_server.app.models import Workflow
from fractal_server.app.schemas import ProjectRead
from fractal_server.app.schemas import WorkflowRead


with next(get_sync_db()) as db:

    stm = select(Project)
    projects = db.execute(stm).scalars().all()
    for project in projects:
        ProjectRead(**project.model_dump())

    stm = select(Workflow).options(
        joinedload(Workflow.project), joinedload(Workflow.task_list)
    )
    workflows = db.execute(stm).scalars().all()
    for workflow in workflows:
        WorkflowRead(**workflow.model_dump())
