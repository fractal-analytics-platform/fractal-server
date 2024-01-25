from sqlalchemy import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import ApplyWorkflow
from fractal_server.app.models import Dataset
from fractal_server.app.models import Project
from fractal_server.app.models import Workflow
from fractal_server.app.schemas import ApplyWorkflowRead
from fractal_server.app.schemas import DatasetRead
from fractal_server.app.schemas import ProjectRead
from fractal_server.app.schemas import WorkflowRead


with next(get_sync_db()) as db:

    stm = select(Project)
    projects = db.execute(stm).scalars().all()
    for project in projects:
        ProjectRead(**project.model_dump())

    stm = select(Workflow)
    workflows = db.execute(stm).scalars().all()
    for workflow in workflows:
        WorkflowRead(**workflow.model_dump())

    stm = select(Dataset)
    datasets = db.execute(stm).scalars().all()
    for dataset in datasets:
        DatasetRead(**dataset.model_dump())

    stm = select(ApplyWorkflow)
    jobs = db.execute(stm).scalars().all()
    for job in jobs:
        ApplyWorkflowRead(**job.model_dump())
