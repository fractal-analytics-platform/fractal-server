from sqlalchemy import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import ApplyWorkflow
from fractal_server.app.models import Dataset
from fractal_server.app.models import Project
from fractal_server.app.models import State
from fractal_server.app.models import UserOAuth
from fractal_server.app.models import Workflow
from fractal_server.app.schemas import ApplyWorkflowRead
from fractal_server.app.schemas import DatasetRead
from fractal_server.app.schemas import ProjectRead
from fractal_server.app.schemas import ResourceRead
from fractal_server.app.schemas import StateRead
from fractal_server.app.schemas import TaskRead
from fractal_server.app.schemas import UserRead
from fractal_server.app.schemas import WorkflowRead
from fractal_server.app.schemas import WorkflowTaskRead

with next(get_sync_db()) as db:

    # PROJECTS
    stm = select(Project)
    projects = db.execute(stm).scalars().all()
    for project in sorted(projects, key=lambda x: x.id):
        ProjectRead(**project.model_dump())
        print(f"Project {project.id} validated")

    # WORKFLOWS
    stm = select(Workflow)
    workflows = db.execute(stm).scalars().all()
    for workflow in sorted(workflows, key=lambda x: x.id):
        WorkflowRead(
            **workflow.model_dump(),
            project=ProjectRead(**workflow.project.model_dump()),
            task_list=[
                WorkflowTaskRead(
                    **wftask.model_dump(),
                    task=TaskRead(**wftask.task.model_dump()),
                )
                for wftask in workflow.task_list
            ],
        )
        print(f"Workflow {workflow.id} validated")

    # DATASETS
    stm = select(Dataset)
    datasets = db.execute(stm).scalars().all()
    for dataset in sorted(datasets, key=lambda x: x.id):
        DatasetRead(
            **dataset.model_dump(),
            project=ProjectRead(**dataset.project.model_dump()),
            resource_list=[
                ResourceRead(**resource.model_dump())
                for resource in dataset.resource_list
            ],
        )
        print(f"Dataset {dataset.id} validated")

    # JOBS
    stm = select(ApplyWorkflow)
    jobs = db.execute(stm).scalars().all()
    for job in sorted(jobs, key=lambda x: x.id):
        ApplyWorkflowRead(**job.model_dump())
        print(f"ApplyWorkflow {job.id} validated")

    # STATES
    stm = select(State)
    states = db.execute(stm).scalars().all()
    for state in sorted(states, key=lambda x: x.id):
        StateRead(**state.model_dump())
        print(f"State {state.id} validated")

    # USERS
    stm = select(UserOAuth)
    users = db.execute(stm).scalars().unique().all()
    for user in sorted(users, key=lambda x: x.id):
        UserRead(**user.model_dump())
        print(f"User {user.id} validated")
