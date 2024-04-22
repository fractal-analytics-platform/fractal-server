from sqlalchemy import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v1 import ApplyWorkflow
from fractal_server.app.models.v1 import Dataset
from fractal_server.app.models.v1 import Project
from fractal_server.app.models.v1 import Resource
from fractal_server.app.models.v1 import State
from fractal_server.app.models.v1 import Task
from fractal_server.app.models.v1 import UserOAuth
from fractal_server.app.models.v1 import Workflow
from fractal_server.app.schemas.v1 import ApplyWorkflowReadV1
from fractal_server.app.schemas.v1 import DatasetReadV1
from fractal_server.app.schemas.v1 import ProjectReadV1
from fractal_server.app.schemas.v1 import ResourceReadV1
from fractal_server.app.schemas.v1 import StateRead
from fractal_server.app.schemas.v1 import TaskReadV1
from fractal_server.app.schemas.v1 import UserRead
from fractal_server.app.schemas.v1 import WorkflowReadV1
from fractal_server.app.schemas.v1 import WorkflowTaskReadV1

with next(get_sync_db()) as db:

    # PROJECTS
    stm = select(Project)
    projects = db.execute(stm).scalars().all()
    for project in sorted(projects, key=lambda x: x.id):
        ProjectReadV1(**project.model_dump())
        print(f"Project {project.id} validated")

    # TASKS
    stm = select(Task)
    tasks = db.execute(stm).scalars().all()
    for task in sorted(tasks, key=lambda x: x.id):
        TaskReadV1(**task.model_dump())
        print(f"Task {task.id} validated")

    # WORKFLOWS
    stm = select(Workflow)
    workflows = db.execute(stm).scalars().all()
    for workflow in sorted(workflows, key=lambda x: x.id):
        WorkflowReadV1(
            **workflow.model_dump(),
            project=ProjectReadV1(**workflow.project.model_dump()),
            task_list=[
                WorkflowTaskReadV1(
                    **wftask.model_dump(),
                    task=TaskReadV1(**wftask.task.model_dump()),
                )
                for wftask in workflow.task_list
            ],
        )
        print(f"Workflow {workflow.id} validated")

    # TASKS
    stm = select(Resource)
    resources = db.execute(stm).scalars().all()
    for resource in sorted(resources, key=lambda x: x.id):
        ResourceReadV1(**resource.model_dump())
        print(f"Resource {resource.id} validated")

    # DATASETS
    stm = select(Dataset)
    datasets = db.execute(stm).scalars().all()
    for dataset in sorted(datasets, key=lambda x: x.id):
        DatasetReadV1(
            **dataset.model_dump(),
            project=ProjectReadV1(**dataset.project.model_dump()),
            resource_list=[
                ResourceReadV1(**resource.model_dump())
                for resource in dataset.resource_list
            ],
        )
        print(f"Dataset {dataset.id} validated")

    # JOBS
    stm = select(ApplyWorkflow)
    jobs = db.execute(stm).scalars().all()
    for job in sorted(jobs, key=lambda x: x.id):
        ApplyWorkflowReadV1(**job.model_dump())
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
