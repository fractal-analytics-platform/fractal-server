from sqlalchemy import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v1 import ApplyWorkflow
from fractal_server.app.models.v1 import Dataset
from fractal_server.app.models.v1 import Project
from fractal_server.app.models.v1 import Resource
from fractal_server.app.models.v1 import State
from fractal_server.app.models.v1 import Task
from fractal_server.app.models.v1 import Workflow
from fractal_server.app.models.v2 import CollectionStateV2
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.schemas.v1 import ApplyWorkflowReadV1
from fractal_server.app.schemas.v1 import DatasetReadV1
from fractal_server.app.schemas.v1 import ProjectReadV1
from fractal_server.app.schemas.v1 import ResourceReadV1
from fractal_server.app.schemas.v1 import StateRead
from fractal_server.app.schemas.v1 import TaskReadV1
from fractal_server.app.schemas.v1 import UserRead
from fractal_server.app.schemas.v1 import WorkflowReadV1
from fractal_server.app.schemas.v1 import WorkflowTaskReadV1
from fractal_server.app.schemas.v2 import DatasetReadV2
from fractal_server.app.schemas.v2 import JobReadV2
from fractal_server.app.schemas.v2 import ProjectReadV2
from fractal_server.app.schemas.v2 import TaskLegacyReadV2
from fractal_server.app.schemas.v2 import TaskReadV2
from fractal_server.app.schemas.v2 import WorkflowReadV2
from fractal_server.app.schemas.v2 import WorkflowTaskReadV2

with next(get_sync_db()) as db:

    # USERS
    stm = select(UserOAuth)
    users = db.execute(stm).scalars().unique().all()
    for user in sorted(users, key=lambda x: x.id):
        UserRead(**user.model_dump())
        print(f"User {user.id} validated")

    # V1

    # PROJECTS V1
    stm = select(Project)
    projects = db.execute(stm).scalars().all()
    for project in sorted(projects, key=lambda x: x.id):
        ProjectReadV1(**project.model_dump())
        print(f"V1 - Project {project.id} validated")

    # TASKS V1
    stm = select(Task)
    tasks = db.execute(stm).scalars().all()
    for task in sorted(tasks, key=lambda x: x.id):
        TaskReadV1(**task.model_dump())
        print(f"V1 - Task {task.id} validated")

    # WORKFLOWS V1
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
        print(f"V1 - Workflow {workflow.id} validated")

    # RESOURCES V1
    stm = select(Resource)
    resources = db.execute(stm).scalars().all()
    for resource in sorted(resources, key=lambda x: x.id):
        ResourceReadV1(**resource.model_dump())
        print(f"V1 - Resource {resource.id} validated")

    # DATASETS V1
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
        print(f"V1 - Dataset {dataset.id} validated")

    # JOBS V1
    stm = select(ApplyWorkflow)
    jobs = db.execute(stm).scalars().all()
    for job in sorted(jobs, key=lambda x: x.id):
        ApplyWorkflowReadV1(**job.model_dump())
        print(f"V1 - ApplyWorkflow {job.id} validated")

    # STATES V1
    stm = select(State)
    states = db.execute(stm).scalars().all()
    for state in sorted(states, key=lambda x: x.id):
        StateRead(**state.model_dump())
        print(f"V1 - State {state.id} validated")

    # V2

    # PROJECTS V2
    stm = select(ProjectV2)
    projects = db.execute(stm).scalars().all()
    for project in sorted(projects, key=lambda x: x.id):
        ProjectReadV2(**project.model_dump())
        print(f"V2 - Project {project.id} validated")

    # TASKS V2
    stm = select(TaskV2)
    tasks = db.execute(stm).scalars().all()
    for task in sorted(tasks, key=lambda x: x.id):
        TaskReadV2(**task.model_dump())
        print(f"V2 - Task {task.id} validated")

    # WORKFLOWS V2
    stm = select(WorkflowV2)
    workflows = db.execute(stm).scalars().all()
    for workflow in sorted(workflows, key=lambda x: x.id):
        # validate task_list
        task_list = []
        for wftask in workflow.task_list:
            if wftask.is_legacy_task is True:
                task_list.append(
                    WorkflowTaskReadV2(
                        **wftask.model_dump(),
                        task_legacy=TaskLegacyReadV2(
                            **wftask.task.model_dump()
                        ),
                    )
                )
            else:
                task_list.append(
                    WorkflowTaskReadV2(
                        **wftask.model_dump(),
                        task=TaskReadV2(**wftask.task.model_dump()),
                    )
                )

        WorkflowReadV2(
            **workflow.model_dump(),
            project=ProjectReadV2(**workflow.project.model_dump()),
            task_list=task_list,
        )
        print(f"V2 - Workflow {workflow.id} validated")

    # DATASETS V2
    stm = select(DatasetV2)
    datasets = db.execute(stm).scalars().all()
    for dataset in sorted(datasets, key=lambda x: x.id):
        DatasetReadV2(
            **dataset.model_dump(),
            project=ProjectReadV2(**dataset.project.model_dump()),
        )
        print(f"V2 - Dataset {dataset.id} validated")

    # JOBS V2
    stm = select(JobV2)
    jobs = db.execute(stm).scalars().all()
    for job in sorted(jobs, key=lambda x: x.id):
        JobReadV2(**job.model_dump())
        print(f"V2 - Job {job.id} validated")

    # COLLECTION STATES V2
    stm = select(CollectionStateV2)
    states = db.execute(stm).scalars().all()
    for collection_state in sorted(states, key=lambda x: x.id):
        CollectionStateV2(**collection_state.model_dump())
        print(f"V2 - CollectionState {state.id} validated")
