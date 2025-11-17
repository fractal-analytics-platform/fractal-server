from sqlalchemy import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models.linkusergroup import LinkUserGroup
from fractal_server.app.models.security import UserGroup
from fractal_server.app.models.security import UserOAuth
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.schemas.user import UserRead
from fractal_server.app.schemas.user_group import UserGroupRead
from fractal_server.app.schemas.v2 import DatasetReadV2
from fractal_server.app.schemas.v2 import JobReadV2
from fractal_server.app.schemas.v2 import ProjectReadV2
from fractal_server.app.schemas.v2 import TaskGroupActivityV2Read
from fractal_server.app.schemas.v2 import TaskGroupReadV2
from fractal_server.app.schemas.v2 import TaskReadV2
from fractal_server.app.schemas.v2 import WorkflowReadV2
from fractal_server.app.schemas.v2 import WorkflowTaskReadV2
from fractal_server.config import get_settings
from fractal_server.syringe import Inject


FRACTAL_DEFAULT_GROUP_NAME = Inject(get_settings).FRACTAL_DEFAULT_GROUP_NAME

with next(get_sync_db()) as db:
    # USERS
    stm = select(UserOAuth)
    users = db.execute(stm).scalars().unique().all()
    for user in sorted(users, key=lambda x: x.id):
        UserRead(**user.model_dump(), oauth_accounts=user.oauth_accounts)
        print(f"User {user.id} validated")

    # USER GROUPS
    stm = select(UserGroup)
    groups = db.execute(stm).scalars().unique().all()
    for group in sorted(groups, key=lambda x: x.id):
        UserGroupRead(**group.model_dump())
        print(f"UserGroup {group.id} validated")

    # DEFAULT GROUP
    default_group = next(
        (
            group
            for group in groups
            if group.name == FRACTAL_DEFAULT_GROUP_NAME
        ),
        None,
    )
    if default_group is None:
        raise ValueError(
            f"Default group '{FRACTAL_DEFAULT_GROUP_NAME}' does not exist."
        )

    stm = (
        select(UserOAuth.id)
        .join(LinkUserGroup, LinkUserGroup.user_id == UserOAuth.id)
        .where(LinkUserGroup.group_id == default_group.id)
    )
    user_ids_in_default_group = set(db.execute(stm).scalars().unique().all())
    all_user_ids = set(user.id for user in users)
    if user_ids_in_default_group == all_user_ids:
        print(f"All users are in default group '{FRACTAL_DEFAULT_GROUP_NAME}'")
    else:
        user_ids_not_in_default_group = (
            all_user_ids - user_ids_in_default_group
        )
        raise ValueError(
            "The following users are not in defualt group:\n"
            f"{user_ids_not_in_default_group}"
        )

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

    # TASK GROUPS V2
    stm = select(TaskGroupV2)
    task_groups = db.execute(stm).scalars().all()
    for task_group in sorted(task_groups, key=lambda x: x.id):
        task_list = []
        for task in task_group.task_list:
            task_list.append(TaskReadV2(**task.model_dump()))
        TaskGroupReadV2(**task_group.model_dump(), task_list=task_list)
        print(f"V2 - TaskGroup {task_group.id} validated")

    # TASK GROUP V2 ACTIVITIES
    stm = select(TaskGroupActivityV2)
    task_group_activities = db.execute(stm).scalars().all()
    for activity in sorted(task_group_activities, key=lambda x: x.id):
        TaskGroupActivityV2Read(**activity.model_dump())
        print(f"V2 - TaskGroupActivity {activity.id} validated")

    # WORKFLOWS V2
    stm = select(WorkflowV2)
    workflows = db.execute(stm).scalars().all()
    for workflow in sorted(workflows, key=lambda x: x.id):
        # validate task_list
        task_list = []
        for wftask in workflow.task_list:
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
