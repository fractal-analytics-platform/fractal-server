from typing import Any

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from pydantic import BaseModel
from pydantic import EmailStr
from pydantic import Field
from sqlmodel import func
from sqlmodel import select
from sqlmodel import update

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import LinkUserProjectV2
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.routes.auth import current_superuser_act
from fractal_server.app.routes.pagination import PaginationRequest
from fractal_server.app.routes.pagination import PaginationResponse
from fractal_server.app.routes.pagination import get_paginated_response
from fractal_server.app.routes.pagination import get_pagination_params
from fractal_server.app.schemas.v2.task import TaskType
from fractal_server.types import ListUniqueNonNegativeInt

from ._aux_functions_core_tasks import (
    _verify_non_duplication_task_core_constraint,
)

router = APIRouter()


class TaskMinimal(BaseModel):
    id: int
    name: str
    type: str
    is_core: bool
    taskgroupv2_id: int
    command_non_parallel: str | None = None
    command_parallel: str | None = None
    version: str


class ProjectUser(BaseModel):
    id: int
    email: EmailStr


class TaskRelationship(BaseModel):
    workflow_id: int
    workflow_name: str
    project_id: int
    project_name: str
    project_users: list[ProjectUser] = Field(default_factory=list)


class TaskInfo(BaseModel):
    task: TaskMinimal
    relationships: list[TaskRelationship]


@router.get("/", response_model=PaginationResponse[TaskInfo])
async def query_tasks(
    id: int | None = None,
    version: str | None = None,
    name: str | None = None,
    task_type: TaskType | None = None,
    category: str | None = None,
    modality: str | None = None,
    author: str | None = None,
    resource_id: int | None = None,
    only_core: bool = False,
    owner_id: int | None = None,
    task_group_name: str | None = None,
    private: bool | None = None,
    active: bool | None = None,
    pagination: PaginationRequest = Depends(get_pagination_params),
    superuser: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> dict[str, Any]:
    """
    Query `TaskV2` and get information about related workflows and projects.
    """
    # Prepare statements
    stm = select(TaskV2).order_by(TaskV2.id)
    stm_count = select(func.count(TaskV2.id))
    if id is not None:
        stm = stm.where(TaskV2.id == id)
        stm_count = stm_count.where(TaskV2.id == id)
    if version is not None:
        stm = stm.where(TaskV2.version == version)
        stm_count = stm_count.where(TaskV2.version == version)
    if name is not None:
        stm = stm.where(TaskV2.name.icontains(name))
        stm_count = stm_count.where(TaskV2.name.icontains(name))
    if task_type is not None:
        stm = stm.where(TaskV2.type == task_type)
        stm_count = stm_count.where(TaskV2.type == task_type)
    if category is not None:
        stm = stm.where(func.lower(TaskV2.category) == category.lower())
        stm_count = stm_count.where(
            func.lower(TaskV2.category) == category.lower()
        )
    if modality is not None:
        stm = stm.where(func.lower(TaskV2.modality) == modality.lower())
        stm_count = stm_count.where(
            func.lower(TaskV2.modality) == modality.lower()
        )
    if author is not None:
        stm = stm.where(TaskV2.authors.icontains(author))
        stm_count = stm_count.where(TaskV2.authors.icontains(author))
    if only_core is True:
        stm = stm.where(TaskV2.is_core)
        stm_count = stm_count.where(TaskV2.is_core)

    # TaskGroupV2 related query parameters
    if any(
        query_parameter is not None
        for query_parameter in (
            resource_id,
            owner_id,
            task_group_name,
            private,
            active,
        )
    ):
        stm = stm.join(TaskGroupV2, TaskGroupV2.id == TaskV2.taskgroupv2_id)
        stm_count = stm_count.join(
            TaskGroupV2, TaskGroupV2.id == TaskV2.taskgroupv2_id
        )
        if resource_id is not None:
            stm = stm.where(TaskGroupV2.resource_id == resource_id)
            stm_count = stm_count.where(TaskGroupV2.resource_id == resource_id)
        if owner_id is not None:
            stm = stm.where(TaskGroupV2.user_id == owner_id)
            stm_count = stm_count.where(TaskGroupV2.user_id == owner_id)
        if task_group_name is not None:
            stm = stm.where(TaskGroupV2.pkg_name.icontains(task_group_name))
            stm_count = stm_count.where(
                TaskGroupV2.pkg_name.icontains(task_group_name)
            )
        if private is not None:
            match private:
                case True:
                    stm = stm.where(TaskGroupV2.user_group_id.is_(None))
                    stm_count = stm_count.where(
                        TaskGroupV2.user_group_id.is_(None)
                    )
                case False:
                    stm = stm.where(TaskGroupV2.user_group_id.is_not(None))
                    stm_count = stm_count.where(
                        TaskGroupV2.user_group_id.is_not(None)
                    )
        if active is not None:
            stm = stm.where(TaskGroupV2.active.is_(active))
            stm_count = stm_count.where(TaskGroupV2.user_group_id.is_(active))

    response = await get_paginated_response(
        stm=stm, stm_count=stm_count, pagination=pagination, db=db
    )

    task_info_list = []
    for task in response.items:
        stm = (
            select(WorkflowV2)
            .join(
                WorkflowTaskV2,
                WorkflowTaskV2.workflow_id == WorkflowV2.id,
            )
            .where(WorkflowTaskV2.task_id == task.id)
        )
        res = await db.execute(stm)
        wf_list = res.scalars().all()

        project_users = {}
        for project_id in set([workflow.project_id for workflow in wf_list]):
            res = await db.execute(
                select(UserOAuth.id, UserOAuth.email)
                .join(
                    LinkUserProjectV2,
                    LinkUserProjectV2.user_id == UserOAuth.id,
                )
                .where(LinkUserProjectV2.project_id == project_id)
                .where(LinkUserProjectV2.is_owner.is_(True))
            )
            project_users[project_id] = [
                ProjectUser(id=p_user[0], email=p_user[1])
                for p_user in res.all()
            ]

        task_info_list.append(
            dict(
                task=task.model_dump(),
                relationships=[
                    dict(
                        workflow_id=workflow.id,
                        workflow_name=workflow.name,
                        project_id=workflow.project.id,
                        project_name=workflow.project.name,
                        project_users=project_users[workflow.project_id],
                    )
                    for workflow in wf_list
                ],
            )
        )
    return dict(
        total_count=response.total_count,
        page_size=response.page_size,
        current_page=response.current_page,
        items=task_info_list,
    )


@router.post(
    "/make-core/",
    status_code=status.HTTP_200_OK,
)
async def make_task_core(
    task_ids: ListUniqueNonNegativeInt,
    superuser: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    res = await db.execute(
        select(TaskV2, TaskGroupV2)
        .join(TaskGroupV2, TaskGroupV2.id == TaskV2.taskgroupv2_id)
        .where(TaskV2.id.in_(task_ids))
    )
    tasks_and_groups = res.all()
    if len(tasks_and_groups) != len(task_ids):
        missing_ids = sorted(
            list(set(task_ids) - set([tg[0].id for tg in tasks_and_groups]))
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Not all tasks were found (Missing IDs: {missing_ids}).",
        )

    # Acquire a lock on all rows that could result into conflicting core tasks,
    # to avoid a race condition where two "make-core" endpoints are called at
    # the same time. See
    # https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE
    # and https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-ROWS
    await db.execute(
        select(TaskV2)
        .where(TaskV2.name.in_([t.name for t, _ in tasks_and_groups]))
        .where(TaskV2.version.in_([t.version for t, _ in tasks_and_groups]))
        .where(TaskV2.is_core.is_(False))
        .with_for_update()
    )

    payload_tuples = [
        (
            task.name,
            task_group.pkg_name,
            task_group.version,
            task_group.resource_id,
        )
        for task, task_group in tasks_and_groups
    ]
    if len(set(payload_tuples)) != len(payload_tuples):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                "This request would generate conflicting core tasks "
                "(with the same task name and task-group properties). "
                "Hint: include fewer tasks in the request body and retry."
            ),
        )

    # Non-duplication check constraint
    for task, task_group in tasks_and_groups:
        await _verify_non_duplication_task_core_constraint(
            task=task, task_group=task_group, db=db
        )

    # Update
    await db.execute(
        update(TaskV2).where(TaskV2.id.in_(task_ids)).values(is_core=True)
    )
    await db.commit()

    return Response(status_code=status.HTTP_200_OK)


@router.post(
    "/make-not-core/",
    status_code=status.HTTP_200_OK,
)
async def make_task_not_core(
    task_ids: ListUniqueNonNegativeInt,
    superuser: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    res = await db.execute(select(TaskV2).where(TaskV2.id.in_(task_ids)))
    tasks = res.scalars().all()
    if len(tasks) != len(task_ids):
        missing_ids = sorted(list(set(task_ids) - set([t.id for t in tasks])))
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Not all tasks were found (Missing IDs: {missing_ids}).",
        )

    # Update
    await db.execute(
        update(TaskV2).where(TaskV2.id.in_(task_ids)).values(is_core=False)
    )
    await db.commit()

    return Response(status_code=status.HTTP_200_OK)
