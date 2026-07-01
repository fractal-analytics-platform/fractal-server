from typing import Any

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Response
from fastapi import status
from pydantic import BaseModel
from pydantic import EmailStr
from pydantic import Field
from sqlmodel import func
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import LinkUserProjectV2
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.security import UserGroup
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.routes.auth import current_superuser_act
from fractal_server.app.routes.pagination import PaginationRequest
from fractal_server.app.routes.pagination import PaginationResponse
from fractal_server.app.routes.pagination import get_pagination_data
from fractal_server.app.routes.pagination import get_pagination_params
from fractal_server.app.schemas.v2.task import TaskType
from fractal_server.types import ListUniqueNonNegativeInt

from ._aux_functions_core_tasks import _make_task_core_bulk
from ._aux_functions_core_tasks import _make_task_not_core_bulk

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
    pkg_name: str
    active: bool
    owner: EmailStr
    user_group: str | None = None


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
    owner_id: int | None = None,
    task_group: str | None = None,
    core: bool | None = None,
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
    stm = (
        select(
            TaskV2,
            TaskGroupV2.pkg_name,
            TaskGroupV2.active,
            UserOAuth.email,
            UserGroup.name,
        )
        .join(TaskGroupV2, TaskGroupV2.id == TaskV2.taskgroupv2_id)
        .join(UserOAuth, UserOAuth.id == TaskGroupV2.user_id)
        .outerjoin(UserGroup, UserGroup.id == TaskGroupV2.user_group_id)
        .order_by(TaskV2.id)
    )
    stm_count = select(func.count(TaskV2.id)).join(
        TaskGroupV2, TaskGroupV2.id == TaskV2.taskgroupv2_id
    )
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
    if core is not None:
        stm = stm.where(TaskV2.is_core.is_(core))
        stm_count = stm_count.where(TaskV2.is_core.is_(core))
    if resource_id is not None:
        stm = stm.where(TaskGroupV2.resource_id == resource_id)
        stm_count = stm_count.where(TaskGroupV2.resource_id == resource_id)
    if owner_id is not None:
        stm = stm.where(TaskGroupV2.user_id == owner_id)
        stm_count = stm_count.where(TaskGroupV2.user_id == owner_id)
    if task_group is not None:
        stm = stm.where(TaskGroupV2.pkg_name.icontains(task_group))
        stm_count = stm_count.where(TaskGroupV2.pkg_name.icontains(task_group))
    if private is not None:
        match private:
            case True:
                stm = stm.where(TaskGroupV2.user_group_id.is_(None))
                stm_count = stm_count.where(TaskGroupV2.user_group_id.is_(None))
            case False:
                stm = stm.where(TaskGroupV2.user_group_id.is_not(None))
                stm_count = stm_count.where(
                    TaskGroupV2.user_group_id.is_not(None)
                )
    if active is not None:
        stm = stm.where(TaskGroupV2.active.is_(active))
        stm_count = stm_count.where(TaskGroupV2.active.is_(active))

    stm, pagination_data = await get_pagination_data(
        stm=stm,
        stm_count=stm_count,
        pagination=pagination,
        db=db,
    )

    res = await db.execute(stm)
    records = res.all()

    task_info_list = []
    for task, pkg_name, _active, owner, user_group in records:
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
                task=dict(
                    **task.model_dump(),
                    pkg_name=pkg_name,
                    active=_active,
                    owner=owner,
                    user_group=user_group,
                ),
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
        items=task_info_list,
        **pagination_data.model_dump(),
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
    res = await _make_task_core_bulk(task_ids=task_ids, db=db)
    return res


@router.post(
    "/make-not-core/",
    status_code=status.HTTP_200_OK,
)
async def make_task_not_core(
    task_ids: ListUniqueNonNegativeInt,
    superuser: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    res = await _make_task_not_core_bulk(task_ids=task_ids, db=db)
    return res
