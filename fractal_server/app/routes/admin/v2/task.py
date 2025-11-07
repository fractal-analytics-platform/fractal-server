from fastapi import APIRouter
from fastapi import Depends
from pydantic import BaseModel
from pydantic import EmailStr
from pydantic import Field
from sqlmodel import func
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.routes.auth import current_superuser_act
from fractal_server.app.routes.pagination import get_pagination_params
from fractal_server.app.routes.pagination import PaginationRequest
from fractal_server.app.routes.pagination import PaginationResponse
from fractal_server.app.schemas.v2.task import TaskType

router = APIRouter()


class TaskV2Minimal(BaseModel):
    id: int
    name: str
    type: str
    taskgroupv2_id: int
    command_non_parallel: str | None = None
    command_parallel: str | None = None
    source: str | None = None
    version: str | None = None


class ProjectUser(BaseModel):
    id: int
    email: EmailStr


class TaskV2Relationship(BaseModel):
    workflow_id: int
    workflow_name: str
    project_id: int
    project_name: str
    project_users: list[ProjectUser] = Field(default_factory=list)


class TaskV2Info(BaseModel):
    task: TaskV2Minimal
    relationships: list[TaskV2Relationship]


@router.get("/", response_model=PaginationResponse[TaskV2Info])
async def query_tasks(
    id: int | None = None,
    source: str | None = None,
    version: str | None = None,
    name: str | None = None,
    task_type: TaskType | None = None,
    category: str | None = None,
    modality: str | None = None,
    author: str | None = None,
    resource_id: int | None = None,
    pagination: PaginationRequest = Depends(get_pagination_params),
    user: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> PaginationResponse[TaskV2Info]:
    """
    Query `TaskV2` and get information about related workflows and projects.
    """

    # Assign pagination parameters
    page = pagination.page
    page_size = pagination.page_size

    # Prepare statements
    stm = select(TaskV2).order_by(TaskV2.id)
    stm_count = select(func.count(TaskV2.id))
    if id is not None:
        stm = stm.where(TaskV2.id == id)
        stm_count = stm_count.where(TaskV2.id == id)
    if source is not None:
        stm = stm.where(TaskV2.source.icontains(source))
        stm_count = stm_count.where(TaskV2.source.icontains(source))
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
    if resource_id is not None:
        stm = (
            stm.join(TaskGroupV2)
            .where(TaskGroupV2.id == TaskV2.taskgroupv2_id)
            .where(TaskGroupV2.resource_id == resource_id)
        )
        stm_count = (
            stm_count.join(TaskGroupV2)
            .where(TaskGroupV2.id == TaskV2.taskgroupv2_id)
            .where(TaskGroupV2.resource_id == resource_id)
        )

    # Find total number of elements
    res_total_count = await db.execute(stm_count)
    total_count = res_total_count.scalar()
    if page_size is None:
        page_size = total_count
    else:
        stm = stm.offset((page - 1) * page_size).limit(page_size)

    # Get `page_size` rows
    res = await db.execute(stm)
    task_list = res.scalars().all()

    task_info_list = []
    for task in task_list:
        stm = (
            select(WorkflowV2)
            .join(WorkflowTaskV2)
            .where(WorkflowTaskV2.workflow_id == WorkflowV2.id)
            .where(WorkflowTaskV2.task_id == task.id)
        )
        res = await db.execute(stm)
        wf_list = res.scalars().all()

        task_info_list.append(
            dict(
                task=task.model_dump(),
                relationships=[
                    dict(
                        workflow_id=workflow.id,
                        workflow_name=workflow.name,
                        project_id=workflow.project.id,
                        project_name=workflow.project.name,
                        project_users=[
                            dict(id=user.id, email=user.email)
                            for user in workflow.project.user_list
                        ],
                    )
                    for workflow in wf_list
                ],
            )
        )

    return PaginationResponse[TaskV2Info](
        total_count=total_count,
        page_size=page_size,
        current_page=page,
        items=task_info_list,
    )
