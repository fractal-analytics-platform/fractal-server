from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from pydantic import BaseModel
from pydantic import EmailStr
from pydantic import Field
from sqlmodel import func
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.routes.auth import current_active_superuser

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


@router.get("/", response_model=list[TaskV2Info])
async def query_tasks(
    id: int | None = None,
    source: str | None = None,
    version: str | None = None,
    name: str | None = None,
    max_number_of_results: int = 25,
    category: str | None = None,
    modality: str | None = None,
    author: str | None = None,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[TaskV2Info]:
    """
    Query `TaskV2` table and get information about related items
    (WorkflowV2s and ProjectV2s)

    Args:
        id: If not `None`, query for matching `task.id`.
        source: If not `None`, query for contained case insensitive
            `task.source`.
        version: If not `None`, query for matching `task.version`.
        name: If not `None`, query for contained case insensitive `task.name`.
        max_number_of_results: The maximum length of the response.
        category:
        modality:
        author:
    """

    stm = select(TaskV2)

    if id is not None:
        stm = stm.where(TaskV2.id == id)
    if source is not None:
        stm = stm.where(TaskV2.source.icontains(source))
    if version is not None:
        stm = stm.where(TaskV2.version == version)
    if name is not None:
        stm = stm.where(TaskV2.name.icontains(name))
    if category is not None:
        stm = stm.where(func.lower(TaskV2.category) == category.lower())
    if modality is not None:
        stm = stm.where(func.lower(TaskV2.modality) == modality.lower())
    if author is not None:
        stm = stm.where(TaskV2.authors.icontains(author))

    res = await db.execute(stm)
    task_list = res.scalars().all()
    if len(task_list) > max_number_of_results:
        await db.close()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Too many Tasks ({len(task_list)} > {max_number_of_results})."
                " Please add more query filters."
            ),
        )

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

    return task_info_list
