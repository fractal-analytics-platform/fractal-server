from typing import Literal

from fastapi import APIRouter
from fastapi import Depends
from sqlmodel import func
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import WorkflowTemplate
from fractal_server.app.routes.auth import get_api_guest
from fractal_server.app.routes.pagination import PaginationRequest
from fractal_server.app.routes.pagination import PaginationResponse
from fractal_server.app.routes.pagination import get_pagination_params
from fractal_server.app.schemas.v2 import WorkflowTemplateRead

router = APIRouter()


@router.get(
    "/workflow_template/",
    response_model=PaginationResponse[WorkflowTemplateRead],
)
async def get_workflow_template_list(
    is_owner: bool = False,
    user_email: str | None = None,
    name: str | None = None,
    version: str | None = None,
    sort_by: Literal["user-name-version", "timestamp"] = "user-name-version",
    user: UserOAuth = Depends(get_api_guest),
    db: AsyncSession = Depends(get_async_db),
    pagination: PaginationRequest = Depends(get_pagination_params),
) -> PaginationResponse[WorkflowTemplateRead]:
    page = pagination.page
    page_size = pagination.page_size

    stm = select(WorkflowTemplate)
    stm_count = select(func.count(WorkflowTemplate.id))

    if is_owner:
        stm = stm.where(WorkflowTemplate.user_id == user.id)
        stm_count = stm_count.where(WorkflowTemplate.user_id == user.id)
    if user_email:
        stm = stm.join(
            UserOAuth, UserOAuth.id == WorkflowTemplate.user_id
        ).where(UserOAuth.email == user_email)
        stm_count = stm_count.join(
            UserOAuth, UserOAuth.id == WorkflowTemplate.user_id
        ).where(UserOAuth.email == user_email)
    if name:
        stm = stm.where(WorkflowTemplate.name.icontains(name))
        stm_count = stm_count.where(WorkflowTemplate.name.icontains(name))
    if version:
        stm = stm.where(WorkflowTemplate.version == version)
        stm_count = stm_count.where(WorkflowTemplate.version == version)

    # FIXME: sort with `sort_by`

    res_total_count = await db.execute(stm_count)
    total_count = res_total_count.scalar()
    if page_size is None:
        page_size = total_count
    else:
        stm = stm.offset((page - 1) * page_size).limit(page_size)

    res = await db.execute(stm)
    workflow_templates = res.scalars().all()

    return dict(
        total_count=total_count,
        page_size=page_size,
        current_page=page,
        items=workflow_templates,
    )
