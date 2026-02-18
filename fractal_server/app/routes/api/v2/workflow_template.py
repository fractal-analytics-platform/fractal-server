from typing import Literal

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import func
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import WorkflowTemplate
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_workflow_check_access,
)
from fractal_server.app.routes.api.v2._aux_functions import _get_workflow_or_404
from fractal_server.app.routes.api.v2.workflow import export_workflow
from fractal_server.app.routes.auth import get_api_guest
from fractal_server.app.routes.auth import get_api_user
from fractal_server.app.routes.auth._aux_auth import (
    _verify_user_belongs_to_group,
)
from fractal_server.app.routes.pagination import PaginationRequest
from fractal_server.app.routes.pagination import PaginationResponse
from fractal_server.app.routes.pagination import get_pagination_params
from fractal_server.app.schemas.v2 import WorkflowTemplateCreate
from fractal_server.app.schemas.v2 import WorkflowTemplateRead
from fractal_server.app.schemas.v2 import WorkflowTemplateUpdate
from fractal_server.app.schemas.v2.sharing import ProjectPermissions

router = APIRouter()


@router.get(
    "/workflow_template/",
    response_model=PaginationResponse[WorkflowTemplateRead],
)
async def get_workflow_template_list(
    is_owner: bool = False,
    user_email: str | None = None,
    name: str | None = None,
    version: int | None = None,
    sort_by: Literal["user-name-version", "timestamp"] = "user-name-version",
    user: UserOAuth = Depends(get_api_guest),
    db: AsyncSession = Depends(get_async_db),
    pagination: PaginationRequest = Depends(get_pagination_params),
) -> PaginationResponse[WorkflowTemplateRead]:
    page = pagination.page
    page_size = pagination.page_size

    stm = select(WorkflowTemplate, UserOAuth.email).join(
        UserOAuth, UserOAuth.id == WorkflowTemplate.user_id
    )
    stm_count = select(func.count(WorkflowTemplate.id))

    if is_owner:
        stm = stm.where(WorkflowTemplate.user_id == user.id)
        stm_count = stm_count.where(WorkflowTemplate.user_id == user.id)
    if user_email:
        stm = stm.where(UserOAuth.email == user_email)
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
    workflow_templates_and_user_email = res.all()

    return dict(
        total_count=total_count,
        page_size=page_size,
        current_page=page,
        items=[
            dict(
                user_email=email,
                **workflow_template.model_dump(exclude={"user_id"}),
            )
            for workflow_template, email in workflow_templates_and_user_email
        ],
    )


@router.get(
    "/workflow_template/{workflow_template_id}/",
    response_model=WorkflowTemplateRead,
)
async def get_workflow_template(
    workflow_template_id: int,
    user: UserOAuth = Depends(get_api_guest),
    db: AsyncSession = Depends(get_async_db),
) -> WorkflowTemplateRead:
    res = await db.execute(
        select(WorkflowTemplate, UserOAuth.email)
        .join(UserOAuth, UserOAuth.id == WorkflowTemplate.user_id)
        .where(WorkflowTemplate.id == workflow_template_id)
    )
    workflow_template_and_user_email = res.one_or_none()

    if workflow_template_and_user_email is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"WorkflowTemplate[{workflow_template_id}] not found.",
        )
    workflow_template, user_email = workflow_template_and_user_email

    return dict(
        user_email=user_email,
        **workflow_template.model_dump(exclude={"user_id"}),
    )


@router.post(
    "/workflow_template/",
    status_code=status.HTTP_201_CREATED,
    response_model=WorkflowTemplateRead,
)
async def post_workflow_template(
    workflow_id: int,
    workflow_template_create: WorkflowTemplateCreate,
    user: UserOAuth = Depends(get_api_user),
    db: AsyncSession = Depends(get_async_db),
) -> WorkflowTemplateRead:
    workflow = await _get_workflow_or_404(workflow_id=workflow_id, db=db)
    await _get_workflow_check_access(
        project_id=workflow.project_id,
        workflow_id=workflow_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.READ,
        db=db,
    )
    if workflow_template_create.user_group_id:
        await _verify_user_belongs_to_group(
            user_id=user.id,
            user_group_id=workflow_template_create.user_group_id,
            db=db,
        )

    res = await db.execute(
        select(WorkflowTemplate)
        .where(WorkflowTemplate.user_id == user.id)
        .where(WorkflowTemplate.name == workflow_template_create.name)
        .where(WorkflowTemplate.version == workflow_template_create.version)
    )
    duplicate = res.one_or_none()
    if duplicate:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                "There is already a WorkflowTemplate with "
                f"user_id='{user.id}', "
                f"name='{workflow_template_create.name}', "
                f"version='{workflow_template_create.version}'."
            ),
        )
    data = await export_workflow(
        project_id=workflow.project_id,
        workflow_id=workflow_id,
        user=user,
        db=db,
    )

    workflow_template = WorkflowTemplate(
        user_id=user.id,
        data=data.model_dump(),
        **workflow_template_create.model_dump(),
    )
    db.add(workflow_template)
    await db.commit()
    await db.refresh(workflow_template)

    return dict(
        user_email=user.email,
        **workflow_template.model_dump(exclude={"user_id"}),
    )


@router.patch(
    "/workflow_template/{workflow_template_id}/",
    response_model=WorkflowTemplateRead,
)
async def patch_workflow_template(
    workflow_template_id: int,
    workflow_template_update: WorkflowTemplateUpdate,
    user: UserOAuth = Depends(get_api_user),
    db: AsyncSession = Depends(get_async_db),
) -> WorkflowTemplateRead:
    workflow_template = await db.get(WorkflowTemplate, workflow_template_id)
    if workflow_template is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"WorkflowTemplate[{workflow_template_id}] not found.",
        )
    if workflow_template.user_id != user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "You are not authorized to edit "
                f"WorkflowTemplate[{workflow_template_id}]."
            ),
        )
    if workflow_template_update.user_group_id:
        await _verify_user_belongs_to_group(
            user_id=user.id,
            user_group_id=workflow_template_update.user_group_id,
            db=db,
        )
    for key, value in workflow_template_update.model_dump(
        exclude_unset=True
    ).items():
        setattr(workflow_template, key, value)
    await db.commit()
    await db.refresh(workflow_template)

    return dict(
        user_email=user.email,
        **workflow_template.model_dump(exclude={"user_id"}),
    )


@router.delete(
    "/workflow_template/{workflow_template_id}/",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_workflow_template(
    workflow_template_id: int,
    user: UserOAuth = Depends(get_api_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    workflow_template = await db.get(WorkflowTemplate, workflow_template_id)
    if workflow_template is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"WorkflowTemplate[{workflow_template_id}] not found.",
        )
    if workflow_template.user_id != user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "You are not authorized to delete "
                f"WorkflowTemplate[{workflow_template_id}]."
            ),
        )
    await db.delete(workflow_template)
    await db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
