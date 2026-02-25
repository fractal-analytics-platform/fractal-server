from fastapi import APIRouter
from fastapi import Depends
from fastapi import Response
from fastapi import status
from pydantic import EmailStr
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlmodel import case
from sqlmodel import func
from sqlmodel import or_
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.linkusergroup import LinkUserGroup
from fractal_server.app.models.v2 import WorkflowTemplate
from fractal_server.app.routes.api.v2._aux_functions import (
    _create_workflow_export,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_workflow_check_access,
)
from fractal_server.app.routes.api.v2._aux_functions import _get_workflow_or_404
from fractal_server.app.routes.api.v2._aux_functions_templates import (
    _check_template_duplication,
)
from fractal_server.app.routes.api.v2._aux_functions_templates import (
    _get_template_full_access,
)
from fractal_server.app.routes.api.v2._aux_functions_templates import (
    _get_template_read_access,
)
from fractal_server.app.routes.auth import get_api_guest
from fractal_server.app.routes.auth import get_api_user
from fractal_server.app.routes.auth._aux_auth import (
    _verify_user_belongs_to_group,
)
from fractal_server.app.routes.pagination import PaginationRequest
from fractal_server.app.routes.pagination import PaginationResponse
from fractal_server.app.routes.pagination import get_pagination_params
from fractal_server.app.schemas.v2 import WorkflowTemplateCreate
from fractal_server.app.schemas.v2 import WorkflowTemplateExport
from fractal_server.app.schemas.v2 import WorkflowTemplateGroup
from fractal_server.app.schemas.v2 import WorkflowTemplateImport
from fractal_server.app.schemas.v2 import WorkflowTemplateRead
from fractal_server.app.schemas.v2 import WorkflowTemplateUpdate
from fractal_server.app.schemas.v2.sharing import ProjectPermissions

router = APIRouter()


class TemplatePage(PaginationResponse[WorkflowTemplateGroup]):
    email_list: list[EmailStr]


@router.get("/workflow_template/", response_model=TemplatePage)
async def get_workflow_template_list(
    template_id: int | None = None,
    is_owner: bool = False,
    user_email: str | None = None,
    name: str | None = None,
    version: int | None = None,
    # sort_by: Literal["user-name-version", "timestamp"] = "user-name-version",
    user: UserOAuth = Depends(get_api_guest),
    db: AsyncSession = Depends(get_async_db),
    pagination: PaginationRequest = Depends(get_pagination_params),
) -> TemplatePage:
    page = pagination.page
    page_size = pagination.page_size

    stm = (
        select(
            UserOAuth.email,
            WorkflowTemplate.name,
            func.array_agg(
                aggregate_order_by(
                    func.row(WorkflowTemplate.id, WorkflowTemplate.version),
                    WorkflowTemplate.version.desc(),
                )
            ),
        )
        .join(UserOAuth, UserOAuth.id == WorkflowTemplate.user_id)
        .where(
            or_(
                WorkflowTemplate.user_id == user.id,
                WorkflowTemplate.user_group_id.in_(
                    select(LinkUserGroup.group_id).where(
                        LinkUserGroup.user_id == user.id
                    )
                ),
            )
        )
        .group_by(
            WorkflowTemplate.user_id,
            WorkflowTemplate.name,
            UserOAuth.email,
        )
        .order_by(
            case(
                (WorkflowTemplate.user_id == user.id, 0),
                else_=1,
            ),
            UserOAuth.email,
            WorkflowTemplate.name,
        )
    )

    stm_count_subquery = (
        select(
            WorkflowTemplate.user_id,
            WorkflowTemplate.name,
        )
        .join(UserOAuth, UserOAuth.id == WorkflowTemplate.user_id)
        .where(
            or_(
                WorkflowTemplate.user_id == user.id,
                WorkflowTemplate.user_group_id.in_(
                    select(LinkUserGroup.group_id).where(
                        LinkUserGroup.user_id == user.id
                    )
                ),
            )
        )
    )

    if template_id:
        stm = stm.where(WorkflowTemplate.id == template_id)
        stm_count_subquery = stm_count_subquery.where(
            WorkflowTemplate.id == template_id
        )
    if is_owner:
        stm = stm.where(WorkflowTemplate.user_id == user.id)
        stm_count_subquery = stm_count_subquery.where(
            WorkflowTemplate.user_id == user.id
        )
    if user_email:
        stm = stm.where(UserOAuth.email == user_email)
        stm_count_subquery = stm_count_subquery.where(
            UserOAuth.email == user_email
        )
    if name:
        stm = stm.where(WorkflowTemplate.name.icontains(name))
        stm_count_subquery = stm_count_subquery.where(
            WorkflowTemplate.name.icontains(name)
        )
    if version:
        stm = stm.where(WorkflowTemplate.version == version)
        stm_count_subquery = stm_count_subquery.where(
            WorkflowTemplate.version == version
        )

    stm_count = select(func.count()).select_from(
        stm_count_subquery.group_by(
            WorkflowTemplate.user_id,
            WorkflowTemplate.name,
        ).subquery()
    )
    res_total_count = await db.execute(stm_count)
    total_count = res_total_count.scalar()
    if page_size is None:
        page_size = total_count
    else:
        stm = stm.offset((page - 1) * page_size).limit(page_size)

    res = await db.execute(stm)
    template_groups = res.all()

    stm_email = (
        select(UserOAuth.email)
        .join(WorkflowTemplate, WorkflowTemplate.user_id == UserOAuth.id)
        .where(
            or_(
                WorkflowTemplate.user_id == user.id,
                WorkflowTemplate.user_group_id.in_(
                    select(LinkUserGroup.group_id).where(
                        LinkUserGroup.user_id == user.id
                    )
                ),
            )
        )
        .order_by(UserOAuth.email.asc())
    )
    res = await db.execute(stm_email)
    email_list = res.unique().scalars().all()

    return dict(
        total_count=total_count,
        page_size=page_size,
        current_page=page,
        items=[
            dict(
                user_email=email,
                template_name=template_name,
                templates=[
                    {
                        "template_id": template_id,
                        "template_version": template_version,
                    }
                    for template_id, template_version in members
                ],
            )
            for email, template_name, members in template_groups
        ],
        email_list=email_list,
    )


@router.get(
    "/workflow_template/{template_id}/",
    response_model=WorkflowTemplateRead,
)
async def get_workflow_template(
    template_id: int,
    user: UserOAuth = Depends(get_api_guest),
    db: AsyncSession = Depends(get_async_db),
) -> WorkflowTemplateRead:
    template = await _get_template_read_access(
        user_id=user.id, template_id=template_id, db=db
    )
    res = await db.execute(
        select(UserOAuth.email).where(UserOAuth.id == template.user_id)
    )
    user_email = res.scalars().one()
    return dict(
        user_email=user_email,
        **template.model_dump(exclude={"user_id"}),
    )


@router.post(
    "/workflow_template/",
    status_code=status.HTTP_201_CREATED,
    response_model=WorkflowTemplateRead,
)
async def post_workflow_template(
    workflow_id: int,
    template_create: WorkflowTemplateCreate,
    user_group_id: int | None = None,
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
    if user_group_id:
        await _verify_user_belongs_to_group(
            user_id=user.id,
            user_group_id=user_group_id,
            db=db,
        )
    await _check_template_duplication(
        user_id=user.id,
        name=template_create.name,
        version=template_create.version,
        db=db,
    )

    workflow_export = await _create_workflow_export(workflow=workflow, db=db)

    template = WorkflowTemplate(
        user_id=user.id,
        user_group_id=user_group_id,
        data=workflow_export.model_dump(),
        **template_create.model_dump(),
    )
    db.add(template)
    await db.commit()
    await db.refresh(template)
    return dict(
        user_email=user.email,
        **template.model_dump(exclude={"user_id"}),
    )


@router.patch(
    "/workflow_template/{template_id}/",
    response_model=WorkflowTemplateRead,
)
async def patch_workflow_template(
    template_id: int,
    template_update: WorkflowTemplateUpdate,
    user: UserOAuth = Depends(get_api_user),
    db: AsyncSession = Depends(get_async_db),
) -> WorkflowTemplateRead:
    template = await _get_template_full_access(
        user_id=user.id, template_id=template_id, db=db
    )
    if template_update.user_group_id:
        await _verify_user_belongs_to_group(
            user_id=user.id,
            user_group_id=template_update.user_group_id,
            db=db,
        )
    for key, value in template_update.model_dump(exclude_unset=True).items():
        setattr(template, key, value)
    await db.commit()
    await db.refresh(template)

    return dict(
        user_email=user.email,
        **template.model_dump(exclude={"user_id"}),
    )


@router.delete(
    "/workflow_template/{template_id}/",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_workflow_template(
    template_id: int,
    user: UserOAuth = Depends(get_api_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    template = await _get_template_full_access(
        user_id=user.id, template_id=template_id, db=db
    )
    await db.delete(template)
    await db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/workflow_template/import/",
    status_code=status.HTTP_201_CREATED,
    response_model=WorkflowTemplateRead,
)
async def import_workflow_template(
    template_import: WorkflowTemplateImport,
    user_group_id: int | None = None,
    user: UserOAuth = Depends(get_api_user),
    db: AsyncSession = Depends(get_async_db),
) -> WorkflowTemplateRead:
    if user_group_id:
        await _verify_user_belongs_to_group(
            user_id=user.id,
            user_group_id=user_group_id,
            db=db,
        )
    await _check_template_duplication(
        user_id=user.id,
        name=template_import.name,
        version=template_import.version,
        db=db,
    )
    template = WorkflowTemplate(
        user_id=user.id,
        user_group_id=user_group_id,
        **template_import.model_dump(),
    )
    db.add(template)
    await db.commit()
    await db.refresh(template)

    return dict(
        user_email=user.email,
        **template.model_dump(exclude={"user_id"}),
    )


@router.get(
    "/workflow_template/{template_id}/export/",
    response_model=WorkflowTemplateExport,
)
async def export_workflow_template(
    template_id: int,
    user: UserOAuth = Depends(get_api_guest),
    db: AsyncSession = Depends(get_async_db),
) -> WorkflowTemplateExport:
    template = await _get_template_read_access(
        user_id=user.id, template_id=template_id, db=db
    )
    return template.model_dump()
