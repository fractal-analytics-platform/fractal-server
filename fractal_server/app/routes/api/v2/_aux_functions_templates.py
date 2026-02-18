from fastapi import HTTPException
from fastapi import status
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.models.linkusergroup import LinkUserGroup
from fractal_server.app.models.v2 import WorkflowTemplate


async def _get_template_full_access(
    *, user_id: int, template_id: int, db: AsyncSession
) -> WorkflowTemplate:
    template = await db.get(WorkflowTemplate, template_id)
    if template is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"WorkflowTemplate[{template_id}] not found.",
        )
    if template.user_id != user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"You are not the owner of WorkflowTemplate[{template_id}]."
            ),
        )
    return template


async def _get_template_read_access(
    *, user_id: int, template_id: int, db: AsyncSession
) -> WorkflowTemplate:
    template = await db.get(WorkflowTemplate, template_id)
    if template is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"WorkflowTemplate[{template_id}] not found.",
        )
    if template.user_id != user_id:
        link = await db.get(LinkUserGroup, (template.user_group_id, user_id))
        if link is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    "You are not authorized to "
                    f"WorkflowTemplate[{template_id}]."
                ),
            )
    return template


async def _check_template_duplication(
    *, user_id: int, name: str, version: int, db: AsyncSession
):
    res = await db.execute(
        select(WorkflowTemplate)
        .where(WorkflowTemplate.user_id == user_id)
        .where(WorkflowTemplate.name == name)
        .where(WorkflowTemplate.version == version)
    )
    duplicate = res.one_or_none()
    if duplicate:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                "There is already a WorkflowTemplate with "
                f"{user_id=}, {name=}, {version=}."
            ),
        )
