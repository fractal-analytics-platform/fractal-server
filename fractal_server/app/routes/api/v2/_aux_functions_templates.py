from fastapi import HTTPException
from fastapi import status
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.models.linkusergroup import LinkUserGroup
from fractal_server.app.models.v2 import WorkflowTemplate


async def _get_template_or_404(
    *, template_id: int, db: AsyncSession
) -> WorkflowTemplate:
    """
    Retrieve a `WorkflowTemplate` by ID.

    Args:
        template_id:
        db:

    Returns:
        The `WorkflowTemplate` object.

    Raises:
        HTTPException(status_code=404_NOT_FOUND):
            If no template exists with the given ID.
    """
    template = await db.get(WorkflowTemplate, template_id)
    if template is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"WorkflowTemplate[{template_id}] not found.",
        )
    return template


async def _get_template_full_access(
    *, user_id: int, template_id: int, db: AsyncSession
) -> WorkflowTemplate:
    """
    Retrieve a `WorkflowTemplate` and ensure the user is its owner.

    Args:
        user_id:
        template_id:
        db:

    Returns:
        The `WorkflowTemplate` object.

    Raises:
        HTTPException(status_code=404_NOT_FOUND):
            If no template exists with the given ID.
        HTTPException(status_code=403_FORBIDDEN):
            If the user is not the owner.
    """
    template = await _get_template_or_404(template_id=template_id, db=db)
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
    """
    Retrieve a `WorkflowTemplate` and ensure the user has read access.

    Access is granted if the user is the owner of the template or belongs to
    the template's user group.

    Args:
        user_id:
        template_id:
        db:

    Returns:
        The `WorkflowTemplate` object.

    Raises:
        HTTPException(status_code=404_NOT_FOUND):
            If no template exists with the given ID.
        HTTPException(status_code=403_FORBIDDEN):
            If the user has not read access.
    """
    template = await _get_template_or_404(template_id=template_id, db=db)
    if template.user_id != user_id:
        link = await db.get(LinkUserGroup, (template.user_group_id, user_id))
        if link is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    "You are not authorized to view "
                    f"WorkflowTemplate[{template_id}]."
                ),
            )
    return template


async def _check_template_duplication(
    *, user_id: int, name: str, version: int, db: AsyncSession
) -> None:
    """
    Ensure that no `WorkflowTemplate` with the same
    (`user_id`, `name`, `version`) already exists.

    Args:
        user_id:
        name:
        version:
        db:

    Raises:
        HTTPException(status_code=HTTP_422_UNPROCESSABLE_CONTENT):
            If a duplicate template is found.


    """
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
