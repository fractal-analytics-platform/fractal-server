# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original author(s):
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Marco Franzon <marco.franzon@exact-lab.it>
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from sqlmodel import select

from .....logger import close_logger
from .....logger import set_logger
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v1 import Project
from ....models.v1 import Workflow
from ....schemas.v1 import WorkflowExportV1
from ....schemas.v1 import WorkflowReadV1
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_workflow_check_owner
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_user


router = APIRouter()


@router.get(
    "/project/{project_id}/workflow/",
    response_model=list[WorkflowReadV1],
)
async def get_workflow_list(
    project_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[list[WorkflowReadV1]]:
    """
    Get workflow list for given project
    """
    # Access control
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    # Find workflows of the current project. Note: this select/where approach
    # has much better scaling than refreshing all elements of
    # `project.workflow_list` - ref
    # https://github.com/fractal-analytics-platform/fractal-server/pull/1082#issuecomment-1856676097.
    stm = select(Workflow).where(Workflow.project_id == project.id)
    workflow_list = (await db.execute(stm)).scalars().all()
    return workflow_list


@router.get(
    "/project/{project_id}/workflow/{workflow_id}/",
    response_model=WorkflowReadV1,
)
async def read_workflow(
    project_id: int,
    workflow_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[WorkflowReadV1]:
    """
    Get info on an existing workflow
    """

    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )

    return workflow


@router.get(
    "/project/{project_id}/workflow/{workflow_id}/export/",
    response_model=WorkflowExportV1,
)
async def export_worfklow(
    project_id: int,
    workflow_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[WorkflowExportV1]:
    """
    Export an existing workflow, after stripping all IDs
    """
    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )
    # Emit a warning when exporting a workflow with custom tasks
    logger = set_logger(None)
    for wftask in workflow.task_list:
        if wftask.task.owner is not None:
            logger.warning(
                f"Custom tasks (like the one with id={wftask.task.id} and "
                f'source="{wftask.task.source}") are not meant to be '
                "portable; re-importing this workflow may not work as "
                "expected."
            )
    close_logger(logger)

    await db.close()
    return workflow


@router.get("/workflow/", response_model=list[WorkflowReadV1])
async def get_user_workflows(
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[WorkflowReadV1]:
    """
    Returns all the workflows of the current user
    """
    stm = select(Workflow)
    stm = stm.join(Project).where(
        Project.user_list.any(UserOAuth.id == user.id)
    )
    res = await db.execute(stm)
    workflow_list = res.scalars().all()
    return workflow_list
