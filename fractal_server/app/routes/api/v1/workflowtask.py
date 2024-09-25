# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original author(s):
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Marco Franzon <marco.franzon@exact-lab.it>
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
# Yuri Chiucconi <yuri.chiucconi@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
from fastapi import APIRouter
from fastapi import Depends

from ....db import AsyncSession
from ....db import get_async_db
from ....schemas.v1 import WorkflowTaskReadV1
from ._aux_functions import _get_workflow_task_check_owner
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_user

router = APIRouter()


@router.get(
    "/project/{project_id}/workflow/{workflow_id}/wftask/{workflow_task_id}/",
    response_model=WorkflowTaskReadV1,
)
async def read_workflowtask(
    project_id: int,
    workflow_id: int,
    workflow_task_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
):
    workflow_task, _ = await _get_workflow_task_check_owner(
        project_id=project_id,
        workflow_task_id=workflow_task_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )
    return workflow_task
