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
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import select

from ....common.schemas import WorkflowCreate
from ....common.schemas import WorkflowExport
from ....common.schemas import WorkflowImport
from ....common.schemas import WorkflowRead
from ....common.schemas import WorkflowTaskCreate
from ....common.schemas import WorkflowUpdate
from ....logger import close_logger
from ....logger import set_logger
from ...db import AsyncSession
from ...db import get_db
from ...models import ApplyWorkflow
from ...models import Task
from ...models import Workflow
from ...security import current_active_user
from ...security import User
from ._aux_functions import _check_workflow_exists
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_workflow_check_owner


router = APIRouter()


@router.get(
    "/project/{project_id}/workflow/",
    response_model=list[WorkflowRead],
)
async def get_workflow_list(
    project_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[list[WorkflowRead]]:
    """
    Get list of workflows associated to the current project
    """
    await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    stm = select(Workflow).where(Workflow.project_id == project_id)
    res = await db.execute(stm)
    workflow_list = res.scalars().all()
    await db.close()
    return workflow_list


@router.post(
    "/project/{project_id}/workflow/",
    response_model=WorkflowRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_workflow(
    project_id: int,
    workflow: WorkflowCreate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:
    """
    Create a workflow, associate to a project
    """
    await _get_project_check_owner(
        project_id=project_id,
        user_id=user.id,
        db=db,
    )
    await _check_workflow_exists(
        name=workflow.name, project_id=project_id, db=db
    )

    db_workflow = Workflow(project_id=project_id, **workflow.dict())
    db.add(db_workflow)
    await db.commit()
    await db.refresh(db_workflow)
    await db.close()
    return db_workflow


@router.get(
    "/project/{project_id}/workflow/{workflow_id}",
    response_model=WorkflowRead,
)
async def read_workflow(
    project_id: int,
    workflow_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:
    """
    Get info on an existing workflow
    """

    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )

    return workflow


@router.patch(
    "/project/{project_id}/workflow/{workflow_id}",
    response_model=WorkflowRead,
)
async def update_workflow(
    project_id: int,
    workflow_id: int,
    patch: WorkflowUpdate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:
    """
    Edit a workflow
    """
    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )

    for key, value in patch.dict(exclude_unset=True).items():
        if key == "reordered_workflowtask_ids":
            current_workflowtask_ids = [
                wftask.id for wftask in workflow.task_list
            ]
            num_tasks = len(workflow.task_list)
            if len(value) != num_tasks or set(value) != set(
                current_workflowtask_ids
            ):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        "`reordered_workflowtask_ids` must be a permutation of"
                        f" {current_workflowtask_ids} (given {value})"
                    ),
                )
            for ind_wftask in range(num_tasks):
                new_order = value.index(workflow.task_list[ind_wftask].id)
                workflow.task_list[ind_wftask].order = new_order
        else:
            setattr(workflow, key, value)
    await db.commit()
    await db.refresh(workflow)
    await db.close()

    return workflow


@router.delete(
    "/project/{project_id}/workflow/{workflow_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_workflow(
    project_id: int,
    workflow_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """
    Delte a workflow
    """

    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )

    # Check that no ApplyWorkflow is in relationship with the current Workflow
    stm = select(ApplyWorkflow).where(ApplyWorkflow.workflow_id == workflow_id)
    res = await db.execute(stm)
    job = res.scalars().first()
    if job:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Cannot remove workflow {workflow_id}: "
                f"it's still linked to job {job.id}."
            ),
        )

    await db.delete(workflow)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/project/{project_id}/workflow/{workflow_id}/export/",
    response_model=WorkflowExport,
)
async def export_worfklow(
    project_id: int,
    workflow_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowExport]:
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


@router.post(
    "/project/{project_id}/workflow/import/",
    response_model=WorkflowRead,
    status_code=status.HTTP_201_CREATED,
)
async def import_workflow(
    project_id: int,
    workflow: WorkflowImport,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:
    """
    Import an existing workflow into a project

    Also create all required objects (i.e. Workflow and WorkflowTask's) along
    the way.
    """

    # Preliminary checks
    await _get_project_check_owner(
        project_id=project_id,
        user_id=user.id,
        db=db,
    )

    await _check_workflow_exists(
        name=workflow.name, project_id=project_id, db=db
    )

    # Check that all required tasks are available
    tasks = [wf_task.task for wf_task in workflow.task_list]
    source_to_id = {}
    for task in tasks:
        source = task.source
        if source not in source_to_id.keys():
            stm = select(Task).where(Task.source == source)
            tasks_by_source = (await db.execute(stm)).scalars().all()
            if len(tasks_by_source) != 1:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        f"Found {len(tasks_by_source)} tasks with {source=}."
                    ),
                )
            source_to_id[source] = tasks_by_source[0].id

    # Create new Workflow (with empty task_list)
    db_workflow = Workflow(
        project_id=project_id,
        **workflow.dict(exclude_none=True, exclude={"task_list"}),
    )
    db.add(db_workflow)
    await db.commit()
    await db.refresh(db_workflow)

    # Insert tasks
    async with db:
        for _, wf_task in enumerate(workflow.task_list):
            # Identify task_id
            source = wf_task.task.source
            task_id = source_to_id[source]
            # Prepare new_wf_task
            new_wf_task = WorkflowTaskCreate(
                **wf_task.dict(exclude_none=True),
            )
            # Insert task
            await db_workflow.insert_task(
                **new_wf_task.dict(),
                task_id=task_id,
                db=db,
            )

    await db.close()
    return db_workflow
