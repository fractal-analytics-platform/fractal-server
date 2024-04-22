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

from .....logger import close_logger
from .....logger import set_logger
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v1 import ApplyWorkflow
from ....models.v1 import Project
from ....models.v1 import Task
from ....models.v1 import Workflow
from ....schemas.v1 import WorkflowCreateV1
from ....schemas.v1 import WorkflowExportV1
from ....schemas.v1 import WorkflowImportV1
from ....schemas.v1 import WorkflowReadV1
from ....schemas.v1 import WorkflowTaskCreateV1
from ....schemas.v1 import WorkflowUpdateV1
from ....security import current_active_user
from ....security import User
from ._aux_functions import _check_workflow_exists
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_submitted_jobs_statement
from ._aux_functions import _get_workflow_check_owner
from ._aux_functions import _workflow_insert_task


router = APIRouter()


@router.get(
    "/project/{project_id}/workflow/",
    response_model=list[WorkflowReadV1],
)
async def get_workflow_list(
    project_id: int,
    user: User = Depends(current_active_user),
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


@router.post(
    "/project/{project_id}/workflow/",
    response_model=WorkflowReadV1,
    status_code=status.HTTP_201_CREATED,
)
async def create_workflow(
    project_id: int,
    workflow: WorkflowCreateV1,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[WorkflowReadV1]:
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
    "/project/{project_id}/workflow/{workflow_id}/",
    response_model=WorkflowReadV1,
)
async def read_workflow(
    project_id: int,
    workflow_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[WorkflowReadV1]:
    """
    Get info on an existing workflow
    """

    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )

    return workflow


@router.patch(
    "/project/{project_id}/workflow/{workflow_id}/",
    response_model=WorkflowReadV1,
)
async def update_workflow(
    project_id: int,
    workflow_id: int,
    patch: WorkflowUpdateV1,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[WorkflowReadV1]:
    """
    Edit a workflow
    """
    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )

    if patch.name:
        await _check_workflow_exists(
            name=patch.name, project_id=project_id, db=db
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
    "/project/{project_id}/workflow/{workflow_id}/",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_workflow(
    project_id: int,
    workflow_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Delete a workflow
    """

    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )

    # Fail if there exist jobs that are submitted and in relation with the
    # current workflow.
    stm = _get_submitted_jobs_statement().where(
        ApplyWorkflow.workflow_id == workflow.id
    )
    res = await db.execute(stm)
    jobs = res.scalars().all()
    if jobs:
        string_ids = str([job.id for job in jobs])[1:-1]
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Cannot delete workflow {workflow.id} because it "
                f"is linked to active job(s) {string_ids}."
            ),
        )

    # Cascade operations: set foreign-keys to null for jobs which are in
    # relationship with the current workflow
    stm = select(ApplyWorkflow).where(ApplyWorkflow.workflow_id == workflow_id)
    res = await db.execute(stm)
    jobs = res.scalars().all()
    for job in jobs:
        job.workflow_id = None
        await db.merge(job)
    await db.commit()

    # Delete workflow
    await db.delete(workflow)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/project/{project_id}/workflow/{workflow_id}/export/",
    response_model=WorkflowExportV1,
)
async def export_worfklow(
    project_id: int,
    workflow_id: int,
    user: User = Depends(current_active_user),
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


@router.post(
    "/project/{project_id}/workflow/import/",
    response_model=WorkflowReadV1,
    status_code=status.HTTP_201_CREATED,
)
async def import_workflow(
    project_id: int,
    workflow: WorkflowImportV1,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[WorkflowReadV1]:
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
            new_wf_task = WorkflowTaskCreateV1(
                **wf_task.dict(exclude_none=True),
            )
            # Insert task
            await _workflow_insert_task(
                **new_wf_task.dict(),
                workflow_id=db_workflow.id,
                task_id=task_id,
                db=db,
            )

    await db.close()
    return db_workflow


@router.get("/workflow/", response_model=list[WorkflowReadV1])
async def get_user_workflows(
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[WorkflowReadV1]:
    """
    Returns all the workflows of the current user
    """
    stm = select(Workflow)
    stm = stm.join(Project).where(Project.user_list.any(User.id == user.id))
    res = await db.execute(stm)
    workflow_list = res.scalars().all()
    return workflow_list
