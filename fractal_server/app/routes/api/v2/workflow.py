from copy import copy

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from pydantic import BaseModel
from sqlmodel import select

from ....db import AsyncSession
from ....db import get_async_db
from ....models.v2 import JobV2
from ....models.v2 import ProjectV2
from ....models.v2 import WorkflowV2
from ....schemas.v2 import WorkflowCreateV2
from ....schemas.v2 import WorkflowExportV2
from ....schemas.v2 import WorkflowReadV2
from ....schemas.v2 import WorkflowReadV2WithWarnings
from ....schemas.v2 import WorkflowUpdateV2
from ._aux_functions import _check_workflow_exists
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_submitted_jobs_statement
from ._aux_functions import _get_workflow_check_owner
from ._aux_functions import _workflow_has_submitted_job
from ._aux_functions_tasks import _add_warnings_to_workflow_tasks
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.routes.auth import current_active_user
from fractal_server.images.tools import merge_type_filters

router = APIRouter()


@router.get(
    "/project/{project_id}/workflow/",
    response_model=list[WorkflowReadV2],
)
async def get_workflow_list(
    project_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[WorkflowReadV2] | None:
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
    stm = select(WorkflowV2).where(WorkflowV2.project_id == project.id)
    workflow_list = (await db.execute(stm)).scalars().all()
    return workflow_list


@router.post(
    "/project/{project_id}/workflow/",
    response_model=WorkflowReadV2,
    status_code=status.HTTP_201_CREATED,
)
async def create_workflow(
    project_id: int,
    workflow: WorkflowCreateV2,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> WorkflowReadV2 | None:
    """
    Create a workflow, associate to a project
    """
    await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    await _check_workflow_exists(
        name=workflow.name, project_id=project_id, db=db
    )

    db_workflow = WorkflowV2(project_id=project_id, **workflow.model_dump())
    db.add(db_workflow)
    await db.commit()
    await db.refresh(db_workflow)
    await db.close()
    return db_workflow


@router.get(
    "/project/{project_id}/workflow/{workflow_id}/",
    response_model=WorkflowReadV2WithWarnings,
)
async def read_workflow(
    project_id: int,
    workflow_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> WorkflowReadV2WithWarnings | None:
    """
    Get info on an existing workflow
    """

    workflow = await _get_workflow_check_owner(
        project_id=project_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )

    wftask_list_with_warnings = await _add_warnings_to_workflow_tasks(
        wftask_list=workflow.task_list, user_id=user.id, db=db
    )
    workflow_data = dict(
        **workflow.model_dump(),
        project=workflow.project,
        task_list=wftask_list_with_warnings,
    )

    return workflow_data


@router.patch(
    "/project/{project_id}/workflow/{workflow_id}/",
    response_model=WorkflowReadV2WithWarnings,
)
async def update_workflow(
    project_id: int,
    workflow_id: int,
    patch: WorkflowUpdateV2,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> WorkflowReadV2WithWarnings | None:
    """
    Edit a workflow
    """
    workflow = await _get_workflow_check_owner(
        project_id=project_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )

    if patch.name:
        await _check_workflow_exists(
            name=patch.name, project_id=project_id, db=db
        )

    for key, value in patch.model_dump(exclude_unset=True).items():
        if key == "reordered_workflowtask_ids":
            if await _workflow_has_submitted_job(
                workflow_id=workflow_id, db=db
            ):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        "Cannot re-order WorkflowTasks while a Job is running "
                        "for this Workflow."
                    ),
                )

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

    wftask_list_with_warnings = await _add_warnings_to_workflow_tasks(
        wftask_list=workflow.task_list, user_id=user.id, db=db
    )
    workflow_data = dict(
        **workflow.model_dump(),
        project=workflow.project,
        task_list=wftask_list_with_warnings,
    )

    return workflow_data


@router.delete(
    "/project/{project_id}/workflow/{workflow_id}/",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_workflow(
    project_id: int,
    workflow_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Delete a workflow
    """

    workflow = await _get_workflow_check_owner(
        project_id=project_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )

    # Fail if there exist jobs that are submitted and in relation with the
    # current workflow.
    stm = _get_submitted_jobs_statement().where(
        JobV2.workflow_id == workflow.id
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

    # Delete workflow
    await db.delete(workflow)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/project/{project_id}/workflow/{workflow_id}/export/",
    response_model=WorkflowExportV2,
)
async def export_workflow(
    project_id: int,
    workflow_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> WorkflowExportV2 | None:
    """
    Export an existing workflow, after stripping all IDs
    """
    workflow = await _get_workflow_check_owner(
        project_id=project_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )
    wf_task_list = []
    for wftask in workflow.task_list:
        task_group = await db.get(TaskGroupV2, wftask.task.taskgroupv2_id)
        wf_task_list.append(wftask.model_dump())
        wf_task_list[-1]["task"] = dict(
            pkg_name=task_group.pkg_name,
            version=task_group.version,
            name=wftask.task.name,
        )

    wf = WorkflowExportV2(
        **workflow.model_dump(),
        task_list=wf_task_list,
    )
    return wf


@router.get("/workflow/", response_model=list[WorkflowReadV2])
async def get_user_workflows(
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[WorkflowReadV2]:
    """
    Returns all the workflows of the current user
    """
    stm = select(WorkflowV2)
    stm = stm.join(ProjectV2).where(
        ProjectV2.user_list.any(UserOAuth.id == user.id)
    )
    res = await db.execute(stm)
    workflow_list = res.scalars().all()
    return workflow_list


class WorkflowTaskTypeFiltersInfo(BaseModel):
    workflowtask_id: int
    current_type_filters: dict[str, bool]
    input_type_filters: dict[str, bool]
    output_type_filters: dict[str, bool]


@router.get("/project/{project_id}/workflow/{workflow_id}/type-filters-flow/")
async def get_workflow_type_filters(
    project_id: int,
    workflow_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[WorkflowTaskTypeFiltersInfo]:
    """
    Get info on type/type-filters flow for a workflow.
    """

    workflow = await _get_workflow_check_owner(
        project_id=project_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )

    num_tasks = len(workflow.task_list)
    if num_tasks == 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Workflow has no tasks.",
        )

    current_type_filters = {}

    response_items = []
    for wftask in workflow.task_list:
        # Compute input_type_filters, based on wftask and task manifest
        input_type_filters = merge_type_filters(
            wftask_type_filters=wftask.type_filters,
            task_input_types=wftask.task.input_types,
        )

        # Append current item to response list
        response_items.append(
            dict(
                workflowtask_id=wftask.id,
                current_type_filters=copy(current_type_filters),
                input_type_filters=copy(input_type_filters),
                output_type_filters=copy(wftask.task.output_types),
            )
        )

        # Update `current_type_filters`
        current_type_filters.update(wftask.task.output_types)

    return response_items
