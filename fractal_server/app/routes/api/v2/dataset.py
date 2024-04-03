import json
from pathlib import Path
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import select

from ....db import AsyncSession
from ....db import get_async_db
from ....models.v2 import DatasetV2
from ....models.v2 import JobV2
from ....models.v2 import ProjectV2
from ....schemas.v2 import DatasetCreateV2
from ....schemas.v2 import DatasetReadV2
from ....schemas.v2 import DatasetUpdateV2
from ....schemas.v2.dataset import DatasetStatusReadV2
from ....schemas.v2.dataset import WorkflowTaskStatusTypeV2
from ....security import current_active_user
from ....security import User
from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_submitted_jobs_statement
from ._aux_functions import _get_workflow_check_owner
from fractal_server.app.runner.filenames import HISTORY_FILENAME

router = APIRouter()


@router.post(
    "/project/{project_id}/dataset/",
    response_model=DatasetReadV2,
    status_code=status.HTTP_201_CREATED,
)
async def create_dataset(
    project_id: int,
    dataset: DatasetCreateV2,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[DatasetReadV2]:
    """
    Add new dataset to current project
    """
    await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    db_dataset = DatasetV2(project_id=project_id, **dataset.dict())
    db.add(db_dataset)
    await db.commit()
    await db.refresh(db_dataset)
    await db.close()

    return db_dataset


@router.get(
    "/project/{project_id}/dataset/",
    response_model=list[DatasetReadV2],
)
async def read_dataset_list(
    project_id: int,
    history: bool = True,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[list[DatasetReadV2]]:
    """
    Get dataset list for given project
    """
    # Access control
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    # Find datasets of the current project. Note: this select/where approach
    # has much better scaling than refreshing all elements of
    # `project.dataset_list` - ref
    # https://github.com/fractal-analytics-platform/fractal-server/pull/1082#issuecomment-1856676097.
    stm = select(DatasetV2).where(DatasetV2.project_id == project.id)
    res = await db.execute(stm)
    dataset_list = res.scalars().all()
    await db.close()
    if not history:
        for ds in dataset_list:
            setattr(ds, "history", [])
    return dataset_list


@router.get(
    "/project/{project_id}/dataset/{dataset_id}/",
    response_model=DatasetReadV2,
)
async def read_dataset(
    project_id: int,
    dataset_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[DatasetReadV2]:
    """
    Get info on a dataset associated to the current project
    """
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    dataset = output["dataset"]
    await db.close()
    return dataset


@router.patch(
    "/project/{project_id}/dataset/{dataset_id}/",
    response_model=DatasetReadV2,
)
async def update_dataset(
    project_id: int,
    dataset_id: int,
    dataset_update: DatasetUpdateV2,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[DatasetReadV2]:
    """
    Edit a dataset associated to the current project
    """

    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    db_dataset = output["dataset"]

    for key, value in dataset_update.dict(exclude_unset=True).items():
        setattr(db_dataset, key, value)

    await db.commit()
    await db.refresh(db_dataset)
    await db.close()
    return db_dataset


@router.delete(
    "/project/{project_id}/dataset/{dataset_id}/",
    status_code=204,
)
async def delete_dataset(
    project_id: int,
    dataset_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Delete a dataset associated to the current project
    """
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    dataset = output["dataset"]

    # Fail if there exist jobs that are submitted and in relation with the
    # current dataset.
    stm = _get_submitted_jobs_statement().where(JobV2.dataset_id == dataset_id)
    res = await db.execute(stm)
    jobs = res.scalars().all()
    if jobs:
        string_ids = str([job.id for job in jobs])[1:-1]
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Cannot delete dataset {dataset.id} because it "
                f"is linked to active job(s) {string_ids}."
            ),
        )

    # Cascade operations: set foreign-keys to null for jobs which are in
    # relationship with the current dataset
    stm = select(JobV2).where(JobV2.dataset_id == dataset_id)
    res = await db.execute(stm)
    jobs = res.scalars().all()
    for job in jobs:
        job.dataset_id = None
    await db.commit()

    # Delete dataset
    await db.delete(dataset)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/dataset/", response_model=list[DatasetReadV2])
async def get_user_datasets(
    history: bool = True,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[DatasetReadV2]:
    """
    Returns all the datasets of the current user
    """
    stm = select(DatasetV2)
    stm = stm.join(ProjectV2).where(
        ProjectV2.user_list.any(User.id == user.id)
    )

    res = await db.execute(stm)
    dataset_list = res.scalars().all()
    await db.close()
    if not history:
        for ds in dataset_list:
            setattr(ds, "history", [])
    return dataset_list


@router.get(
    "/project/{project_id}/dataset/{dataset_id}/status/",
    response_model=DatasetStatusReadV2,
)
async def get_workflowtask_status(
    project_id: int,
    dataset_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[DatasetStatusReadV2]:
    """
    Extract the status of all `WorkflowTask`s that ran on a given `DatasetV2`.
    """
    # Get the dataset DB entry
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    dataset = output["dataset"]

    # Check whether there exists a job such that
    # 1. `job.dataset_id == dataset_id`, and
    # 2. `job.status` is submitted
    # If one such job exists, it will be used later. If there are multiple
    # jobs, raise an error.
    stm = _get_submitted_jobs_statement().where(JobV2.dataset_id == dataset_id)
    res = await db.execute(stm)
    running_jobs = res.scalars().all()
    if len(running_jobs) == 0:
        running_job = None
    elif len(running_jobs) == 1:
        running_job = running_jobs[0]
    else:
        string_ids = str([job.id for job in running_jobs])[1:-1]
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Cannot get WorkflowTaskV2 statuses as DatasetV2 {dataset.id}"
                f" is linked to multiple active jobs: {string_ids}."
            ),
        )

    # Initialize empty dictionary for WorkflowTaskV2 status
    workflow_tasks_status_dict: dict = {}

    # Lowest priority: read status from DB, which corresponds to jobs that are
    # not running
    history = dataset.history
    for history_item in history:
        wftask_id = history_item["workflowtask"]["id"]
        wftask_status = history_item["status"]
        workflow_tasks_status_dict[wftask_id] = wftask_status

    # If a job is running, then gather more up-to-date information
    if running_job is not None:
        # Get the workflow DB entry
        running_workflow = await _get_workflow_check_owner(
            project_id=project_id,
            workflow_id=running_job.workflow_id,
            user_id=user.id,
            db=db,
        )
        # Mid priority: Set all WorkflowTask's that are part of the running job
        # as "submitted"
        start = running_job.first_task_index
        end = running_job.last_task_index + 1
        for wftask in running_workflow.task_list[start:end]:
            workflow_tasks_status_dict[
                wftask.id
            ] = WorkflowTaskStatusTypeV2.SUBMITTED

        # Highest priority: Read status updates coming from the running-job
        # temporary file. Note: this file only contains information on
        # # WorkflowTask's that ran through successfully.
        tmp_file = Path(running_job.working_dir) / HISTORY_FILENAME
        try:
            with tmp_file.open("r") as f:
                history = json.load(f)
        except FileNotFoundError:
            history = []
        for history_item in history:
            wftask_id = history_item["workflowtask"]["id"]
            wftask_status = history_item["status"]
            workflow_tasks_status_dict[wftask_id] = wftask_status

    response_body = DatasetStatusReadV2(status=workflow_tasks_status_dict)
    return response_body
