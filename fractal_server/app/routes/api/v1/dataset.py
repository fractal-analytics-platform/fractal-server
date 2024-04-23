import json
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import or_
from sqlmodel import select

from ....db import AsyncSession
from ....db import get_async_db
from ....models.v1 import ApplyWorkflow
from ....models.v1 import Dataset
from ....models.v1 import Project
from ....models.v1 import Resource
from ....runner.filenames import HISTORY_FILENAME
from ....schemas.v1 import DatasetCreateV1
from ....schemas.v1 import DatasetReadV1
from ....schemas.v1 import DatasetStatusReadV1
from ....schemas.v1 import DatasetUpdateV1
from ....schemas.v1 import ResourceCreateV1
from ....schemas.v1 import ResourceReadV1
from ....schemas.v1 import ResourceUpdateV1
from ....schemas.v1 import WorkflowExportV1
from ....schemas.v1 import WorkflowTaskExportV1
from ....security import current_active_user
from ....security import User
from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_submitted_jobs_statement
from ._aux_functions import _get_workflow_check_owner


router = APIRouter()


@router.post(
    "/project/{project_id}/dataset/",
    response_model=DatasetReadV1,
    status_code=status.HTTP_201_CREATED,
)
async def create_dataset(
    project_id: int,
    dataset: DatasetCreateV1,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[DatasetReadV1]:
    """
    Add new dataset to current project
    """
    await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    db_dataset = Dataset(project_id=project_id, **dataset.dict())
    db.add(db_dataset)
    await db.commit()
    await db.refresh(db_dataset)
    await db.close()

    return db_dataset


@router.get(
    "/project/{project_id}/dataset/",
    response_model=list[DatasetReadV1],
)
async def read_dataset_list(
    project_id: int,
    history: bool = True,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[list[DatasetReadV1]]:
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
    stm = select(Dataset).where(Dataset.project_id == project.id)
    res = await db.execute(stm)
    dataset_list = res.scalars().all()
    await db.close()
    if not history:
        for ds in dataset_list:
            setattr(ds, "history", [])
    return dataset_list


@router.get(
    "/project/{project_id}/dataset/{dataset_id}/",
    response_model=DatasetReadV1,
)
async def read_dataset(
    project_id: int,
    dataset_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[DatasetReadV1]:
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
    response_model=DatasetReadV1,
)
async def update_dataset(
    project_id: int,
    dataset_id: int,
    dataset_update: DatasetUpdateV1,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[DatasetReadV1]:
    """
    Edit a dataset associated to the current project
    """

    if dataset_update.history is not None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Cannot modify dataset history.",
        )

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
    stm = _get_submitted_jobs_statement().where(
        or_(
            ApplyWorkflow.input_dataset_id == dataset_id,
            ApplyWorkflow.output_dataset_id == dataset_id,
        )
    )
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
    # input_dataset
    stm = select(ApplyWorkflow).where(
        ApplyWorkflow.input_dataset_id == dataset_id
    )
    res = await db.execute(stm)
    jobs = res.scalars().all()
    for job in jobs:
        job.input_dataset_id = None
        await db.merge(job)
    await db.commit()
    # output_dataset
    stm = select(ApplyWorkflow).where(
        ApplyWorkflow.output_dataset_id == dataset_id
    )
    res = await db.execute(stm)
    jobs = res.scalars().all()
    for job in jobs:
        job.output_dataset_id = None
        await db.merge(job)
    await db.commit()

    # Delete dataset
    await db.delete(dataset)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/project/{project_id}/dataset/{dataset_id}/resource/",
    response_model=ResourceReadV1,
    status_code=status.HTTP_201_CREATED,
)
async def create_resource(
    project_id: int,
    dataset_id: int,
    resource: ResourceCreateV1,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[ResourceReadV1]:
    """
    Add resource to an existing dataset
    """
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    dataset = output["dataset"]
    db_resource = Resource(dataset_id=dataset.id, **resource.dict())
    db.add(db_resource)
    await db.commit()
    await db.refresh(db_resource)
    await db.close()
    return db_resource


@router.get(
    "/project/{project_id}/dataset/{dataset_id}/resource/",
    response_model=list[ResourceReadV1],
)
async def get_resource_list(
    project_id: int,
    dataset_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[list[ResourceReadV1]]:
    """
    Get resources from a dataset
    """
    await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    stm = select(Resource).where(Resource.dataset_id == dataset_id)
    res = await db.execute(stm)
    resource_list = res.scalars().all()
    await db.close()
    return resource_list


@router.patch(
    "/project/{project_id}/dataset/{dataset_id}/resource/{resource_id}/",
    response_model=ResourceReadV1,
)
async def update_resource(
    project_id: int,
    dataset_id: int,
    resource_id: int,
    resource_update: ResourceUpdateV1,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[ResourceReadV1]:
    """
    Edit a resource of a dataset
    """
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    dataset = output["dataset"]
    orig_resource = await db.get(Resource, resource_id)

    if orig_resource not in dataset.resource_list:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Resource {resource_id} is not part of "
                f"dataset {dataset_id}"
            ),
        )

    for key, value in resource_update.dict(exclude_unset=True).items():
        setattr(orig_resource, key, value)
    await db.commit()
    await db.refresh(orig_resource)
    await db.close()
    return orig_resource


@router.delete(
    "/project/{project_id}/dataset/{dataset_id}/resource/{resource_id}/",
    status_code=204,
)
async def delete_resource(
    project_id: int,
    dataset_id: int,
    resource_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Delete a resource of a dataset
    """
    # Get the dataset DB entry
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    dataset = output["dataset"]
    resource = await db.get(Resource, resource_id)
    if not resource or resource.dataset_id != dataset.id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Resource does not exist or does not belong to dataset",
        )
    await db.delete(resource)
    await db.commit()
    await db.close()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/project/{project_id}/dataset/{dataset_id}/export_history/",
    response_model=WorkflowExportV1,
)
async def export_history_as_workflow(
    project_id: int,
    dataset_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[WorkflowExportV1]:
    """
    Extract a reproducible workflow from the dataset history.
    """
    # Get the dataset DB entry
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    dataset = output["dataset"]

    # Check whether there exists a submitted job such that
    # `job.output_dataset_id==dataset_id`.
    # If at least one such job exists, then this endpoint will fail.
    # We do not support the use case of exporting a reproducible workflow when
    # job execution is in progress; this may change in the future.
    stm = _get_submitted_jobs_statement().where(
        ApplyWorkflow.output_dataset_id == dataset_id
    )
    res = await db.execute(stm)
    jobs = res.scalars().all()
    if jobs:
        string_ids = str([job.id for job in jobs])[1:-1]
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Cannot export history because dataset {dataset.id} "
                f"is linked to active job(s) {string_ids}."
            ),
        )

    # It such a job does not exist, continue with the endpoint. Note that this
    # means that the history in the DB is up-to-date.

    # Read history from DB
    history = dataset.history

    # Construct reproducible workflow
    task_list = []
    for history_item in history:
        wftask = history_item["workflowtask"]
        wftask_status = history_item["status"]
        if wftask_status == "done":
            task_list.append(WorkflowTaskExportV1(**wftask))

    def _slugify_dataset_name(_name: str) -> str:
        _new_name = _name
        for char in (" ", ".", "/", "\\"):
            _new_name = _new_name.replace(char, "_")
        return _new_name

    name = f"history_{_slugify_dataset_name(dataset.name)}"

    workflow = WorkflowExportV1(name=name, task_list=task_list)
    return workflow


@router.get(
    "/project/{project_id}/dataset/{dataset_id}/status/",
    response_model=DatasetStatusReadV1,
)
async def get_workflowtask_status(
    project_id: int,
    dataset_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[DatasetStatusReadV1]:
    """
    Extract the status of all `WorkflowTask`s that ran on a given `Dataset`.
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
    # 1. `job.output_dataset_id == dataset_id`, and
    # 2. `job.status` is either submitted or running.
    # If one such job exists, it will be used later. If there are multiple
    # jobs, raise an error.
    # Note: see
    # https://sqlmodel.tiangolo.com/tutorial/where/#type-annotations-and-errors
    # regarding the type-ignore in this code block
    stm = _get_submitted_jobs_statement().where(
        ApplyWorkflow.output_dataset_id == dataset_id
    )
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
                f"Cannot get WorkflowTask statuses as dataset {dataset.id} "
                f"is linked to multiple active jobs: {string_ids}"
            ),
        )

    # Initialize empty dictionary for workflowtasks status
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
            workflow_tasks_status_dict[wftask.id] = "submitted"

        # Highest priority: Read status updates coming from the running-job
        # temporary file. Note: this file only contains information on
        # WorkflowTask's that ran through successfully
        tmp_file = Path(running_job.working_dir) / HISTORY_FILENAME
        try:
            with tmp_file.open("r") as f:
                history = json.load(f)
        except FileNotFoundError:
            history = []
        except JSONDecodeError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="History file does not include a valid JSON.",
            )

        for history_item in history:
            wftask_id = history_item["workflowtask"]["id"]
            wftask_status = history_item["status"]
            workflow_tasks_status_dict[wftask_id] = wftask_status

    response_body = DatasetStatusReadV1(status=workflow_tasks_status_dict)
    return response_body


@router.get("/dataset/", response_model=list[DatasetReadV1])
async def get_user_datasets(
    history: bool = True,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[DatasetReadV1]:
    """
    Returns all the datasets of the current user
    """
    stm = select(Dataset)
    stm = stm.join(Project).where(Project.user_list.any(User.id == user.id))
    res = await db.execute(stm)
    dataset_list = res.scalars().all()
    await db.close()
    if not history:
        for ds in dataset_list:
            setattr(ds, "history", [])
    return dataset_list
