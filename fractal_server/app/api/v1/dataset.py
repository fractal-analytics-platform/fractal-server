from typing import Optional

from fastapi import APIRouter  # type: ignore[import]
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import or_  # type: ignore[import]
from sqlmodel import select

from ....common.schemas import DatasetCreate
from ....common.schemas import DatasetRead
from ....common.schemas import DatasetUpdate
from ....common.schemas import ResourceCreate
from ....common.schemas import ResourceRead
from ....common.schemas import ResourceUpdate
from ....common.schemas import WorkflowExport
from ....common.schemas import WorkflowTaskExport
from ...db import AsyncSession
from ...db import get_db
from ...models import ApplyWorkflow
from ...models import Dataset
from ...models import JobStatusType
from ...models import Resource
from ...security import current_active_user
from ...security import User
from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_project_check_owner


router = APIRouter()


@router.post(
    "/project/{project_id}/dataset/",
    response_model=DatasetRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_dataset(
    project_id: int,
    dataset: DatasetCreate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[DatasetRead]:
    """
    Add new dataset to current project
    """
    await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db  # type: ignore[arg-type]
    )
    db_dataset = Dataset(project_id=project_id, **dataset.dict())
    db.add(db_dataset)
    await db.commit()
    await db.refresh(db_dataset)
    await db.close()

    return db_dataset


@router.get(
    "/project/{project_id}/dataset/{dataset_id}",
    response_model=DatasetRead,
)
async def read_dataset(
    project_id: int,
    dataset_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[DatasetRead]:
    """
    Get info on a dataset associated to the current project
    """
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,  # type: ignore[arg-type]
        db=db,
    )
    dataset = output["dataset"]
    await db.close()
    return dataset


@router.patch(
    "/project/{project_id}/dataset/{dataset_id}",
    response_model=DatasetRead,
)
async def update_dataset(
    project_id: int,
    dataset_id: int,
    dataset_update: DatasetUpdate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[DatasetRead]:
    """
    Edit a dataset associated to the current project
    """
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,  # type: ignore[arg-type]
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
    "/project/{project_id}/dataset/{dataset_id}",
    status_code=204,
)
async def delete_dataset(
    project_id: int,
    dataset_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """
    Delete a dataset associated to the current project
    """
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,  # type: ignore[arg-type]
        db=db,
    )
    dataset = output["dataset"]

    # Check that no ApplyWorkflow is in relationship with the current Dataset
    stm = select(ApplyWorkflow).filter(
        or_(
            ApplyWorkflow.input_dataset_id == dataset_id,
            ApplyWorkflow.output_dataset_id == dataset_id,
        )
    )
    res = await db.execute(stm)
    job = res.scalars().first()
    if job:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Cannot remove dataset {dataset_id}: "
                f"it's still linked to job {job.id}."
            ),
        )

    await db.delete(dataset)
    await db.commit()
    await db.close()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/project/{project_id}/dataset/{dataset_id}/resource/",
    response_model=ResourceRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_resource(
    project_id: int,
    dataset_id: int,
    resource: ResourceCreate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[ResourceRead]:
    """
    Add resource to an existing dataset
    """
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,  # type: ignore[arg-type]
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
    response_model=list[ResourceRead],
)
async def get_resource_list(
    project_id: int,
    dataset_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[list[ResourceRead]]:
    """
    Get resources from a dataset
    """
    await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,  # type: ignore[arg-type]
        db=db,
    )
    stm = select(Resource).where(Resource.dataset_id == dataset_id)
    res = await db.execute(stm)
    resource_list = res.scalars().all()
    await db.close()
    return resource_list


@router.patch(
    "/project/{project_id}/dataset/{dataset_id}/resource/{resource_id}",
    response_model=ResourceRead,
)
async def update_resource(
    project_id: int,
    dataset_id: int,
    resource_id: int,
    resource_update: ResourceUpdate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[ResourceRead]:
    """
    Edit a resource of a dataset
    """
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,  # type: ignore[arg-type]
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
    "/project/{project_id}/dataset/{dataset_id}/resource/{resource_id}",
    status_code=204,
)
async def delete_resource(
    project_id: int,
    dataset_id: int,
    resource_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """
    Delete a resource of a dataset
    """
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db  # type: ignore[arg-type]
    )
    resource = await db.get(Resource, resource_id)
    if not resource or resource.dataset_id not in (
        ds.id for ds in project.dataset_list
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Resource does not exist or does not belong to project",
        )
    await db.delete(resource)
    await db.commit()
    await db.close()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    ("/project/{project_id}/dataset/{dataset_id}/export_history"),
    response_model=WorkflowExport,
)
async def export_history_as_workflow(
    project_id: int,
    dataset_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowExport]:
    """
    Extract a reproducible workflow from the dataset history
    """
    # Get the dataset DB entry
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,  # type: ignore[arg-type]
        db=db,
    )
    dataset = output["dataset"]

    # Check whether there exists a job such that
    # 1. `job.output_dataset_id == dataset_id`
    # 2. `job.status` is either submitted or running
    # Note: see
    # https://sqlmodel.tiangolo.com/tutorial/where/#type-annotations-and-errors
    # regarding the type-ignore in this code block
    stm = (
        select(ApplyWorkflow)
        .where(ApplyWorkflow.output_dataset_id == dataset_id)
        .where(
            ApplyWorkflow.status.in_(  # type: ignore
                [JobStatusType.SUBMITTED, JobStatusType.RUNNING]
            )
        )
    )
    res = await db.execute(stm)

    # If at least one such job exists, then this endpoint should fail (what
    # would "extract a reproducible workflow" mean when execution is
    # in-progress?)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="FIXME",
        )

    # It such a job does not exist, continue with the endpoint. Note that this
    # means that the history in the DB is up-to-date.

    # Read history from DB
    history_next = dataset.meta["history_next"]

    # Construct reproducible workflow
    task_list = []
    for history_item in history_next:
        wftask = history_item["workflowtask"]
        wftask_status = history_item["status"]
        if wftask_status == "done":
            task_list.append(WorkflowTaskExport(**wftask))

    workflow = WorkflowExport(task_list=task_list)
    return workflow
