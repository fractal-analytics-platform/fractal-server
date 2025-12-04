import os
from pathlib import Path

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.routes.auth import current_user_act_ver_prof
from fractal_server.app.schemas.v2 import DatasetCreate
from fractal_server.app.schemas.v2 import DatasetRead
from fractal_server.app.schemas.v2 import DatasetUpdate
from fractal_server.app.schemas.v2.dataset import DatasetExport
from fractal_server.app.schemas.v2.dataset import DatasetImport
from fractal_server.app.schemas.v2.sharing import ProjectPermissions
from fractal_server.string_tools import sanitize_string
from fractal_server.urls import normalize_url

from ._aux_functions import _get_dataset_check_access
from ._aux_functions import _get_project_check_access
from ._aux_functions import _get_submitted_jobs_statement

router = APIRouter()


@router.post(
    "/project/{project_id}/dataset/",
    response_model=DatasetRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_dataset(
    project_id: int,
    dataset: DatasetCreate,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> DatasetRead | None:
    """
    Add new dataset to current project
    """
    project = await _get_project_check_access(
        project_id=project_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.WRITE,
        db=db,
    )

    db_dataset = DatasetV2(
        project_id=project_id,
        zarr_dir="__PLACEHOLDER__",
        **dataset.model_dump(exclude={"project_dir", "zarr_subfolder"}),
    )
    db.add(db_dataset)
    await db.commit()
    await db.refresh(db_dataset)

    if dataset.project_dir is None:
        project_dir = user.project_dirs[0]
    else:
        if dataset.project_dir not in user.project_dirs:
            await db.delete(db_dataset)
            await db.commit()
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"You are not allowed to use {dataset.project_dir=}.",
            )
        project_dir = dataset.project_dir

    if dataset.zarr_subfolder is None:
        zarr_subfolder = (
            f"fractal/{project_id}_{sanitize_string(project.name)}/"
            f"{db_dataset.id}_{sanitize_string(db_dataset.name)}"
        )
    else:
        zarr_subfolder = dataset.zarr_subfolder

    zarr_dir = os.path.join(project_dir, zarr_subfolder)
    db_dataset.zarr_dir = normalize_url(zarr_dir)

    db.add(db_dataset)
    await db.commit()
    await db.refresh(db_dataset)

    return db_dataset


@router.get(
    "/project/{project_id}/dataset/",
    response_model=list[DatasetRead],
)
async def read_dataset_list(
    project_id: int,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> list[DatasetRead] | None:
    """
    Get dataset list for given project
    """
    # Access control
    project = await _get_project_check_access(
        project_id=project_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.READ,
        db=db,
    )
    # Find datasets of the current project. Note: this select/where approach
    # has much better scaling than refreshing all elements of
    # `project.dataset_list` - ref
    # https://github.com/fractal-analytics-platform/fractal-server/pull/1082#issuecomment-1856676097.
    stm = select(DatasetV2).where(DatasetV2.project_id == project.id)
    res = await db.execute(stm)
    dataset_list = res.scalars().all()
    return dataset_list


@router.get(
    "/project/{project_id}/dataset/{dataset_id}/",
    response_model=DatasetRead,
)
async def read_dataset(
    project_id: int,
    dataset_id: int,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> DatasetRead | None:
    """
    Get info on a dataset associated to the current project
    """
    output = await _get_dataset_check_access(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.READ,
        db=db,
    )
    dataset = output["dataset"]
    return dataset


@router.patch(
    "/project/{project_id}/dataset/{dataset_id}/",
    response_model=DatasetRead,
)
async def update_dataset(
    project_id: int,
    dataset_id: int,
    dataset_update: DatasetUpdate,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> DatasetRead | None:
    """
    Edit a dataset associated to the current project
    """

    output = await _get_dataset_check_access(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.WRITE,
        db=db,
    )
    db_dataset = output["dataset"]

    for key, value in dataset_update.model_dump(exclude_unset=True).items():
        setattr(db_dataset, key, value)

    await db.commit()
    await db.refresh(db_dataset)
    return db_dataset


@router.delete(
    "/project/{project_id}/dataset/{dataset_id}/",
    status_code=204,
)
async def delete_dataset(
    project_id: int,
    dataset_id: int,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Delete a dataset associated to the current project
    """
    output = await _get_dataset_check_access(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.WRITE,
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
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Cannot delete dataset {dataset.id} because it "
                f"is linked to active job(s) {string_ids}."
            ),
        )

    # Delete dataset
    await db.delete(dataset)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/project/{project_id}/dataset/{dataset_id}/export/",
    response_model=DatasetExport,
)
async def export_dataset(
    project_id: int,
    dataset_id: int,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> DatasetExport | None:
    """
    Export an existing dataset
    """
    dict_dataset_project = await _get_dataset_check_access(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.READ,
        db=db,
    )
    dataset = dict_dataset_project["dataset"]

    return dataset


@router.post(
    "/project/{project_id}/dataset/import/",
    response_model=DatasetRead,
    status_code=status.HTTP_201_CREATED,
)
async def import_dataset(
    project_id: int,
    dataset: DatasetImport,
    user: UserOAuth = Depends(current_user_act_ver_prof),
    db: AsyncSession = Depends(get_async_db),
) -> DatasetRead | None:
    """
    Import an existing dataset into a project
    """

    # Preliminary checks
    await _get_project_check_access(
        project_id=project_id,
        user_id=user.id,
        required_permissions=ProjectPermissions.WRITE,
        db=db,
    )

    if not any(
        Path(dataset.zarr_dir).is_relative_to(project_dir)
        for project_dir in user.project_dirs
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"{dataset.zarr_dir=} is not relative to any of user's project "
                "dirs."
            ),
        )

    # Create new Dataset
    db_dataset = DatasetV2(
        project_id=project_id,
        **dataset.model_dump(exclude_none=True),
    )
    db.add(db_dataset)
    await db.commit()
    await db.refresh(db_dataset)

    return db_dataset
