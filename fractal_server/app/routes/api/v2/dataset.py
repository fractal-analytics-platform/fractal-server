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
from ....schemas.v2.dataset import DatasetExportV2
from ....schemas.v2.dataset import DatasetImportV2
from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_submitted_jobs_statement
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_user
from fractal_server.string_tools import sanitize_string
from fractal_server.urls import normalize_url

router = APIRouter()


@router.post(
    "/project/{project_id}/dataset/",
    response_model=DatasetReadV2,
    status_code=status.HTTP_201_CREATED,
)
async def create_dataset(
    project_id: int,
    dataset: DatasetCreateV2,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> DatasetReadV2 | None:
    """
    Add new dataset to current project
    """
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )

    if dataset.zarr_dir is None:
        if user.settings.project_dir is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Both 'dataset.zarr_dir' and 'user.settings.project_dir' "
                    "are null"
                ),
            )

        db_dataset = DatasetV2(
            project_id=project_id,
            zarr_dir="__PLACEHOLDER__",
            **dataset.model_dump(exclude={"zarr_dir"}),
        )
        db.add(db_dataset)
        await db.commit()
        await db.refresh(db_dataset)
        path = (
            f"{user.settings.project_dir}/fractal/"
            f"{project_id}_{sanitize_string(project.name)}/"
            f"{db_dataset.id}_{sanitize_string(db_dataset.name)}"
        )
        normalized_path = normalize_url(path)
        db_dataset.zarr_dir = normalized_path

        db.add(db_dataset)
        await db.commit()
        await db.refresh(db_dataset)
    else:
        db_dataset = DatasetV2(project_id=project_id, **dataset.model_dump())
        db.add(db_dataset)
        await db.commit()
        await db.refresh(db_dataset)

    return db_dataset


@router.get(
    "/project/{project_id}/dataset/",
    response_model=list[DatasetReadV2],
)
async def read_dataset_list(
    project_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[DatasetReadV2] | None:
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
    return dataset_list


@router.get(
    "/project/{project_id}/dataset/{dataset_id}/",
    response_model=DatasetReadV2,
)
async def read_dataset(
    project_id: int,
    dataset_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> DatasetReadV2 | None:
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
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> DatasetReadV2 | None:
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

    if (dataset_update.zarr_dir is not None) and (len(db_dataset.images) != 0):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Cannot modify `zarr_dir` because the dataset has a non-empty "
                "image list."
            ),
        )

    for key, value in dataset_update.model_dump(exclude_unset=True).items():
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
    user: UserOAuth = Depends(current_active_user),
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

    # Delete dataset
    await db.delete(dataset)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/dataset/", response_model=list[DatasetReadV2])
async def get_user_datasets(
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[DatasetReadV2]:
    """
    Returns all the datasets of the current user
    """
    stm = select(DatasetV2)
    stm = stm.join(ProjectV2).where(
        ProjectV2.user_list.any(UserOAuth.id == user.id)
    )

    res = await db.execute(stm)
    dataset_list = res.scalars().all()
    await db.close()
    return dataset_list


@router.get(
    "/project/{project_id}/dataset/{dataset_id}/export/",
    response_model=DatasetExportV2,
)
async def export_dataset(
    project_id: int,
    dataset_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> DatasetExportV2 | None:
    """
    Export an existing dataset
    """
    dict_dataset_project = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    await db.close()

    dataset = dict_dataset_project["dataset"]

    return dataset


@router.post(
    "/project/{project_id}/dataset/import/",
    response_model=DatasetReadV2,
    status_code=status.HTTP_201_CREATED,
)
async def import_dataset(
    project_id: int,
    dataset: DatasetImportV2,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> DatasetReadV2 | None:
    """
    Import an existing dataset into a project
    """

    # Preliminary checks
    await _get_project_check_owner(
        project_id=project_id,
        user_id=user.id,
        db=db,
    )

    for image in dataset.images:
        if not image.zarr_url.startswith(dataset.zarr_dir):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"Cannot import dataset: zarr_url {image.zarr_url} is not "
                    f"relative to zarr_dir={dataset.zarr_dir}."
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
    await db.close()

    return db_dataset
