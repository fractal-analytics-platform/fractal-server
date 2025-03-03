from fastapi import APIRouter
from fastapi import Depends
from sqlmodel import delete
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import HistoryItemV2
from fractal_server.app.models.v2 import ImageStatus
from fractal_server.app.routes.auth import current_active_superuser

router = APIRouter()


@router.post("/image-status/", response_model=list[ImageStatus])
async def populate_image_status(
    workflowtask_id: int,
    dataset_id: int,
    user: UserOAuth = Depends(current_active_superuser),
    db: AsyncSession = Depends(get_async_db),
) -> list[ImageStatus]:

    stm = (
        delete(ImageStatus)
        .where(ImageStatus.workflowtask_id == workflowtask_id)
        .where(ImageStatus.dataset_id == dataset_id)
    )
    await db.execute(stm)

    stm = (
        select(HistoryItemV2)
        .where(HistoryItemV2.workflowtask_id == workflowtask_id)
        .where(HistoryItemV2.dataset_id == dataset_id)
        .order_by(HistoryItemV2.timestamp_started.desc())
    )
    res = await db.execute(stm)
    history_items = res.scalars().all()

    images = set()
    for history_item in history_items:
        common_args = {
            "workflowtask_id": history_item.workflowtask_id,
            "dataset_id": history_item.dataset_id,
            "parameters_hash": history_item.parameters_hash,
        }
        new_images = {
            k: v for k, v in history_item.images.items() if k not in images
        }
        for image, status in new_images.items():
            db.add(ImageStatus(zarr_url=image, status=status, **common_args))
        images.update(new_images.keys())

    await db.commit()

    stm = (
        select(ImageStatus)
        .where(ImageStatus.workflowtask_id == workflowtask_id)
        .where(ImageStatus.dataset_id == dataset_id)
    )
    res = await db.execute(stm)
    images_statuses = res.scalars().all()

    return images_statuses
