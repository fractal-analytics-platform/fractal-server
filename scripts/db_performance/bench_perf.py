import time

from sqlalchemy import select
from sqlalchemy.orm import Session

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import HistoryImageCache
from fractal_server.images.status_tools import _prepare_query


def get_zarr_urls(db: Session):
    res = db.execute(
        select(
            HistoryImageCache.zarr_url,
        )
    )
    zarr_urls = [z_urls[0] for z_urls in res.all()]
    return zarr_urls


def measure_query_time(
    dataset_id: int, wftask_id: int, zarr_urls: list[int], db: Session
):
    stm = _prepare_query(
        dataset_id=dataset_id, workflowtask_id=wftask_id, zarr_urls=zarr_urls
    )
    start = time.perf_counter()
    res = db.execute(stm)
    _ = res.all()
    end = time.perf_counter()

    print(end - start)


if __name__ == "__main__":
    with next(get_sync_db()) as db:
        DATASET_ID = 1
        WORKFLOWTASK_ID = 1
        zarr_urls = get_zarr_urls(db=db)
        measure_query_time(
            dataset_id=DATASET_ID,
            wftask_id=WORKFLOWTASK_ID,
            zarr_urls=zarr_urls,
            db=db,
        )
