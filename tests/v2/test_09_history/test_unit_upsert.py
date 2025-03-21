import time
from typing import Any

import pytest
from devtools import debug
from sqlalchemy.orm import Session
from sqlmodel import select

from fractal_server.app.history.status_enum import XXXStatus
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.runner.v2._db_tools import (
    bulk_upsert_image_cache_fast,
)


def bulk_upsert_image_cache_slow(
    *, db: Session, list_upsert_objects: list[dict[str, Any]]
) -> None:
    for obj in list_upsert_objects:
        db.merge(HistoryImageCache(**obj))
    db.commit()


@pytest.mark.parametrize(
    "bulk_upsert_image_cache_function",
    [bulk_upsert_image_cache_fast, bulk_upsert_image_cache_slow],
)
async def test_upsert_function(
    project_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflowtask_factory_v2,
    db_sync,
    bulk_upsert_image_cache_function,
    MockCurrentUser,
):
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id)
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(user_id=user.id)

        wftask = await workflowtask_factory_v2(
            workflow_id=workflow.id, task_id=task.id
        )
        run = HistoryRun(
            workflowtask_id=wftask.id,
            dataset_id=dataset.id,
            workflowtask_dump={},
            task_group_dump={},
            num_available_images=3,
            status=XXXStatus.SUBMITTED,
        )
        db_sync.add(run)
        db_sync.commit()
        db_sync.refresh(run)

        NUM = 200
        OLD_ZARR_URLS = [f"/already-there/{i:05d}" for i in range(NUM)]
        NEW_ZARR_URLS = [f"/not-there/{i:05d}" for i in range(NUM)]

        # Create an `HistoryImageCache` that should be updated
        unit1 = HistoryUnit(
            history_run_id=run.id,
            status=XXXStatus.SUBMITTED,
            zarr_urls=OLD_ZARR_URLS,
        )
        db_sync.add(unit1)
        db_sync.commit()
        db_sync.refresh(unit1)
        db_sync.add_all(
            [
                HistoryImageCache(
                    zarr_url=zarr_url,
                    dataset_id=dataset.id,
                    workflowtask_id=wftask.id,
                    latest_history_unit_id=unit1.id,
                )
                for zarr_url in OLD_ZARR_URLS
            ]
        )
        db_sync.commit()

        # Create `HistoryImageCache` rows that should be inserted
        unit2 = HistoryUnit(
            history_run_id=run.id,
            status=XXXStatus.DONE,
            zarr_urls=OLD_ZARR_URLS + NEW_ZARR_URLS,
        )
        db_sync.add(unit2)
        db_sync.commit()
        db_sync.refresh(unit2)

        # Run upsert function
        list_upsert_objects = [
            {
                "zarr_url": zarr_url,
                "dataset_id": dataset.id,
                "workflowtask_id": wftask.id,
                "latest_history_unit_id": unit2.id,
            }
            for zarr_url in sorted(OLD_ZARR_URLS + NEW_ZARR_URLS)
        ]

        t0 = time.perf_counter()
        bulk_upsert_image_cache_function(
            db=db_sync,
            list_upsert_objects=list_upsert_objects,
        )
        elapsed_time = time.perf_counter() - t0
        debug(bulk_upsert_image_cache_function)
        debug(elapsed_time)

        # Assert correctness
        caches = (
            db_sync.execute(
                select(HistoryImageCache).order_by(HistoryImageCache.zarr_url)
            )
            .scalars()
            .all()
        )
        actual_caches = [cache.model_dump() for cache in caches]
        expected_chaches = [
            {
                "zarr_url": zarr_url,
                "dataset_id": dataset.id,
                "workflowtask_id": wftask.id,
                "latest_history_unit_id": unit2.id,
            }
            for zarr_url in (OLD_ZARR_URLS + NEW_ZARR_URLS)
        ]
        assert actual_caches == expected_chaches
