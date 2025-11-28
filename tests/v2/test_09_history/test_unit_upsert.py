import time
from typing import Any

import pytest
from devtools import debug
from sqlalchemy.orm import Session
from sqlmodel import select

from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.runner.v2.db_tools import bulk_upsert_image_cache_fast


def bulk_upsert_image_cache_slow(
    *, db: Session, list_upsert_objects: list[dict[str, Any]]
) -> None:
    for obj in list_upsert_objects:
        db.merge(HistoryImageCache(**obj))
    db.commit()


@pytest.mark.parametrize(
    "bulk_upsert_image_cache_function,num",
    [
        (bulk_upsert_image_cache_fast, 100),
        (bulk_upsert_image_cache_slow, 100),
        (bulk_upsert_image_cache_fast, 3_500),
    ],
)
async def test_upsert_function(
    project_factory,
    workflow_factory,
    task_factory,
    dataset_factory,
    workflowtask_factory,
    job_factory,
    db_sync,
    db,
    MockCurrentUser,
    bulk_upsert_image_cache_function,
    num,
):
    async with MockCurrentUser() as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(user_id=user.id)

        wftask = await workflowtask_factory(
            workflow_id=workflow.id, task_id=task.id
        )
        job = await job_factory(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir="/foo",
            status="done",
        )
        run = HistoryRun(
            workflowtask_id=wftask.id,
            dataset_id=dataset.id,
            workflowtask_dump={},
            task_group_dump={},
            num_available_images=3,
            status=HistoryUnitStatus.SUBMITTED,
            job_id=job.id,
        )
        db_sync.add(run)
        db_sync.commit()
        db_sync.refresh(run)

        OLD_ZARR_URLS = [f"/already-there/{i:05d}" for i in range(num)]
        NEW_ZARR_URLS = [f"/not-there/{i:05d}" for i in range(num)]

        # Create an `HistoryImageCache` that should be updated
        unit1 = HistoryUnit(
            history_run_id=run.id,
            status=HistoryUnitStatus.SUBMITTED,
            zarr_urls=OLD_ZARR_URLS,
            logfile="/fake/log",
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
            status=HistoryUnitStatus.DONE,
            logfile="/fake/log",
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
