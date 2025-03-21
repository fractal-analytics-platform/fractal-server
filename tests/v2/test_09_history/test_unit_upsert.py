from typing import Any

import pytest
from sqlalchemy.orm import Session
from sqlmodel import select

from fractal_server.app.history.status_enum import XXXStatus
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.runner.v2.runner_functions import (
    bulk_upsert_image_cache_fast,
)


def bulk_upsert_image_cache_slow(
    *, db: Session, list_upsert_objects: list[dict[str, Any]]
) -> None:
    for obj in list_upsert_objects:
        db.merge(HistoryImageCache(**obj))
    db.commit()


@pytest.mark.parametrize(
    "upsert_function",
    [bulk_upsert_image_cache_fast, bulk_upsert_image_cache_slow],
)
async def test_upsert_function(
    project_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflowtask_factory_v2,
    db_sync,
    upsert_function,
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

        unit1 = HistoryUnit(
            history_run_id=run.id,
            status=XXXStatus.SUBMITTED,
            logfile="/log/a",
            zarr_urls=["/a"],
        )
        unit2 = HistoryUnit(
            history_run_id=run.id,
            status=XXXStatus.DONE,
            logfile="/log/b",
            zarr_urls=["/a", "/b"],
        )
        db_sync.add_all([unit1, unit2])
        db_sync.commit()
        db_sync.refresh(unit1)
        db_sync.refresh(unit2)

        cache1 = HistoryImageCache(
            zarr_url="/a",
            dataset_id=dataset.id,
            workflowtask_id=wftask.id,
            latest_history_unit_id=unit1.id,
        )
        db_sync.add(cache1)
        db_sync.commit()

        upsert_function(
            db_sync,
            [
                {
                    "zarr_url": "/a",
                    "dataset_id": dataset.id,
                    "workflowtask_id": wftask.id,
                    "latest_history_unit_id": unit2.id,
                },
                {
                    "zarr_url": "/b",
                    "dataset_id": dataset.id,
                    "workflowtask_id": wftask.id,
                    "latest_history_unit_id": unit2.id,
                },
            ],
        )

        caches = (
            db_sync.execute(
                select(HistoryImageCache).order_by(HistoryImageCache.zarr_url)
            )
            .scalars()
            .all()
        )

        assert [cache.model_dump() for cache in caches] == [
            {
                "zarr_url": "/a",
                "dataset_id": dataset.id,
                "workflowtask_id": wftask.id,
                "latest_history_unit_id": unit2.id,
            },
            {
                "zarr_url": "/b",
                "dataset_id": dataset.id,
                "workflowtask_id": wftask.id,
                "latest_history_unit_id": unit2.id,
            },
        ]
