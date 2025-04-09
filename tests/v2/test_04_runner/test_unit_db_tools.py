import time

import pytest
from devtools import debug
from sqlmodel import select

from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.runner.v2.db_tools import (
    bulk_update_status_of_history_unit,
)
from fractal_server.app.runner.v2.db_tools import update_status_of_history_unit
from fractal_server.app.schemas.v2 import HistoryUnitStatus


@pytest.mark.parametrize("num_history_units", [10, 100, 500])
async def test_update_status_of_history_unit(
    num_history_units: int,
    # Fixtures
    db_sync,
    dataset_factory_v2,
    project_factory_v2,
    task_factory_v2,
    workflow_factory_v2,
    workflowtask_factory_v2,
    MockCurrentUser,
):

    async with MockCurrentUser() as user:
        task = await task_factory_v2(user.id)
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id)
        workflow = await workflow_factory_v2(project_id=project.id)
        wftask = await workflowtask_factory_v2(
            workflow_id=workflow.id,
            task_id=task.id,
        )

        hr = HistoryRun(
            dataset_id=dataset.id,
            workflowtask_id=wftask.id,
            task_group_dump={},
            workflowtask_dump={},
            status=HistoryUnitStatus.SUBMITTED,
            num_available_images=0,
        )
        db_sync.add(hr)
        db_sync.commit()
        db_sync.refresh(hr)

        db_sync.add_all(
            [
                HistoryUnit(
                    history_run_id=hr.id, status=HistoryUnitStatus.SUBMITTED
                )
                for _ in range(num_history_units)
            ]
        )
        db_sync.commit()

        res = db_sync.execute(select(HistoryUnit))
        hrs = res.scalars().all()
        assert len(hrs) == num_history_units
        assert all(hr.status == HistoryUnitStatus.SUBMITTED for hr in hrs)

        # Non-Bulk function
        start = time.perf_counter()
        for hr in hrs:
            update_status_of_history_unit(
                history_unit_id=hr.id,
                status=HistoryUnitStatus.FAILED,
                db_sync=db_sync,
            )
        stop = time.perf_counter()
        non_bulk_time = stop - start

        res = db_sync.execute(select(HistoryUnit))
        hrs = res.scalars().all()
        assert len(hrs) == num_history_units
        assert all(hr.status == HistoryUnitStatus.FAILED for hr in hrs)

        # Bulk function
        start = time.perf_counter()
        bulk_update_status_of_history_unit(
            history_unit_ids=[hr.id for hr in hrs],
            status=HistoryUnitStatus.DONE,
            db_sync=db_sync,
        )
        stop = time.perf_counter()
        bulk_time = stop - start

        res = db_sync.execute(select(HistoryUnit))
        hrs = res.scalars().all()
        assert len(hrs) == num_history_units
        assert all(hr.status == HistoryUnitStatus.DONE for hr in hrs)

        # Benchmark

        debug(
            f"History Units:    {num_history_units}\n"
            f"Non bulk time:    {non_bulk_time}\n"
            f"Bulk time:        {bulk_time}\n"
        )
