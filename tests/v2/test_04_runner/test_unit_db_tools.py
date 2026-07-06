import math
import time

import pytest
from devtools import debug
from sqlalchemy.exc import NoResultFound
from sqlmodel import select

from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.runner.v2.db_tools import (
    bulk_update_has_warnings_history_unit,
)
from fractal_server.runner.v2.db_tools import bulk_update_status_of_history_unit
from fractal_server.runner.v2.db_tools import update_executor_error_log_safe
from fractal_server.runner.v2.db_tools import update_history_unit_no_commit


@pytest.mark.parametrize("num_history_units", [10, 50])
async def test_update_status_of_history_unit(
    num_history_units: int,
    # Fixtures
    db_sync,
    db,
    dataset_factory,
    project_factory,
    task_factory,
    workflow_factory,
    workflowtask_factory,
    job_factory,
    MockCurrentUser,
):
    async with MockCurrentUser() as user:
        task = await task_factory(user.id)
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)
        workflow = await workflow_factory(project_id=project.id)
        wftask = await workflowtask_factory(
            workflow_id=workflow.id,
            task_id=task.id,
        )
        job = await job_factory(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir="/foo",
            status="done",
        )

        hr = HistoryRun(
            dataset_id=dataset.id,
            workflowtask_id=wftask.id,
            task_group_dump={},
            workflowtask_dump={},
            status=HistoryUnitStatus.SUBMITTED,
            num_available_images=0,
            job_id=job.id,
        )
        db_sync.add(hr)
        db_sync.commit()
        db_sync.refresh(hr)

        db_sync.add_all(
            [
                HistoryUnit(
                    history_run_id=hr.id,
                    status=HistoryUnitStatus.SUBMITTED,
                    logfile="/fake/log",
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
            update_history_unit_no_commit(
                history_unit_id=hr.id,
                status=HistoryUnitStatus.FAILED,
                db_sync=db_sync,
            )
        db_sync.commit()
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

        # Test failure due to wrong primary key
        with pytest.raises(
            NoResultFound, match="No row was found when one was required"
        ):
            update_history_unit_no_commit(
                history_unit_id=1111111111,
                status=HistoryUnitStatus.FAILED,
                db_sync=db_sync,
            )


async def test_bulk_update_has_warnings_history_unit(
    # Fixtures
    db_sync,
    dataset_factory,
    project_factory,
    task_factory,
    workflow_factory,
    workflowtask_factory,
    job_factory,
    MockCurrentUser,
    tmp_path,
):
    num_history_units = 51

    warning_logfile = tmp_path / "warnings.log"
    with warning_logfile.open("w") as f:
        for ind in range(10):
            f.write(f"Some logs {ind}\n")
        f.write("This has WaRnInGs!\n")
        for ind in range(10):
            f.write(f"Some more logs {ind}\n")
    no_warning_logfile = tmp_path / "no_warnings.log"
    with no_warning_logfile.open("w") as f:
        for ind in range(10):
            f.write(f"Some logs {ind}\n")

    async with MockCurrentUser() as user:
        task = await task_factory(user.id)
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)
        workflow = await workflow_factory(project_id=project.id)
        wftask = await workflowtask_factory(
            workflow_id=workflow.id,
            task_id=task.id,
        )
        job = await job_factory(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir="/foo",
            status="done",
        )

        hr = HistoryRun(
            dataset_id=dataset.id,
            workflowtask_id=wftask.id,
            task_group_dump={},
            workflowtask_dump={},
            status=HistoryUnitStatus.SUBMITTED,
            num_available_images=0,
            job_id=job.id,
        )
        db_sync.add(hr)
        db_sync.commit()
        db_sync.refresh(hr)

        db_sync.add_all(
            [
                HistoryUnit(
                    history_run_id=hr.id,
                    status=HistoryUnitStatus.DONE,
                    has_warnings=False,
                    logfile=(
                        warning_logfile.as_posix()
                        if unit_index % 2 == 0
                        else no_warning_logfile.as_posix()
                    ),
                )
                for unit_index in range(num_history_units)
            ]
        )
        db_sync.commit()

        res = db_sync.execute(select(HistoryUnit.id))
        history_unit_ids = res.scalars().all()

        # Bulk function
        start = time.perf_counter()
        bulk_update_has_warnings_history_unit(
            history_unit_ids=history_unit_ids,
            db_sync=db_sync,
        )
        stop = time.perf_counter()
        bulk_time = stop - start

        res = db_sync.execute(select(HistoryUnit))
        history_units = res.scalars().all()
        assert sum(
            1
            for history_unit in history_units
            if history_unit.has_warnings is True
        ) == math.ceil(len(history_units) / 2.0)

        # Benchmark

        debug(
            f"History Units:    {num_history_units}\n"
            f"Bulk time:        {bulk_time}\n"
        )


async def test_update_executor_error_log_safe(
    db,
    db_sync,
    dataset_factory,
    project_factory,
    workflow_factory,
    job_factory,
    task_factory,
    MockCurrentUser,
):
    async with MockCurrentUser() as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(user.id)
        await _workflow_insert_task(
            workflow_id=workflow.id,
            task_id=task.id,
            db=db,
            order=0,
        )
        job = await job_factory(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir="/foo",
            status="done",
            first_task_index=0,
            last_task_index=0,
        )
        job_id = job.id

    VALUE = "some error message"
    update_executor_error_log_safe(
        job_id=job_id,
        executor_error_log=VALUE,
        db=db_sync,
    )
    job = db_sync.get(JobV2, job_id)
    assert job.executor_error_log == VALUE

    update_executor_error_log_safe(
        job_id=job_id, executor_error_log="\x00", db=db_sync
    )
    job = db_sync.get(JobV2, job_id)
    assert job.executor_error_log == VALUE

    update_executor_error_log_safe(
        job_id=job_id, executor_error_log=None, db=db_sync
    )
    job = db_sync.get(JobV2, job_id)
    assert job.executor_error_log is None
