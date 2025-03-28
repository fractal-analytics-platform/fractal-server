import pytest

from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.schemas.v2 import HistoryUnitStatus


ZARR_URLS = ["a", "b", "c", "d"]


@pytest.fixture
async def history_run_mock(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    workflowtask_factory_v2,
) -> HistoryRun:
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
            num_available_images=4,
            status=HistoryUnitStatus.SUBMITTED,
        )
        db.add(run)
        await db.commit()
        await db.refresh(run)
    return run


@pytest.fixture
async def history_mock_for_submit(db, history_run_mock) -> tuple[int, int]:
    unit = HistoryUnit(
        history_run_id=history_run_mock.id,
        status=HistoryUnitStatus.SUBMITTED,
        logfile="/log",
        zarr_urls=ZARR_URLS,
    )
    db.add(unit)
    await db.commit()
    await db.refresh(unit)

    for zarr_url in ZARR_URLS:
        db.add(
            HistoryImageCache(
                zarr_url=zarr_url,
                workflowtask_id=history_run_mock.workflowtask_id,
                dataset_id=history_run_mock.dataset_id,
                latest_history_unit_id=unit.id,
            )
        )
    await db.commit()

    return history_run_mock.id, unit.id


@pytest.fixture
async def history_mock_for_multisubmit(
    db, history_run_mock
) -> tuple[int, list[int]]:
    unit_ids = []
    for zarr_url in ZARR_URLS:
        unit = HistoryUnit(
            history_run_id=history_run_mock.id,
            status=HistoryUnitStatus.SUBMITTED,
            logfile="/log/fake",
            zarr_urls=[zarr_url],
        )
        db.add(unit)
        await db.commit()
        await db.refresh(unit)
        unit_ids.append(unit.id)
        db.add(
            HistoryImageCache(
                zarr_url=zarr_url,
                workflowtask_id=history_run_mock.workflowtask_id,
                dataset_id=history_run_mock.dataset_id,
                latest_history_unit_id=unit.id,
            )
        )
        await db.commit()

    return history_run_mock.id, unit_ids
