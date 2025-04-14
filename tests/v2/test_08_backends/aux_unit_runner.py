from pathlib import Path

import pytest

from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.runner.executors.local.get_local_config import (
    LocalBackendConfig,
)
from fractal_server.app.runner.task_files import TaskFiles
from fractal_server.app.schemas.v2 import HistoryUnitStatus

ZARR_URLS = ["/a", "/b", "/c", "/d"]
ZARR_URLS_AND_PARAMETER = [
    {"zarr_url": "/a", "parameter": 1},
    {"zarr_url": "/b", "parameter": 2},
    {"zarr_url": "/c", "parameter": 3},
    {"zarr_url": "/d", "parameter": 4},
]


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
        logfile=None,
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
            logfile=None,
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


def get_dummy_task_files(
    root_dir_local: Path,
    component: str,
    is_slurm: bool = False,
) -> TaskFiles:
    if is_slurm:
        return TaskFiles(
            root_dir_local=root_dir_local / "server",
            root_dir_remote=root_dir_local / "user",
            task_name="name",
            task_order=0,
            component=component,
        )
    return TaskFiles(
        root_dir_local=root_dir_local,
        root_dir_remote=root_dir_local,
        task_name="name",
        task_order=0,
        component=component,
    )


def get_default_local_backend_config():
    """
    Return a default `LocalBackendConfig` configuration object
    """
    return LocalBackendConfig(parallel_tasks_per_job=None)
