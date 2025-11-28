from pathlib import Path

import pytest

from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.runner.config import JobRunnerConfigLocal
from fractal_server.runner.task_files import TaskFiles

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
    project_factory,
    dataset_factory,
    workflow_factory,
    workflowtask_factory,
    task_factory,
    job_factory,
    tmp_path,
) -> HistoryRun:
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
            working_dir=tmp_path.as_posix(),
            status="done",
        )
        run = HistoryRun(
            workflowtask_id=wftask.id,
            dataset_id=dataset.id,
            job_id=job.id,
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
async def history_mock_for_submit(db, history_run_mock) -> tuple[int, int, int]:
    unit = HistoryUnit(
        history_run_id=history_run_mock.id,
        status=HistoryUnitStatus.SUBMITTED,
        logfile="/abcd",
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

    return (
        history_run_mock.id,
        unit.id,
        history_run_mock.workflowtask_id,
    )


@pytest.fixture
async def history_mock_for_multisubmit(
    db, history_run_mock
) -> tuple[int, list[int]]:
    unit_ids = []
    for zarr_url in ZARR_URLS:
        unit = HistoryUnit(
            history_run_id=history_run_mock.id,
            status=HistoryUnitStatus.SUBMITTED,
            logfile=f"{zarr_url}.log",
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

    return (
        history_run_mock.id,
        unit_ids,
        history_run_mock.workflowtask_id,
    )


def get_dummy_task_files(
    base_dir: Path,
    component: str,
    prefix: str | None = None,
    is_slurm: bool = False,
) -> TaskFiles:
    if is_slurm:
        root_dir_local = base_dir / "server"
        root_dir_remote = base_dir / "user"
    else:
        root_dir_local = base_dir
        root_dir_remote = base_dir

    return TaskFiles(
        root_dir_local=root_dir_local,
        root_dir_remote=root_dir_remote,
        task_name="name",
        task_order=0,
        component=component,
        prefix=(prefix or "some-prefix"),
    )


def get_default_local_backend_config():
    """
    Return a default `LocalBackendConfig` configuration object
    """
    return JobRunnerConfigLocal(parallel_tasks_per_job=None)
