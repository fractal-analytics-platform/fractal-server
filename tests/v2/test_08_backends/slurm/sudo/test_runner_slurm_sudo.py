import time

import pytest
from devtools import debug

from fractal_server.app.history import HistoryItemImageStatus
from fractal_server.app.models.v2.history import HistoryItemV2
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.slurm_sudo.runner import (
    RunnerSlurmSudo,
)
from tests.fixtures_slurm import SLURM_USER

ALL_IMAGES = ["a", "b", "c", "d"]


@pytest.fixture
async def mock_history_item(
    db,
    project_factory_v2,
    dataset_factory_v2,
    MockCurrentUser,
):
    # Create test data
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user=user)
        dataset = await dataset_factory_v2(project_id=project.id)
    item = HistoryItemV2(
        dataset_id=dataset.id,
        workflowtask_id=None,
        workflowtask_dump={},
        task_group_dump={},
        parameters_hash="xxx",
        num_current_images=4,
        num_available_images=4,
        images={
            zarr_url: HistoryItemImageStatus.SUBMITTED
            for zarr_url in ALL_IMAGES
        },
    )
    db.add(item)
    await db.commit()
    await db.refresh(item)
    return item


@pytest.mark.container
async def test_submit_success(
    db,
    mock_history_item,
    tmp777_path,
    monkey_slurm,
):
    def do_nothing(parameters: dict) -> int:
        return 42

    with RunnerSlurmSudo(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        slurm_poll_interval=0,
    ) as runner:
        result, exception = runner.submit(
            do_nothing,
            parameters=dict(zarr_urls=["a", "b", "c", "d"]),
            history_item_id=mock_history_item.id,
            workdir_local=tmp777_path / "server/task",
            workdir_remote=tmp777_path / "user/task",
        )
        assert result == 42
        assert exception is None
    db.expunge_all()
    history_item = await db.get(HistoryItemV2, mock_history_item.id)
    assert history_item.images == {
        zarr_url: HistoryItemImageStatus.DONE for zarr_url in ALL_IMAGES
    }


@pytest.mark.container
async def test_submit_fail(
    db,
    mock_history_item,
    tmp777_path,
    monkey_slurm,
):
    ERROR_MSG = "very nice error"

    def raise_ValueError(parameters: dict):
        raise ValueError(ERROR_MSG)

    with RunnerSlurmSudo(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        slurm_poll_interval=0,
    ) as runner:
        result, exception = runner.submit(
            raise_ValueError,
            parameters=dict(zarr_urls=[]),
            history_item_id=mock_history_item.id,
            workdir_local=tmp777_path / "server/task",
            workdir_remote=tmp777_path / "user/task",
        )
    assert result is None
    assert isinstance(exception, TaskExecutionError)
    assert ERROR_MSG in str(exception)
    db.expunge_all()
    history_item = await db.get(HistoryItemV2, mock_history_item.id)
    assert history_item.images == {
        zarr_url: HistoryItemImageStatus.FAILED for zarr_url in ALL_IMAGES
    }


@pytest.mark.container
async def test_multisubmit(
    db,
    mock_history_item,
    tmp777_path,
    monkey_slurm,
):
    def fun(parameters: int):
        zarr_url = parameters["zarr_url"]
        x = parameters["parameter"]
        if x != 3:
            print(f"Running with {zarr_url=} and {x=}, returning {2*x=}.")
            time.sleep(1)
            return 2 * x
        else:
            print(f"Running with {zarr_url=} and {x=}, raising error.")
            time.sleep(1)
            raise ValueError("parameter=3 is very very bad")

    with RunnerSlurmSudo(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        slurm_poll_interval=0,
    ) as runner:
        results, exceptions = runner.multisubmit(
            fun,
            [
                dict(zarr_url="a", parameter=1),
                dict(zarr_url="b", parameter=2),
                dict(zarr_url="c", parameter=3),
                dict(zarr_url="d", parameter=4),
            ],
            history_item_id=mock_history_item.id,
            workdir_local=tmp777_path / "server/task",
            workdir_remote=tmp777_path / "user/task",
        )
        debug(results)
        debug(exceptions)
    db.expunge_all()
    history_item = await db.get(HistoryItemV2, mock_history_item.id)
    debug(history_item.images)
    assert history_item.images == {
        "a": "done",
        "b": "done",
        "c": "failed",
        "d": "done",
    }
