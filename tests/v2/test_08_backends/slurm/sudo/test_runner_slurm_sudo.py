import time
from pathlib import Path

import pytest
from devtools import debug

from ...aux_unit_runner import *  # noqa
from ...aux_unit_runner import ZARR_URLS
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.slurm_sudo.runner import (
    RunnerSlurmSudo,
)
from fractal_server.app.runner.task_files import TaskFiles
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from tests.fixtures_slurm import SLURM_USER
from tests.v2._aux_runner import get_default_slurm_config


def get_dummy_task_files(root_path: Path) -> TaskFiles:
    return TaskFiles(
        root_dir_local=root_path / "server",
        root_dir_remote=root_path / "user",
        task_name="name",
        task_order=0,
    )


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


@pytest.mark.container
@pytest.mark.parametrize(
    "task_type",
    [
        "non_parallel",
        "compound",
        "converter_non_parallel",
        "converter_compound",
    ],
)
async def test_submit_success(
    db,
    tmp777_path,
    history_mock_for_submit,
    monkey_slurm,
    task_type: str,
):
    def do_nothing(parameters: dict, **kwargs) -> int:
        return 42

    history_run_id, history_unit_id = history_mock_for_submit
    parameters = {"__FRACTAL_PARALLEL_COMPONENT__": "000000"}
    if not task_type.startswith("converter_"):
        parameters["zarr_urls"] = ZARR_URLS
    with RunnerSlurmSudo(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        slurm_poll_interval=0,
    ) as runner:
        result, exception = runner.submit(
            do_nothing,
            parameters=parameters,
            history_unit_id=history_unit_id,
            task_files=get_dummy_task_files(tmp777_path),
            slurm_config=get_default_slurm_config(),
            task_type=task_type,
        )
    debug(result, exception)
    assert result == 42
    assert exception is None

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    debug(run)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.submit`
    unit = await db.get(HistoryUnit, history_unit_id)
    debug(unit)
    if task_type in ["non_parallel", "converter_non_parallel"]:
        assert unit.status == HistoryUnitStatus.DONE
    else:
        assert unit.status == HistoryUnitStatus.SUBMITTED


@pytest.mark.container
@pytest.mark.parametrize(
    "task_type",
    [
        "non_parallel",
        "compound",
        "converter_non_parallel",
        "converter_compound",
    ],
)
async def test_submit_fail(
    db,
    tmp777_path,
    monkey_slurm,
    history_mock_for_submit,
    task_type: str,
):
    ERROR_MSG = "very nice error"

    def raise_ValueError(parameters: dict, **kwargs):
        raise ValueError(ERROR_MSG)

    history_run_id, history_unit_id = history_mock_for_submit
    parameters = {"__FRACTAL_PARALLEL_COMPONENT__": "000000"}
    if not task_type.startswith("converter_"):
        parameters["zarr_urls"] = ZARR_URLS

    with RunnerSlurmSudo(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        slurm_poll_interval=0,
    ) as runner:
        result, exception = runner.submit(
            raise_ValueError,
            parameters=parameters,
            history_unit_id=history_unit_id,
            task_files=get_dummy_task_files(tmp777_path),
            slurm_config=get_default_slurm_config(),
            task_type=task_type,
        )

    assert result is None
    assert isinstance(exception, TaskExecutionError)
    assert ERROR_MSG in str(exception)
    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    debug(run)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.submit`
    unit = await db.get(HistoryUnit, history_unit_id)
    debug(unit)
    assert unit.status == HistoryUnitStatus.FAILED


@pytest.mark.container
async def test_multisubmit(
    db, tmp777_path, monkey_slurm, history_mock_for_multisubmit
):
    def fun(parameters: dict, **kwargs):
        zarr_url = parameters["zarr_url"]
        x = parameters["parameter"]
        if x != 3:
            print(f"Running with {zarr_url=} and {x=}, returning {2 * x=}.")
            time.sleep(1)
            return 2 * x
        else:
            print(f"Running with {zarr_url=} and {x=}, raising error.")
            time.sleep(1)
            raise ValueError("parameter=3 is very very bad")

    history_run_id, history_unit_ids = history_mock_for_multisubmit

    with RunnerSlurmSudo(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        slurm_poll_interval=0,
    ) as runner:
        results, exceptions = runner.multisubmit(
            fun,
            [
                {
                    "zarr_url": "a",
                    "parameter": 1,
                    "__FRACTAL_PARALLEL_COMPONENT__": "000000",
                },
                {
                    "zarr_url": "b",
                    "parameter": 2,
                    "__FRACTAL_PARALLEL_COMPONENT__": "000001",
                },
                {
                    "zarr_url": "c",
                    "parameter": 3,
                    "__FRACTAL_PARALLEL_COMPONENT__": "000002",
                },
                {
                    "zarr_url": "d",
                    "parameter": 4,
                    "__FRACTAL_PARALLEL_COMPONENT__": "000003",
                },
            ],
            history_unit_ids=history_unit_ids,
            task_files=get_dummy_task_files(tmp777_path),
            slurm_config=get_default_slurm_config(),
            task_type="parallel",
        )
        debug(results)
        debug(exceptions)
        assert results == {
            3: 8,
            0: 2,
            1: 4,
        }
        # assert isinstance(exceptions[2], ValueError) # TaskExecutionError
        assert "very very bad" in str(exceptions[2])

        # `HistoryRun.status` is updated at a higher level, not from
        # within `runner.submit`
        run = await db.get(HistoryRun, history_run_id)
        debug(run)
        assert run.status == HistoryUnitStatus.SUBMITTED

        # `HistoryUnit.status` is updated from within `runner.submit`
        for ind, _unit_id in enumerate(history_unit_ids):
            unit = await db.get(HistoryUnit, _unit_id)
            debug(unit)
            if ind != 2:
                assert unit.status == HistoryUnitStatus.DONE
            else:
                assert unit.status == HistoryUnitStatus.FAILED
