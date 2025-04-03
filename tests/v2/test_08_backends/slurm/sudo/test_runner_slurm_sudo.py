import time

import pytest
from devtools import debug

from ...aux_unit_runner import *  # noqa
from ...aux_unit_runner import ZARR_URLS
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.slurm_sudo.runner import (
    SudoSlurmRunner,
)
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from tests.fixtures_slurm import SLURM_USER
from tests.v2._aux_runner import get_default_slurm_config
from tests.v2.test_08_backends.aux_unit_runner import get_dummy_task_files


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
    def do_nothing(parameters: dict, remote_files: dict):
        return 42

    history_run_id, history_unit_id = history_mock_for_submit

    if task_type.startswith("converter_"):
        parameters = {}
    else:
        parameters = dict(zarr_urls=ZARR_URLS)

    with SudoSlurmRunner(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        poll_interval=0,
    ) as runner:
        result, exception = runner.submit(
            do_nothing,
            parameters=parameters,
            task_files=get_dummy_task_files(
                tmp777_path, component="0", is_slurm=True
            ),
            task_type=task_type,
            history_unit_id=history_unit_id,
            config=get_default_slurm_config(),
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

    def raise_ValueError(parameters: dict, remote_files: dict):
        raise ValueError(ERROR_MSG)

    history_run_id, history_unit_id = history_mock_for_submit

    if not task_type.startswith("converter_"):
        parameters = dict(zarr_urls=ZARR_URLS)
    else:
        parameters = {}

    with SudoSlurmRunner(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        poll_interval=0,
    ) as runner:
        result, exception = runner.submit(
            raise_ValueError,
            parameters=parameters,
            history_unit_id=history_unit_id,
            task_files=get_dummy_task_files(
                tmp777_path, component="0", is_slurm=True
            ),
            config=get_default_slurm_config(),
            task_type=task_type,
        )
    debug(result, exception)
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
    def fun(parameters: dict, remote_files: dict):
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

    with SudoSlurmRunner(
        slurm_user=SLURM_USER,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        poll_interval=0,
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
            list_task_files=[
                get_dummy_task_files(
                    tmp777_path, component=str(ind), is_slurm=True
                )
                for ind in range(len(ZARR_URLS))
            ],
            task_type="parallel",
            config=get_default_slurm_config(),
            history_unit_ids=history_unit_ids,
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
