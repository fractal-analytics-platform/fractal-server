import pytest
from devtools import debug

from .aux_unit_runner import *  # noqa
from .aux_unit_runner import get_default_local_backend_config
from .aux_unit_runner import ZARR_URLS
from .aux_unit_runner import ZARR_URLS_AND_PARAMETER
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.local.runner import LocalRunner
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from tests.v2.test_08_backends.aux_unit_runner import get_dummy_task_files


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
    history_mock_for_submit,
    tmp_path,
    task_type: str,
):
    def do_nothing(parameters: dict, remote_files: dict) -> int:
        return 42

    history_run_id, history_unit_id = history_mock_for_submit

    if task_type.startswith("converter_"):
        parameters = {}
    else:
        parameters = dict(zarr_urls=ZARR_URLS)

    with LocalRunner(tmp_path) as runner:
        result, exception = runner.submit(
            do_nothing,
            parameters=parameters,
            task_files=get_dummy_task_files(tmp_path, component="0"),
            task_type=task_type,
            history_unit_id=history_unit_id,
            config=get_default_local_backend_config(),
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
    history_mock_for_submit,
    tmp_path,
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

    with LocalRunner(root_dir_local=tmp_path) as runner:
        result, exception = runner.submit(
            raise_ValueError,
            parameters=parameters,
            task_files=get_dummy_task_files(tmp_path, component="0"),
            task_type=task_type,
            history_unit_id=history_unit_id,
            config=get_default_local_backend_config(),
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


def fun(parameters: dict, remote_files: dict):
    zarr_url = parameters["zarr_url"]
    x = parameters["parameter"]
    if x != 3:
        print(f"Running with {zarr_url=} and {x=}, returning {2 * x=}.")
        return 2 * x
    else:
        print(f"Running with {zarr_url=} and {x=}, raising error.")
        raise ValueError("parameter=3 is very very bad")


async def test_multisubmit_parallel(
    tmp_path,
    db,
    history_mock_for_multisubmit,
):
    history_run_id, history_unit_ids = history_mock_for_multisubmit
    with LocalRunner(root_dir_local=tmp_path) as runner:
        results, exceptions = runner.multisubmit(
            fun,
            ZARR_URLS_AND_PARAMETER,
            list_task_files=[
                get_dummy_task_files(tmp_path, component=str(ind))
                for ind in range(len(ZARR_URLS))
            ],
            task_type="parallel",
            history_unit_ids=history_unit_ids,
            config=get_default_local_backend_config(),
        )
    debug(results)
    debug(exceptions)
    assert results == {
        0: 2,
        1: 4,
        3: 8,
    }
    assert isinstance(exceptions[2], TaskExecutionError)
    assert "very very bad" in str(exceptions[2])

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    debug(run)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.multisubmit`
    for ind, _unit_id in enumerate(history_unit_ids):
        unit = await db.get(HistoryUnit, _unit_id)
        debug(unit)
        if ind != 2:
            assert unit.status == HistoryUnitStatus.DONE
        else:
            assert unit.status == HistoryUnitStatus.FAILED


async def test_multisubmit_compound(
    tmp_path,
    db,
    history_mock_for_multisubmit,
):
    history_run_id, history_unit_ids = history_mock_for_multisubmit

    with LocalRunner(root_dir_local=tmp_path) as runner:
        results, exceptions = runner.multisubmit(
            fun,
            ZARR_URLS_AND_PARAMETER,
            list_task_files=[
                get_dummy_task_files(tmp_path, component=str(ind))
                for ind in range(len(ZARR_URLS))
            ],
            task_type="compound",
            history_unit_ids=history_unit_ids,
            config=get_default_local_backend_config(),
        )
    debug(results)
    debug(exceptions)
    assert results == {
        0: 2,
        1: 4,
        3: 8,
    }
    assert isinstance(exceptions[2], TaskExecutionError)
    assert "very very bad" in str(exceptions[2])

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    debug(run)
    assert run.status == HistoryUnitStatus.SUBMITTED

    for _unit_id in history_unit_ids:
        unit = await db.get(HistoryUnit, _unit_id)
        debug(unit)
        # `HistoryUnit.status` is not updated from within `runner.multisubmit`,
        # for compound tasks
        assert unit.status == HistoryUnitStatus.SUBMITTED


@pytest.mark.parametrize("parallel_tasks_per_job", [None, 1, 1000])
async def test_multisubmit_in_chunks(
    tmp_path,
    db,
    history_mock_for_multisubmit,
    parallel_tasks_per_job,
):
    config = get_default_local_backend_config()
    config.parallel_tasks_per_job = parallel_tasks_per_job

    history_run_id, history_unit_ids = history_mock_for_multisubmit

    with LocalRunner(root_dir_local=tmp_path) as runner:
        results, exceptions = runner.multisubmit(
            fun,
            ZARR_URLS_AND_PARAMETER,
            list_task_files=[
                get_dummy_task_files(tmp_path, component=str(ind))
                for ind in range(len(ZARR_URLS))
            ],
            task_type="parallel",
            history_unit_ids=history_unit_ids,
            config=config,
        )
    debug(results)
    debug(exceptions)
    assert results == {
        3: 8,
        0: 2,
        1: 4,
    }
    assert isinstance(exceptions[2], TaskExecutionError)
    assert "very very bad" in str(exceptions[2])

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.submit`
    for ind, _unit_id in enumerate(history_unit_ids):
        unit = await db.get(HistoryUnit, _unit_id)
        if ind != 2:
            assert unit.status == HistoryUnitStatus.DONE
        else:
            assert unit.status == HistoryUnitStatus.FAILED
