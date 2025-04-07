import time

import pytest
from devtools import debug

from ..aux_unit_runner import *  # noqa
from ..aux_unit_runner import get_default_local_backend_config
from ..aux_unit_runner import ZARR_URLS
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
        print(f"Running with {zarr_url=} and {x=}, returning {2*x=}.")
        time.sleep(1)
        return 2 * x
    else:
        print(f"Running with {zarr_url=} and {x=}, raising error.")
        time.sleep(1)
        raise ValueError("parameter=3 is very very bad")


async def test_multisubmit(
    tmp_path,
    db,
    history_mock_for_multisubmit,
):

    history_run_id, history_unit_ids = history_mock_for_multisubmit

    with LocalRunner(root_dir_local=tmp_path) as runner:

        results, exceptions = runner.multisubmit(
            fun,
            [
                {
                    "zarr_url": "a",
                    "parameter": 1,
                },
                {
                    "zarr_url": "b",
                    "parameter": 2,
                },
                {
                    "zarr_url": "c",
                    "parameter": 3,
                },
                {
                    "zarr_url": "d",
                    "parameter": 4,
                },
            ],
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
        3: 8,
        0: 2,
        1: 4,
    }
    assert isinstance(exceptions[2], TaskExecutionError)
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


# @pytest.mark.parametrize("parallel_tasks_per_job", [None, 1, 2, 3, 4, 8, 16])
# def test_executor_map(parallel_tasks_per_job: int):
#     local_backend_config = LocalBackendConfig(
#         parallel_tasks_per_job=parallel_tasks_per_job
#     )

#     NUM = 7

#     # Test function of a single variable
#     with LocalRunner() as executor:

#         def fun_x(x):
#             return 3 * x + 1

#         inputs = list(range(NUM))
#         result_generator = executor.map(
#             fun_x,
#             inputs,
#             local_backend_config=local_backend_config,
#         )
#         results = list(result_generator)
#         assert results == [fun_x(x) for x in inputs]

#     # Test function of two variables
#     with LocalRunner() as executor:

#         def fun_xy(x, y):
#             return 2 * x + y

#         inputs_x = list(range(3, 3 + NUM))
#         inputs_y = list(range(NUM))
#         result_generator = executor.map(
#             fun_xy,
#             inputs_x,
#             inputs_y,
#             local_backend_config=local_backend_config,
#         )
#         results = list(result_generator)
#         assert results == [fun_xy(x, y) for x, y in zip(inputs_x, inputs_y)]
