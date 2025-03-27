import time
from pathlib import Path

import pytest
from devtools import debug

from ..aux_unit_runner import *  # noqa
from ..aux_unit_runner import ZARR_URLS
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.runner.executors.local.runner import LocalRunner
from fractal_server.app.runner.task_files import TaskFiles
from fractal_server.app.schemas.v2 import HistoryUnitStatus


def get_dummy_task_files(root_dir_local: Path) -> TaskFiles:
    return TaskFiles(
        root_dir_local=root_dir_local,
        root_dir_remote=root_dir_local,
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


async def test_submit_success(
    db,
    history_mock_for_submit,
    tmp_path,
):
    def do_nothing(parameters: dict) -> int:
        return 42

    history_run_id, history_unit_id = history_mock_for_submit

    with LocalRunner(tmp_path) as runner:
        result, exception = runner.submit(
            do_nothing,
            parameters={
                "zarr_urls": ZARR_URLS,
                "__FRACTAL_PARALLEL_COMPONENT__": "000000",
            },
            task_files=get_dummy_task_files(tmp_path),
            task_type="non_parallel",
            history_unit_id=history_unit_id,
        )
    assert result == 42
    assert exception is None

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    debug(run)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.submit`
    unit = await db.get(HistoryUnit, history_run_id)
    debug(unit)
    assert unit.status == HistoryUnitStatus.DONE


async def test_submit_fail(
    db,
    history_mock_for_submit,
    tmp_path,
):
    ERROR_MSG = "very nice error"

    def raise_ValueError(parameters: dict):
        raise ValueError(ERROR_MSG)

    history_run_id, history_unit_id = history_mock_for_submit

    with LocalRunner(root_dir_local=tmp_path) as runner:
        result, exception = runner.submit(
            raise_ValueError,
            parameters={
                "zarr_urls": ZARR_URLS,
                "__FRACTAL_PARALLEL_COMPONENT__": "000000",
            },
            task_files=get_dummy_task_files(tmp_path),
            task_type="non_parallel",
            history_unit_id=history_unit_id,
        )
    assert result is None
    assert isinstance(exception, ValueError)
    assert ERROR_MSG in str(exception)

    # `HistoryRun.status` is updated at a higher level, not from
    # within `runner.submit`
    run = await db.get(HistoryRun, history_run_id)
    debug(run)
    assert run.status == HistoryUnitStatus.SUBMITTED

    # `HistoryUnit.status` is updated from within `runner.submit`
    unit = await db.get(HistoryUnit, history_run_id)
    debug(unit)
    assert unit.status == HistoryUnitStatus.FAILED


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


async def test_multisubmit(tmp_path):
    with LocalRunner(root_dir_local=tmp_path) as runner:
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
            task_files=get_dummy_task_files(tmp_path),
            task_type="parallel",
        )
        debug(results)
        debug(exceptions)


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


# @pytest.mark.parametrize("parallel_tasks_per_job", [None, 1, 2, 4, 8, 16])
# def test_executor_map_with_exception(parallel_tasks_per_job):
#     def _raise(n: int):
#         if n == 5:
#             raise ValueError
#         else:
#             return n

#     local_backend_config = LocalBackendConfig(
#         parallel_tasks_per_job=parallel_tasks_per_job
#     )

#     with pytest.raises(ValueError):
#         with LocalRunner() as executor:
#             _ = executor.map(
#                 _raise,
#                 range(10),
#                 local_backend_config=local_backend_config,
#             )
