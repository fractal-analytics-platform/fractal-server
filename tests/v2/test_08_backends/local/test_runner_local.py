import time
from pathlib import Path

from devtools import debug

from ..aux_unit_runner import *  # noqa
from ..aux_unit_runner import ZARR_URLS
from fractal_server.app.runner.executors.local.runner import LocalRunner
from fractal_server.app.runner.task_files import TaskFiles


def get_dummy_task_files(root_dir_local: Path) -> TaskFiles:
    return TaskFiles(
        root_dir_local=root_dir_local,
        root_dir_remote=root_dir_local,
        task_name="name",
        task_order=0,
    )


async def test_submit_success(
    db,
    mock_history_item,
    tmp_path,
):
    def do_nothing(parameters: dict) -> int:
        return 42

    with LocalRunner(tmp_path) as runner:
        result, exception = runner.submit(
            do_nothing,
            parameters={
                "zarr_urls": ZARR_URLS,
                "__FRACTAL_PARALLEL_COMPONENT__": "000000",
            },
            task_files=get_dummy_task_files(tmp_path),
        )
    assert result == 42
    assert exception is None
    db.expunge_all()


async def test_submit_fail(
    db,
    mock_history_item,
    tmp_path,
):
    ERROR_MSG = "very nice error"

    def raise_ValueError(parameters: dict):
        raise ValueError(ERROR_MSG)

    with LocalRunner(root_dir_local=tmp_path) as runner:
        result, exception = runner.submit(
            raise_ValueError,
            parameters={
                "zarr_urls": ZARR_URLS,
                "__FRACTAL_PARALLEL_COMPONENT__": "000000",
            },
            task_files=get_dummy_task_files(tmp_path),
        )
    assert result is None
    assert isinstance(exception, ValueError)
    assert ERROR_MSG in str(exception)


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


async def test_multisubmit(db, mock_history_item, tmp_path):
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
