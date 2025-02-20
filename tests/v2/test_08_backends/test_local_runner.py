import time

import pytest
from devtools import debug

from fractal_server.app.runner.executors.local._local_config import (
    LocalBackendConfig,
)
from fractal_server.app.runner.executors.local.runner import (
    LocalRunner,
)


def test_submit_success():
    def do_nothing(parameters: dict) -> int:
        return 42

    with LocalRunner() as runner:
        result, exception = runner.submit(
            do_nothing,
            parameters=dict(zarr_urls=[]),
            history_item_id=999,
            in_compound_task=True,
        )
        assert result == 42
        assert exception is None


def test_submit_fail():
    def raise_ValueError(parameters: dict):
        raise ValueError("error message")

    with LocalRunner() as runner:
        result, exception = runner.submit(
            raise_ValueError,
            parameters=dict(zarr_urls=[]),
            history_item_id=999,
            in_compound_task=True,
        )
    assert result is None
    assert isinstance(exception, ValueError)


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


def test_multisubmit():
    with LocalRunner() as runner:
        results, exceptions = runner.multisubmit(
            fun,
            [
                dict(zarr_url="a", parameter=1),
                dict(zarr_url="b", parameter=2),
                dict(zarr_url="c", parameter=3),
                dict(zarr_url="d", parameter=4),
            ],
            history_item_id=999,
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


# def test_executor_map_failure():
#     """
#     Iterables of different length -> ValueError
#     """

#     with pytest.raises(ValueError):
#         with LocalRunner() as executor:
#             executor.map(
#                 lambda x, y: 42,
#                 [0, 1],
#                 [2, 3, 4],
#             )
