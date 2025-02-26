import time

import pytest
from devtools import debug

from fractal_server.app.history import HistoryItemImageStatus
from fractal_server.app.models.v2.history import HistoryItemV2
from fractal_server.app.runner.executors.local.runner import (
    LocalRunner,
)

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


async def test_submit_success(db, mock_history_item):
    def do_nothing(parameters: dict) -> int:
        return 42

    with LocalRunner() as runner:
        result, exception = runner.submit(
            do_nothing,
            parameters=dict(zarr_urls=["a", "b", "c", "d"]),
            history_item_id=mock_history_item.id,
        )
        assert result == 42
        assert exception is None
    db.expunge_all()
    history_item = await db.get(HistoryItemV2, mock_history_item.id)
    assert history_item.images == {
        zarr_url: HistoryItemImageStatus.DONE for zarr_url in ALL_IMAGES
    }


async def test_submit_fail(db, mock_history_item):
    def raise_ValueError(parameters: dict):
        raise ValueError("error message")

    with LocalRunner() as runner:
        result, exception = runner.submit(
            raise_ValueError,
            parameters=dict(zarr_urls=[]),
            history_item_id=mock_history_item.id,
        )
    assert result is None
    assert isinstance(exception, ValueError)
    db.expunge_all()
    history_item = await db.get(HistoryItemV2, mock_history_item.id)
    assert history_item.images == {
        zarr_url: HistoryItemImageStatus.FAILED for zarr_url in ALL_IMAGES
    }


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


async def test_multisubmit(db, mock_history_item):
    with LocalRunner() as runner:
        results, exceptions = runner.multisubmit(
            fun,
            [
                dict(zarr_url="a", parameter=1),
                dict(zarr_url="b", parameter=2),
                dict(zarr_url="c", parameter=3),
                dict(zarr_url="d", parameter=4),
            ],
            history_item_id=mock_history_item.id,
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
