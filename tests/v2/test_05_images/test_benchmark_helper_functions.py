from functools import wraps
from time import perf_counter

import pytest
from devtools import debug

from fractal_server.images import SingleImage
from fractal_server.images.tools import filter_image_list
from fractal_server.runner.v2.deduplicate_list import deduplicate_list
from fractal_server.runner.v2.task_interface import InitArgsModel


def benchmark(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        t_start = perf_counter()
        res = func(*args, **kwargs)
        t_end = perf_counter()
        time_total = round((t_end - t_start), 4)
        debug(time_total)
        return res

    return wrapper


def dummy_image_list(N):
    assert N % 4 == 0
    return [
        SingleImage(
            zarr_url=f"/tmp_{i}_small",
            attributes=dict(a1=(i % 2), a2="a2", a3="whoknows"),
            types=dict(t1=bool(i % 4)),
        ).model_dump()
        for i in range(N)
    ]


@pytest.mark.parametrize(
    "images",
    [dummy_image_list(N) for N in [20, 200, 2_000, 20_000]],
)
@benchmark
def test_filter_image_list_with_filters(
    images,
):
    new_list = filter_image_list(
        images=images,
        attribute_filters=dict(a1=[0], a2=["a2"]),
        type_filters=dict(t1=True, t2=False),
    )
    debug(len(images), len(new_list))
    assert len(new_list) == len(images) // 4


@pytest.mark.parametrize(
    "images",
    [dummy_image_list(N) for N in [20, 200, 2_000, 20_000]],
)
@benchmark
def test_filter_image_list_few_filters(
    images,
):
    new_list = filter_image_list(
        images=images,
        attribute_filters=dict(a1=[0]),
    )
    debug(len(images), len(new_list))
    assert len(new_list) == len(images) // 2


@pytest.mark.parametrize(
    "this_list",
    [
        [
            SingleImage(
                zarr_url=f"/tmp_{ind_image}",
                types=dict(a=True),
                attributes=dict(b=1),
            )
            for ind_time_slice in range(10)
            for ind_image in range(100)
        ],
        [
            SingleImage(
                zarr_url=f"/tmp_{ind_image}",
                types=dict(a=True),
                attributes=dict(b=1),
            )
            for ind_image in range(1_000)
        ],
        [
            InitArgsModel(zarr_url=f"/tmp_{ind_image}", init_args=dict(a=1))
            for ind_time_slice in range(10)
            for ind_image in range(100)
        ],
        [
            InitArgsModel(zarr_url=f"/tmp_{ind_image}", init_args=dict(a=1))
            for ind_image in range(1_000)
        ],
    ],
)
@benchmark
def test_deduplicate_list(this_list):
    """
    This test can be used as a benchmark, by making it run
    on larger image lists.
    """
    new_list = deduplicate_list(this_list=this_list)

    debug(len(this_list), len(new_list))
