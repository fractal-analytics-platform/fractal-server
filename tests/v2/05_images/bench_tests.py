from functools import wraps
from time import perf_counter

import pytest
from devtools import debug

from fractal_server.images import _filter_image_list
from fractal_server.images import deduplicate_list
from fractal_server.images import SingleImage


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


@pytest.mark.parametrize(
    "images",
    [
        [
            SingleImage(path=f"/tmp_{i}_small", attributes=dict(tag=i % 2))
            for i in range(10)
        ],
        [
            SingleImage(path=f"/tmp_{i}_medium", attributes=dict(tag=i % 2))
            for i in range(100)
        ],
        [
            SingleImage(path=f"/tmp_{i}_large", attributes=dict(tag=i % 2))
            for i in range(1000)
        ],
        [
            SingleImage(path=f"/tmp_{i}_extra", attributes=dict(tag=i % 2))
            for i in range(10000)
        ],
        [
            SingleImage(path=f"/tmp_{i}_extra", attributes=dict(tag=i % 2))
            for i in range(100000)
        ],
    ],
)
@benchmark
def test_filter_image_list(
    images,
):
    _filter_image_list(images=images, attribute_filters=dict(tag=0))


@pytest.mark.parametrize(
    "this_list",
    [
        [
            SingleImage(path=f"/tmp_{ind_image}")
            for ind_time in range(1)
            for ind_image in range(20)
        ],
        [
            SingleImage(path=f"/tmp_{ind_image}")
            for ind_time in range(1)
            for ind_image in range(400)
        ],
        [SingleImage(path=f"/tmp_{ind_image}") for ind_image in range(1000)],
        # [
        #     SingleImage(path=f"/tmp_{ind_image}")
        #     for ind_time in range(25)
        #     for ind_image in range(400)
        # ],
    ],
)
@benchmark
def test_deduplicate_list(
    this_list,
):
    new_list = deduplicate_list(this_list=this_list)

    debug(len(this_list), len(new_list))
