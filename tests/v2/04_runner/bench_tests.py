from functools import wraps
from time import perf_counter

import pytest
from devtools import debug

from fractal_server.app.runner.v2.runner_functions import deduplicate_list
from fractal_server.app.runner.v2.runner_functions import InitArgsModel
from fractal_server.images import SingleImage
from fractal_server.images.tools import _filter_image_list


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
            for i in range(10_000)
        ],
        [
            SingleImage(path=f"/tmp_{i}_extra", attributes=dict(tag=i % 2))
            for i in range(100_000)
        ],
    ],
)
@benchmark
def test_filter_image_list(
    images,
):
    debug(len(images))
    new_list = _filter_image_list(images=images, attribute_filters=dict(tag=0))
    assert len(new_list) == len(images) // 2


@pytest.mark.parametrize(
    "this_list,model",
    [
        (
            [
                SingleImage(
                    path=f"/tmp_{ind_image}",
                    types=dict(a=True),
                    attributes=dict(b=1),
                )
                for ind_time_slice in range(25)
                for ind_image in range(400)
            ],
            SingleImage,
        ),
        (
            [
                SingleImage(
                    path=f"/tmp_{ind_image}",
                    types=dict(a=True),
                    attributes=dict(b=1),
                )
                for ind_image in range(10_000)
            ],
            SingleImage,
        ),
        (
            [
                InitArgsModel(path=f"/tmp_{ind_image}", init_args=dict(a=1))
                for ind_time_slice in range(25)
                for ind_image in range(400)
            ],
            InitArgsModel,
        ),
        (
            [
                InitArgsModel(path=f"/tmp_{ind_image}", init_args=dict(a=1))
                for ind_image in range(10_000)
            ],
            InitArgsModel,
        ),
    ],
)
@benchmark
def test_deduplicate_list(this_list, model):
    new_list = deduplicate_list(this_list=this_list, PydanticModel=model)

    debug(len(this_list), len(new_list))
