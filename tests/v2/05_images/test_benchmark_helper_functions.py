from functools import wraps
from time import perf_counter

import pytest
from devtools import debug

from fractal_server.app.runner.v2.runner_functions import deduplicate_list
from fractal_server.app.runner.v2.runner_functions import InitArgsModel
from fractal_server.images import Filters
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


def dummy_image_list(N):
    assert N % 4 == 0
    return [
        SingleImage(
            path=f"/tmp_{i}_small",
            attributes=dict(a1=(i % 2), a2="a2", a3="whoknows"),
            types=dict(t1=bool(i % 4)),
        )
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
    new_list = _filter_image_list(
        images=images,
        filters=Filters(
            attributes=dict(a1=0, a2="a2", a3=None),
            types=dict(t1=True, t2=False),
        ),
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
    new_list = _filter_image_list(
        images=images,
        filters=Filters(attributes=dict(a1=0)),
    )
    debug(len(images), len(new_list))
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
