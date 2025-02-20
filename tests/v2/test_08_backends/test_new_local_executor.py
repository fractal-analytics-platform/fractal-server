import time

from devtools import debug

from fractal_server.app.runner.v2._local.executor import (
    LocalRunner,
)


def fun(*, zarr_url: str, parameter: int):
    if parameter != 3:
        print(f"Running with {parameter=}, returning {2*parameter=}.")
        time.sleep(1)
        return 2 * parameter
    else:
        print(f"Running with {parameter=}, raising error.")
        time.sleep(1)
        raise ValueError("parameter=3 is very very bad")


def test_LocalRunner():
    runner = LocalRunner()

    result, exception = runner.submit(
        fun,
        parameters=dict(
            zarr_urls=["a"],
            parameter=1,
        ),
    )
    debug(result)
    debug(exception)

    results, exceptions = runner.multisubmit(
        fun,
        [
            dict(zarr_url="a", parameter=1),
            dict(zarr_url="b", parameter=2),
            dict(zarr_url="c", parameter=3),
            dict(zarr_url="d", parameter=4),
        ],
    )
    debug(results)
    debug(exceptions)
