"""
Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
University of Zurich

Original author(s):
Tommaso Comparin <tommaso.comparin@exact-lab.it>

This file is part of Fractal and was originally developed by eXact lab S.r.l.
<exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
Institute for Biomedical Research and Pelkmans Lab from the University of
Zurich.
"""
import pytest
from devtools import debug

from fractal_server.app.runner._local._local_config import LocalBackendConfig
from fractal_server.app.runner._local.executor import FractalThreadPoolExecutor


def test_executor_submit():
    with FractalThreadPoolExecutor() as executor:
        res = executor.submit(lambda: 42)
    assert res.result() == 42


def test_executor_submit_with_exception():
    def raise_ValueError():
        raise ValueError

    with pytest.raises(ValueError) as e:
        with FractalThreadPoolExecutor() as executor:
            fut = executor.submit(raise_ValueError)
            debug(fut.result())
    debug(e.value)


@pytest.mark.parametrize("parallel_tasks_per_job", [None, 1, 2, 3, 4, 8, 16])
def test_executor_map(parallel_tasks_per_job: int):
    local_backend_config = LocalBackendConfig(
        parallel_tasks_per_job=parallel_tasks_per_job
    )

    NUM = 7

    # Test function of a single variable
    with FractalThreadPoolExecutor() as executor:

        def fun_x(x):
            return 3 * x + 1

        inputs = list(range(NUM))
        result_generator = executor.map(
            fun_x,
            inputs,
            local_backend_config=local_backend_config,
        )
        results = list(result_generator)
        assert results == [fun_x(x) for x in inputs]

    # Test function of two variables
    with FractalThreadPoolExecutor() as executor:

        def fun_xy(x, y):
            return 2 * x + y

        inputs_x = list(range(3, 3 + NUM))
        inputs_y = list(range(NUM))
        result_generator = executor.map(
            fun_xy,
            inputs_x,
            inputs_y,
            local_backend_config=local_backend_config,
        )
        results = list(result_generator)
        assert results == [fun_xy(x, y) for x, y in zip(inputs_x, inputs_y)]


@pytest.mark.parametrize("parallel_tasks_per_job", [None, 1, 2, 4, 8, 16])
def test_executor_map_with_exception(parallel_tasks_per_job):
    def _raise(n: int):
        if n == 5:
            raise ValueError
        else:
            return n

    local_backend_config = LocalBackendConfig(
        parallel_tasks_per_job=parallel_tasks_per_job
    )

    with pytest.raises(ValueError):
        with FractalThreadPoolExecutor() as executor:
            _ = executor.map(
                _raise,
                range(10),
                local_backend_config=local_backend_config,
            )
