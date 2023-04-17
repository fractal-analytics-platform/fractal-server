# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
"""
Custom version of Python
[ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor)).
"""
from concurrent.futures import ThreadPoolExecutor
from typing import Callable
from typing import Iterable
from typing import Optional
from typing import Sequence

from ._local_config import get_default_local_backend_config
from ._local_config import LocalBackendConfig


class FractalThreadPoolExecutor(ThreadPoolExecutor):
    """
    Custom version of
    [ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor))
    that overrides the `submit` and `map` methods
    """

    def submit(
        self,
        *args,
        local_backend_config: Optional[LocalBackendConfig] = None,
        **kwargs,
    ):
        """
        Compared to the `ThreadPoolExecutor` method, here we accept an addition
        keyword argument (`local_backend_config`), which is then simply
        ignored.
        """
        return super().submit(*args, **kwargs)

    def map(
        self,
        fn: Callable,
        *iterables: Sequence[Iterable],
        local_backend_config: Optional[LocalBackendConfig] = None,
    ):
        """
        Custom version of the `Executor.map` method

        The main change with the respect to the original `map` method is that
        the list of tasks to be executed is split into chunks, and then
        `super().map` is called (sequentially) on each chunk. The goal of this
        change is to limit parallelism, e.g. due to limited computational
        resources.

        Other changes from the `concurrent.futures` `map` method:

        1. Removed `timeout` argument;
        2. Removed `chunksize`;
        3. All iterators (both inputs and output ones) are transformed into
           lists.

        Args:
            fn: A callable function.
            iterables: The argument iterables (one iterable per argument of
                       `fn`).
           local_backend_config: The backend configuration, needed to extract
                                 `parallel_tasks_per_job`.
        """

        # Preliminary check
        iterable_lengths = [len(it) for it in iterables]
        if not len(set(iterable_lengths)) == 1:
            raise ValueError("Iterables have different lengths.")

        # Set total number of arguments
        n_elements = len(iterables[0])

        # Set parallel_tasks_per_job
        if local_backend_config is None:
            local_backend_config = get_default_local_backend_config()
        parallel_tasks_per_job = local_backend_config.parallel_tasks_per_job
        if parallel_tasks_per_job is None:
            parallel_tasks_per_job = n_elements

        # Execute tasks, in chunks of size parallel_tasks_per_job
        results = []
        for ind_chunk in range(0, n_elements, parallel_tasks_per_job):
            chunk_iterables = [
                it[ind_chunk : ind_chunk + parallel_tasks_per_job]  # noqa
                for it in iterables
            ]
            map_iter = super().map(fn, *chunk_iterables)
            results.extend(list(map_iter))

        return iter(results)
