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
from fractal_server.app.history import HistoryItemImageStatus
from fractal_server.app.history import update_single_image


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

    def multisubmit(
        self,
        func: Callable,
        list_kwargs: list[dict],
        local_backend_config: Optional[LocalBackendConfig] = None,
        history_item_id: Optional[int] = None,
    ):
        """
        FIXME
        """

        for kwargs in list_kwargs:
            if not isinstance(kwargs, dict):
                raise RuntimeError("kwargs itemt must be a dictionary.")
            if "zarr_url" not in kwargs.keys():
                raise RuntimeError(f"No 'zarr_url' in {list(kwargs.keys())}")
        zarr_urls = [kwargs["zarr_url"] for kwargs in list_kwargs]
        if len(zarr_urls) != len(set(zarr_urls)):
            raise RuntimeError("Non-unique zarr_urls")

        # Set parallel_tasks_per_job
        n_elements = len(list_kwargs)
        if local_backend_config is None:
            local_backend_config = get_default_local_backend_config()
        parallel_tasks_per_job = local_backend_config.parallel_tasks_per_job
        if parallel_tasks_per_job is None:
            parallel_tasks_per_job = n_elements

        # Execute tasks, in chunks of size parallel_tasks_per_job
        results = {}
        exceptions = {}
        for ind_chunk in range(0, n_elements, parallel_tasks_per_job):
            chunk_kwargs = list_kwargs[
                ind_chunk : ind_chunk + parallel_tasks_per_job
            ]
            from concurrent.futures import Future

            futures: dict[int, Future] = {}
            for ind_within_chunk, kwargs in enumerate(chunk_kwargs):
                positional_index = ind_chunk + ind_within_chunk
                future = super().submit(func, **kwargs)
                futures[positional_index] = future

            while futures:
                finished_futures = [
                    keyval
                    for keyval in futures.items()
                    if not keyval[1].running()
                ]
                for positional_index, fut in finished_futures:
                    futures.pop(positional_index)
                    zarr_url = list_kwargs[positional_index]["zarr_url"]
                    try:
                        results[positional_index] = fut.result()
                        print(f"Mark {zarr_url=} as done, {kwargs}")
                        if history_item_id is not None:
                            update_single_image(
                                history_item_id=history_item_id,
                                zarr_url=zarr_url,
                                status=HistoryItemImageStatus.DONE,
                            )
                    except Exception as e:
                        print(f"Mark {zarr_url=} as failed, {kwargs} - {e}")
                        exceptions[positional_index] = e
                        if history_item_id is not None:
                            update_single_image(
                                history_item_id=history_item_id,
                                zarr_url=zarr_url,
                                status=HistoryItemImageStatus.FAILED,
                            )

        return results, exceptions
