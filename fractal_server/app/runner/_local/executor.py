from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from ._local_config import LocalBackendConfig


class FractalThreadPoolExecutor(ThreadPoolExecutor):
    """
    FIXME
    """

    def submit(
        self,
        *args,
        local_backend_config: Optional[LocalBackendConfig] = None,
        **kwargs,
    ):
        return super().submit(*args, **kwargs)

    def map(
        self,
        fn,
        *iterables,
        local_backend_config: Optional[LocalBackendConfig] = None,
    ):
        """
        FIXME: docstring

        Changes from concurrent.futures interface:
        * Remove timeout
        * Remove chunksize
        * All iterators are transformed into lists

        Args:
            fn: A callable that will take as many arguments as there are
                passed iterables.
        """

        iterable_lengths = [len(it) for it in iterables]
        if not len(set(iterable_lengths)) == 1:
            raise ValueError("Iterables have different lengths.")
        n_elements = iterable_lengths[0]

        parallel_tasks_per_job = local_backend_config.parallel_tasks_per_job

        if parallel_tasks_per_job is None:
            parallel_tasks_per_job = n_elements

        results = []
        for ind_chunk in range(0, n_elements, parallel_tasks_per_job):
            chunked_iterables = [
                it[ind_chunk : ind_chunk + parallel_tasks_per_job]  # noqa
                for it in iterables
            ]
            map_iter = super().map(fn, *chunked_iterables)
            results.extend(list(map_iter))

        return iter(results)
