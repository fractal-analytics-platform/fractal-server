from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from devtools import debug


class FractalThreadPoolExecutor(ThreadPoolExecutor):
    """
    FIXME
    """

    def map(
        self,
        fn,
        *iterables,
        parallel_tasks_per_job: Optional[int] = None,
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

        parallel_tasks_per_job = 1
        debug(parallel_tasks_per_job)

        iterable_lengths = [len(it) for it in iterables]
        if not len(set(iterable_lengths)) == 1:
            raise ValueError("Iterables have different lenghts.")
        n_elements = iterable_lengths[0]

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
