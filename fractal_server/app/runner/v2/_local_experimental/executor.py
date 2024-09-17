"""
Custom version of Python
[ProcessPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor)).
"""
import logging
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
from typing import Callable
from typing import Iterable
from typing import Optional
from typing import Sequence

import psutil

from ._local_config import get_default_local_backend_config
from ._local_config import LocalBackendConfig
from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.logger import set_logger

logger = set_logger("FractalProcessPoolExecutor")


class FractalProcessPoolExecutor(ProcessPoolExecutor):

    shutdown_file: Path
    interval: float
    _shutdown: bool
    _shutdown_file_thread: threading.Thread

    def __init__(
        self, shutdown_file: Path, interval: float = 1.0, *args, **kwargs
    ):
        logging.warning("XXXD01 [FractalProcessPoolExecutor.__init__] START")
        super().__init__(*args, **kwargs)
        self.shutdown_file = Path(shutdown_file)
        self.interval = float(interval)
        logger.debug(
            f"Start monitoring {shutdown_file} every {interval} seconds"
        )
        self._shutdown = False
        self._shutdown_file_thread = threading.Thread(
            target=self._run, daemon=True
        )
        self._shutdown_file_thread.start()
        logging.warning(
            f"XXXD02 [FractalProcessPoolExecutor.__init__] {self._shutdown_file_thread=}"
        )
        logging.warning(
            f"XXXD03 [FractalProcessPoolExecutor.__init__] END, with {id(self)=}"
        )

    def _run(self):
        """
        Running on '_shutdown_file_thread'.
        """
        logging.warning("XXXD04 [FractalProcessPoolExecutor._run] START")

        while True:

            if self.shutdown_file.exists() or self._shutdown:
                logging.warning(
                    f"XXXD05 [FractalProcessPoolExecutor._run] DETECTED SHUTDOWN for {id(self)=}"
                )
                try:
                    self._terminate_processes()
                except Exception as e:
                    logger.error(
                        "Terminate processes failed. "
                        f"Original error: {str(e)}."
                    )
                finally:
                    return
            time.sleep(self.interval)

    def _terminate_processes(self):
        """
        Running on '_shutdown_file_thread'.
        """

        logger.info("Start terminating FractalProcessPoolExecutor processes.")
        logging.warning(
            f"XXXD06 [FractalProcessPoolExecutor._terminate_processes] START, with {id(self)=}"
        )
        # We use 'psutil' in order to easily access the PIDs of the children.
        if self._processes is not None:
            logging.warning(
                f"XXXD07 [FractalProcessPoolExecutor._terminate_processes] {self._processes=}, with {id(self)=}"
            )
            for pid in self._processes.keys():
                logging.info(
                    f"XXXD08 [FractalProcessPoolExecutor._terminate_processes] handle {pid=} with {id(self)=}"
                )
                parent = psutil.Process(pid)
                children = parent.children(recursive=True)
                for child in children:
                    logging.warning(
                        f"XXXD09 [FractalProcessPoolExecutor._terminate_processes] handle {child=} with {id(self)=}"
                    )
                    child.kill()
                logging.warning(
                    f"XXXD10 [FractalProcessPoolExecutor._terminate_processes] handle {parent=} with {id(self)=}"
                )
                parent.kill()
                logger.info(f"Process {pid} and its children terminated.")
        logger.info("FractalProcessPoolExecutor processes terminated.")

    def shutdown(self, *args, **kwargs) -> None:
        logging.warning(
            f"XXXD11 [FractalProcessPoolExecutor._terminate_processes] set _shutdown=True {id(self)=}"
        )
        self._shutdown = True
        logging.warning(
            f"XXXD12 [FractalProcessPoolExecutor._terminate_processes] POST thread join {id(self)=}"
        )
        self._shutdown_file_thread.join()
        logging.warning(
            f"XXXD13 [FractalProcessPoolExecutor._terminate_processes] POST thread join {id(self)=}"
        )
        return super().shutdown(*args, **kwargs)

    def submit(
        self,
        *args,
        local_backend_config: Optional[LocalBackendConfig] = None,
        **kwargs,
    ):
        """
        Compared to the `ProcessPoolExecutor` method, here we accept an
        additional keyword argument (`local_backend_config`), which is then
        simply ignored.
        """
        logging.warning(
            f"XXXD14 [FractalProcessPoolExecutor._terminate_processes] PRE submit, {id(self)=}"
        )
        out = super().submit(*args, **kwargs)
        logging.warning(
            f"XXXD15 [FractalProcessPoolExecutor._terminate_processes] POST submit, {id(self)=}"
        )
        return out

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

        logging.warning(
            f"XXXD16 [FractalProcessPoolExecutor.map] START {id(self)=}"
        )
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
            logging.warning(
                f"XXXD17 [FractalProcessPoolExecutor.map] PRE map {id(self)=}, {chunk_iterables=}"
            )

            map_iter = super().map(fn, *chunk_iterables)

            try:
                results.extend(list(map_iter))
            except BrokenProcessPool as e:
                raise JobExecutionError(info=e.args[0])
            logging.warning(
                f"XXXD18 [FractalProcessPoolExecutor.map] POST map {id(self)=}, {chunk_iterables=}"
            )

        return iter(results)
