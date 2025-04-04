from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from typing import Literal

from .get_local_config import LocalBackendConfig
from fractal_server.app.db import get_sync_db
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.base_runner import BaseRunner
from fractal_server.app.runner.task_files import MULTISUBMIT_PREFIX
from fractal_server.app.runner.task_files import SUBMIT_PREFIX
from fractal_server.app.runner.task_files import TaskFiles
from fractal_server.app.runner.v2.db_tools import (
    update_logfile_of_history_unit,
)
from fractal_server.app.runner.v2.db_tools import update_status_of_history_unit
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.logger import set_logger


logger = set_logger(__name__)


class LocalRunner(BaseRunner):
    executor: ThreadPoolExecutor
    root_dir_local: Path

    def __init__(
        self,
        root_dir_local: Path,
    ):

        self.root_dir_local = root_dir_local
        self.root_dir_local.mkdir(parents=True, exist_ok=True)
        self.executor = ThreadPoolExecutor()
        logger.debug("Create LocalRunner")

    def __enter__(self):
        logger.debug("Enter LocalRunner")
        return self

    def shutdown(self):
        logger.debug("Now shut LocalRunner.executor down")
        self.executor.shutdown(
            wait=False,
            cancel_futures=True,
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Exit LocalRunner")
        self.shutdown()
        return self.executor.__exit__(exc_type, exc_val, exc_tb)

    def submit(
        self,
        func: callable,
        parameters: dict[str, Any],
        history_unit_id: int,
        task_files: TaskFiles,
        task_type: Literal[
            "non_parallel",
            "converter_non_parallel",
            "compound",
            "converter_compound",
        ],
        config: LocalBackendConfig,
    ) -> tuple[Any, Exception]:
        logger.debug("[submit] START")

        self.validate_submit_parameters(parameters, task_type=task_type)
        workdir_local = task_files.wftask_subfolder_local
        workdir_local.mkdir()

        # Add prefix to task_files object
        task_files.prefix = SUBMIT_PREFIX
        update_logfile_of_history_unit(
            history_unit_id=history_unit_id,
            logfile=task_files.log_file_local,
        )

        # SUBMISSION PHASE
        future = self.executor.submit(
            func,
            parameters=parameters,
            remote_files=task_files.remote_files_dict,
        )

        # RETRIEVAL PHASE
        with next(get_sync_db()) as db:
            try:
                result = future.result()
                logger.debug("[submit] END with result")
                if task_type not in ["compound", "converter_compound"]:
                    update_status_of_history_unit(
                        history_unit_id=history_unit_id,
                        status=HistoryUnitStatus.DONE,
                        db_sync=db,
                    )
                return result, None
            except Exception as e:
                logger.debug("[submit] END with exception")
                update_status_of_history_unit(
                    history_unit_id=history_unit_id,
                    status=HistoryUnitStatus.FAILED,
                    db_sync=db,
                )
                return None, TaskExecutionError(str(e))

    def multisubmit(
        self,
        func: callable,
        list_parameters: list[dict],
        history_unit_ids: list[int],
        list_task_files: list[TaskFiles],
        task_type: Literal["parallel", "compound", "converter_compound"],
        config: LocalBackendConfig,
    ):
        """
        Note:

        1. The number of sruns and futures is equal to `len(list_parameters)`.
        2. The number of `HistoryUnit`s is equal to `len(history_unit_ids)`.
        3. For compound tasks, these two numbers are not the same.

        For this reason, we defer database updates to the caller function,
        when we are in one of the "compound" cases

        """

        self.validate_multisubmit_parameters(
            list_parameters=list_parameters,
            task_type=task_type,
            list_task_files=list_task_files,
        )

        self.validate_multisubmit_history_unit_ids(
            history_unit_ids=history_unit_ids,
            task_type=task_type,
            list_parameters=list_parameters,
        )

        logger.debug(f"[multisubmit] START, {len(list_parameters)=}")

        workdir_local = list_task_files[0].wftask_subfolder_local
        if task_type == "parallel":
            workdir_local.mkdir()

        # Set `n_elements` and `parallel_tasks_per_job`
        n_elements = len(list_parameters)
        parallel_tasks_per_job = config.parallel_tasks_per_job
        if parallel_tasks_per_job is None:
            parallel_tasks_per_job = n_elements

        # Execute tasks, in chunks of size `parallel_tasks_per_job`
        results: dict[int, Any] = {}
        exceptions: dict[int, BaseException] = {}
        for ind_chunk in range(0, n_elements, parallel_tasks_per_job):
            list_parameters_chunk = list_parameters[
                ind_chunk : ind_chunk + parallel_tasks_per_job
            ]

            active_futures: dict[int, Future] = {}
            for ind_within_chunk, kwargs in enumerate(list_parameters_chunk):
                positional_index = ind_chunk + ind_within_chunk
                list_task_files[
                    positional_index
                ].prefix = f"{MULTISUBMIT_PREFIX}-{positional_index:06d}"
                future = self.executor.submit(
                    func,
                    parameters=kwargs,
                    remote_files=list_task_files[
                        positional_index
                    ].remote_files_dict,
                )
                active_futures[positional_index] = future

                if task_type == "parallel":
                    # FIXME: replace loop with a `bulk_update_history_unit`
                    # function
                    update_logfile_of_history_unit(
                        history_unit_id=history_unit_ids[positional_index],
                        logfile=list_task_files[
                            positional_index
                        ].log_file_local,
                    )
                else:
                    logger.debug(
                        f"Unclear what logfile to associate to {task_type=} "
                        "within multisubmit (see issue #2382)."
                    )
                    # FIXME: Improve definition for compound tasks
                    pass

            while active_futures:
                # FIXME: add shutdown detection
                # if file exists: cancel all futures, and raise
                finished_futures = [
                    index_and_future
                    for index_and_future in active_futures.items()
                    if not index_and_future[1].running()
                ]
                if len(finished_futures) == 0:
                    continue

                with next(get_sync_db()) as db:
                    for positional_index, fut in finished_futures:
                        active_futures.pop(positional_index)
                        if task_type == "parallel":
                            current_history_unit_id = history_unit_ids[
                                positional_index
                            ]

                        try:
                            results[positional_index] = fut.result()
                            if task_type == "parallel":
                                update_status_of_history_unit(
                                    history_unit_id=current_history_unit_id,
                                    status=HistoryUnitStatus.DONE,
                                    db_sync=db,
                                )

                        except Exception as e:
                            exceptions[positional_index] = TaskExecutionError(
                                str(e)
                            )
                            if task_type == "parallel":
                                update_status_of_history_unit(
                                    history_unit_id=current_history_unit_id,
                                    status=HistoryUnitStatus.FAILED,
                                    db_sync=db,
                                )

                            # FIXME: what should happen here? Option 1: stop
                            # all existing tasks and shutdown runner (for the
                            # compound-task case)

        logger.debug(f"[multisubmit] END, {results=}, {exceptions=}")

        return results, exceptions
