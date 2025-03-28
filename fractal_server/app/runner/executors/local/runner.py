from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from typing import Literal

from sqlmodel import update

from .get_local_config import LocalBackendConfig
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.runner.executors.base_runner import BaseRunner
from fractal_server.app.runner.task_files import TaskFiles
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
                logger.debug(f"[submit] END {result=}")
                if task_type not in ["compound", "converter_compound"]:
                    unit = db.get(HistoryUnit, history_unit_id)
                    unit.status = HistoryUnitStatus.DONE
                    db.merge(unit)
                    db.commit()
                return result, None
            except Exception as e:
                exception = e
                logger.debug(f"[submit] END {exception=}")

                db.execute(
                    update(HistoryUnit)
                    .where(HistoryUnit.id == history_unit_id)
                    .values(status=HistoryUnitStatus.FAILED)
                )
                db.commit()

                return None, exception

    def multisubmit(
        self,
        func: callable,
        list_parameters: list[dict],
        history_unit_ids: list[int],
        list_task_files: list[TaskFiles],
        task_type: Literal[
            "parallel",
            "compound",
            "converter_compound",
        ],
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
        if task_type in ["compound", "converter_compound"]:
            if len(history_unit_ids) != 1:
                raise NotImplementedError(
                    "We are breaking the assumption that compound/multisubmit "
                    "is associated to a single HistoryUnit. This is not "
                    "supported."
                )

        logger.debug(f"[multisubmit] START, {len(list_parameters)=}")

        self.validate_multisubmit_parameters(
            list_parameters=list_parameters,
            task_type=task_type,
            list_task_files=list_task_files,
        )

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
                future = self.executor.submit(
                    func,
                    parameters=kwargs,
                    remote_files=list_task_files[
                        positional_index
                    ].remote_files_dict,
                )
                active_futures[positional_index] = future

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
                        zarr_url = list_parameters[positional_index][
                            "zarr_url"
                        ]
                        if task_type == "parallel":
                            current_history_unit_id = history_unit_ids[
                                positional_index
                            ]

                        try:
                            results[positional_index] = fut.result()
                            print(f"Mark {zarr_url=} as done, {kwargs}")
                            if task_type == "parallel":
                                unit = db.get(
                                    HistoryUnit, current_history_unit_id
                                )
                                unit.status = HistoryUnitStatus.DONE
                                db.merge(unit)
                                db.commit()

                        except Exception as e:
                            print(
                                f"Mark {zarr_url=} as failed, {kwargs} - {e}"
                            )
                            exceptions[positional_index] = e
                            if task_type == "parallel":
                                unit = db.get(
                                    HistoryUnit, current_history_unit_id
                                )
                                unit.status = HistoryUnitStatus.FAILED
                                db.merge(unit)
                                db.commit()

                            # FIXME: what should happen here? Option 1: stop
                            # all existing tasks and shutdown runner (for the
                            # compound-task case)

        logger.debug(f"[multisubmit] END, {results=}, {exceptions=}")

        return results, exceptions
