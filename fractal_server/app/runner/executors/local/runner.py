import json
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from typing import Literal

from ..call_command_wrapper import call_command_wrapper
from .get_local_config import LocalBackendConfig
from fractal_server.app.db import get_sync_db
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.app.runner.executors.base_runner import BaseRunner
from fractal_server.app.runner.task_files import TaskFiles
from fractal_server.app.runner.v2.db_tools import (
    bulk_update_status_of_history_unit,
)
from fractal_server.app.runner.v2.db_tools import update_status_of_history_unit
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.logger import set_logger

logger = set_logger(__name__)


def run_single_task(
    base_command: str,
    parameters: dict[str, Any],
    task_files: TaskFiles,
):

    # Write args.json file
    with open(task_files.args_file_local, "w") as f:
        json.dump(parameters, f)

    # Run command
    full_command = (
        f"{base_command} "
        f"--args-json {task_files.args_file_local} "
        f"--out-json {task_files.metadiff_file_local}"
    )

    call_command_wrapper(
        cmd=full_command,
        log_path=task_files.log_file_local,
    )

    try:
        with open(task_files.metadiff_file_local) as f:
            out_meta = json.load(f)
        return out_meta
    except FileNotFoundError:
        # Command completed, but it produced no metadiff file
        return None


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

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Exit LocalRunner")
        self.executor.shutdown(
            wait=False,
            cancel_futures=True,
        )
        return self.executor.__exit__(exc_type, exc_val, exc_tb)

    def submit(
        self,
        base_command: str,
        workflow_task_order: int,
        workflow_task_id: int,
        task_name: str,
        parameters: dict[str, Any],
        history_unit_id: int,
        task_files: TaskFiles,
        config: LocalBackendConfig,
        task_type: Literal[
            "non_parallel",
            "converter_non_parallel",
            "compound",
            "converter_compound",
        ],
        user_id: int,
    ) -> tuple[Any, Exception]:
        logger.debug("[submit] START")

        try:
            self.validate_submit_parameters(parameters, task_type=task_type)
            workdir_local = task_files.wftask_subfolder_local
            workdir_local.mkdir()

            # SUBMISSION PHASE
            future = self.executor.submit(
                run_single_task,
                base_command=base_command,
                parameters=parameters,
                task_files=task_files,
            )
        except Exception as e:
            logger.error(
                "[submit] Unexpected exception during submission. "
                f"Original error {str(e)}"
            )
            result = None
            exception = TaskExecutionError(str(e))
            with next(get_sync_db()) as db:
                update_status_of_history_unit(
                    history_unit_id=history_unit_id,
                    status=HistoryUnitStatus.FAILED,
                    db_sync=db,
                )
                return None, exception

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
        base_command: str,
        workflow_task_order: int,
        workflow_task_id: int,
        task_name: str,
        list_parameters: list[dict],
        history_unit_ids: list[int],
        list_task_files: list[TaskFiles],
        task_type: Literal["parallel", "compound", "converter_compound"],
        config: LocalBackendConfig,
        user_id: int,
    ) -> tuple[dict[int, Any], dict[int, BaseException]]:
        """
        Note: `list_parameters`, `list_task_files` and `history_unit_ids`
        have the same size. For parallel tasks, this is also the number of
        input images, while for compound tasks these can differ.
        """

        logger.debug(f"[multisubmit] START, {len(list_parameters)=}")
        results: dict[int, Any] = {}
        exceptions: dict[int, BaseException] = {}

        try:

            self.validate_multisubmit_parameters(
                list_parameters=list_parameters,
                task_type=task_type,
                list_task_files=list_task_files,
                history_unit_ids=history_unit_ids,
            )

            workdir_local = list_task_files[0].wftask_subfolder_local
            # Note: the `mkdir` is not needed for compound tasks, but it is
            # needed for parallel tasks
            workdir_local.mkdir(exist_ok=True)

            # Set `n_elements` and `parallel_tasks_per_job`
            n_elements = len(list_parameters)
            parallel_tasks_per_job = config.parallel_tasks_per_job
            if parallel_tasks_per_job is None:
                parallel_tasks_per_job = n_elements

        except Exception as e:
            logger.error(
                "[multisubmit] Unexpected exception during preliminary phase. "
                f"Original error {str(e)}"
            )
            exception = TaskExecutionError(str(e))
            exceptions = {
                ind: exception for ind in range(len(list_parameters))
            }
            if task_type == "parallel":
                with next(get_sync_db()) as db:
                    bulk_update_status_of_history_unit(
                        history_unit_ids=history_unit_ids,
                        status=HistoryUnitStatus.FAILED,
                        db_sync=db,
                    )
            return results, exceptions

        # Execute tasks, in chunks of size `parallel_tasks_per_job`
        for ind_chunk in range(0, n_elements, parallel_tasks_per_job):

            list_parameters_chunk = list_parameters[
                ind_chunk : ind_chunk + parallel_tasks_per_job
            ]

            active_futures: dict[int, Future] = {}
            for ind_within_chunk, kwargs in enumerate(list_parameters_chunk):
                positional_index = ind_chunk + ind_within_chunk
                try:
                    future = self.executor.submit(
                        run_single_task,
                        base_command=base_command,
                        parameters=list_parameters[positional_index],
                        task_files=list_task_files[positional_index],
                    )
                    active_futures[positional_index] = future
                except Exception as e:
                    logger.error(
                        "[multisubmit] Unexpected exception during submission."
                        f" Original error {str(e)}"
                    )
                    current_history_unit_id = history_unit_ids[
                        positional_index
                    ]
                    exceptions[positional_index] = TaskExecutionError(str(e))
                    if task_type == "parallel":
                        with next(get_sync_db()) as db:
                            update_status_of_history_unit(
                                history_unit_id=current_history_unit_id,
                                status=HistoryUnitStatus.FAILED,
                                db_sync=db,
                            )
            while active_futures:
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
                            logger.debug(
                                "Multisubmit failed in retrieval "
                                "phase with the following error "
                                f"{str(e)}"
                            )
                            exceptions[positional_index] = TaskExecutionError(
                                str(e)
                            )
                            if task_type == "parallel":
                                update_status_of_history_unit(
                                    history_unit_id=current_history_unit_id,
                                    status=HistoryUnitStatus.FAILED,
                                    db_sync=db,
                                )

        logger.debug(f"[multisubmit] END, {len(results)=}, {len(exceptions)=}")

        return results, exceptions
