from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from typing import Optional

from ._local_config import get_default_local_backend_config
from ._local_config import LocalBackendConfig
from fractal_server.app.history import HistoryItemImageStatus
from fractal_server.app.history import update_all_images
from fractal_server.app.history import update_single_image
from fractal_server.app.history import update_single_image_logfile
from fractal_server.app.runner.components import _COMPONENT_KEY_
from fractal_server.app.runner.executors.base_runner import BaseRunner
from fractal_server.app.runner.task_files import TaskFiles
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
        history_item_id: int,
        task_files: TaskFiles,
        in_compound_task: bool = False,
        **kwargs,
    ) -> tuple[Any, Exception]:
        logger.debug("[submit] START")

        current_task_files = TaskFiles(
            **task_files.model_dump(
                exclude={"component"},
            ),
            component=parameters[_COMPONENT_KEY_],
        )

        self.validate_submit_parameters(parameters)
        workdir_local = current_task_files.wftask_subfolder_local
        workdir_local.mkdir()
        # SUBMISSION PHASE
        future = self.executor.submit(func, parameters=parameters)

        # RETRIEVAL PHASE
        try:
            result = future.result()
            if not in_compound_task:
                update_all_images(
                    history_item_id=history_item_id,
                    status=HistoryItemImageStatus.DONE,
                    logfile=current_task_files.log_file_local,
                )
            logger.debug(f"[submit] END {result=}")
            return result, None
        except Exception as e:
            exception = e
            update_all_images(
                history_item_id=history_item_id,
                status=HistoryItemImageStatus.FAILED,
                logfile=current_task_files.log_file_local,
            )
            logger.debug(f"[submit] END {exception=}")
            return None, exception

    def multisubmit(
        self,
        func: callable,
        list_parameters: list[dict],
        history_item_id: int,
        task_files: TaskFiles,
        in_compound_task: bool = False,
        local_backend_config: Optional[LocalBackendConfig] = None,
        **kwargs,
    ):
        logger.debug(f"[multisubmit] START, {len(list_parameters)=}")

        self.validate_multisubmit_parameters(
            list_parameters=list_parameters,
            in_compound_task=in_compound_task,
        )

        workdir_local = task_files.wftask_subfolder_local
        if not in_compound_task:
            workdir_local.mkdir()

        # Get local_backend_config
        if local_backend_config is None:
            local_backend_config = get_default_local_backend_config()

        # Set `n_elements` and `parallel_tasks_per_job`
        n_elements = len(list_parameters)
        parallel_tasks_per_job = local_backend_config.parallel_tasks_per_job
        if parallel_tasks_per_job is None:
            parallel_tasks_per_job = n_elements

        original_task_files = task_files

        # Execute tasks, in chunks of size `parallel_tasks_per_job`
        results = {}
        exceptions = {}
        for ind_chunk in range(0, n_elements, parallel_tasks_per_job):
            list_parameters_chunk = list_parameters[
                ind_chunk : ind_chunk + parallel_tasks_per_job
            ]
            from concurrent.futures import Future

            active_futures: dict[int, Future] = {}
            active_task_files: dict[int, TaskFiles] = {}
            for ind_within_chunk, kwargs in enumerate(list_parameters_chunk):
                positional_index = ind_chunk + ind_within_chunk
                component = kwargs[_COMPONENT_KEY_]
                future = self.executor.submit(func, parameters=kwargs)
                active_futures[positional_index] = future
                active_task_files[positional_index] = TaskFiles(
                    **original_task_files.model_dump(exclude={"component"}),
                    component=component,
                )

            while active_futures:
                # FIXME: add shutdown detection
                # if file exists: cancel all futures, and raise
                finished_futures = [
                    keyval
                    for keyval in active_futures.items()
                    if not keyval[1].running()
                ]
                for positional_index, fut in finished_futures:
                    active_futures.pop(positional_index)
                    current_task_files = active_task_files.pop(
                        positional_index
                    )
                    zarr_url = list_parameters[positional_index]["zarr_url"]
                    if not in_compound_task:
                        update_single_image_logfile(
                            history_item_id=history_item_id,
                            zarr_url=zarr_url,
                            logfile=current_task_files.log_file_local,
                        )
                    try:
                        results[positional_index] = fut.result()
                        print(f"Mark {zarr_url=} as done, {kwargs}")
                        if not in_compound_task:
                            update_single_image(
                                history_item_id=history_item_id,
                                zarr_url=zarr_url,
                                status=HistoryItemImageStatus.DONE,
                            )
                    except Exception as e:
                        print(f"Mark {zarr_url=} as failed, {kwargs} - {e}")
                        exceptions[positional_index] = e
                        if not in_compound_task:
                            update_single_image(
                                history_item_id=history_item_id,
                                zarr_url=zarr_url,
                                status=HistoryItemImageStatus.FAILED,
                            )
        if in_compound_task:
            if exceptions == {}:
                update_all_images(
                    history_item_id=history_item_id,
                    status=HistoryItemImageStatus.DONE,
                )
            else:
                update_all_images(
                    history_item_id=history_item_id,
                    status=HistoryItemImageStatus.FAILED,
                )
        logger.debug(f"[multisubmit] END, {results=}, {exceptions=}")

        return results, exceptions
