from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Optional

from ._local_config import LocalBackendConfig
from fractal_server.app.history import HistoryItemImageStatus
from fractal_server.app.history import update_all_images
from fractal_server.app.history import update_single_image
from fractal_server.app.runner.executors.base_runner import BaseRunner


class LocalRunner(BaseRunner):
    executor: ThreadPoolExecutor

    def __init__(self):
        self.executor = ThreadPoolExecutor()

    def __enter__(self):
        return self

    def shutdown(self):
        self.executor.shutdown(
            wait=False,
            cancel_futures=True,
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
        return self.executor.__exit__(exc_type, exc_val, exc_tb)

    def submit(
        self,
        func: callable,
        parameters: dict[str, Any],
        history_item_id: int,
        in_compound_task: bool = False,
        **kwargs,
    ) -> tuple[Any, Exception]:
        self.validate_submit_parameters(parameters)
        future = self.executor.submit(func, parameters=parameters)
        try:
            result = future.result()
            if not in_compound_task:
                update_all_images(
                    history_item_id=history_item_id,
                    status=HistoryItemImageStatus.DONE,
                )
            return result, None
        except Exception as e:
            exception = e
            if not in_compound_task:
                update_all_images(
                    history_item_id=history_item_id,
                    status=HistoryItemImageStatus.FAILED,
                )
            return None, exception

    def multisubmit(
        self,
        func: callable,
        list_parameters: list[dict],
        history_item_id: int,
        in_compound_task: bool = False,
        local_backend_config: Optional[LocalBackendConfig] = None,
        **kwargs,
    ):
        self.validate_multisubmit_parameters(
            list_parameters=list_parameters,
            in_compound_task=in_compound_task,
        )

        # Set parallel_tasks_per_job
        n_elements = len(list_parameters)
        if local_backend_config is None:
            parallel_tasks_per_job = n_elements
        else:
            parallel_tasks_per_job = (
                local_backend_config.parallel_tasks_per_job
            )

        # Execute tasks, in chunks of size `parallel_tasks_per_job`
        results = {}
        exceptions = {}
        for ind_chunk in range(0, n_elements, parallel_tasks_per_job):
            list_parameters_chunk = list_parameters[
                ind_chunk : ind_chunk + parallel_tasks_per_job
            ]
            from concurrent.futures import Future

            futures: dict[int, Future] = {}
            for ind_within_chunk, kwargs in enumerate(list_parameters_chunk):
                positional_index = ind_chunk + ind_within_chunk
                future = self.executor.submit(func, parameters=kwargs)
                futures[positional_index] = future

            while futures:
                finished_futures = [
                    keyval
                    for keyval in futures.items()
                    if not keyval[1].running()
                ]
                for positional_index, fut in finished_futures:
                    futures.pop(positional_index)
                    zarr_url = list_parameters[positional_index]["zarr_url"]
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

        return results, exceptions
