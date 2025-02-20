from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Optional

from ._local_config import get_default_local_backend_config
from ._local_config import LocalBackendConfig
from fractal_server.app.history import HistoryItemImageStatus
from fractal_server.app.history import update_all_images
from fractal_server.app.history import update_single_image


class BaseRunner(object):
    def submit(
        self,
        parameters: dict[str, Any],
        init_of_compound_task: bool = False,
        **kwargs,
    ) -> tuple[Any, BaseException]:
        raise NotImplementedError("'submit' method not available.")

    def multisubmit(
        self,
        func: callable,
        list_parameters: list[dict[str, Any]],
        compute_of_compound_task: bool = False,
        **kwargs,
    ) -> tuple[dict[int, Any], dict[int, BaseException]]:
        raise NotImplementedError("'multisubmit' method not available.")

    def shutdown(self, *args, **kwargs):
        raise NotImplementedError("'shutdown' method not available.")

    def validate_submit_parameters(
        self, single_kwargs: dict[str, Any]
    ) -> None:
        if not isinstance(single_kwargs, dict):
            raise RuntimeError("kwargs itemt must be a dictionary.")
        if "zarr_urls" not in single_kwargs.keys():
            raise RuntimeError(
                f"No 'zarr_urls' key in in {list(single_kwargs.keys())}"
            )

    def validate_multisubmit_parameters(
        self,
        list_parameters: list[dict[str, Any]],
        compute_of_compound_task: bool,
    ) -> None:
        for single_kwargs in list_parameters:
            if not isinstance(single_kwargs, dict):
                raise RuntimeError("kwargs itemt must be a dictionary.")
        if "zarr_url" not in single_kwargs.keys():
            raise RuntimeError(
                f"No 'zarr_url' key in in {list(single_kwargs.keys())}"
            )
        if not compute_of_compound_task:
            zarr_urls = [kwargs["zarr_url"] for kwargs in list_parameters]
            if len(zarr_urls) != len(set(zarr_urls)):
                raise RuntimeError("Non-unique zarr_urls")


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
        init_of_compound_task: bool = False,
        **kwargs,
    ) -> tuple[Any, Exception]:
        self.validate_submit_parameters(parameters)
        import logging

        logging.critical(f"SUBMIT {parameters=}")
        future = self.executor.submit(func, parameters=parameters)
        try:
            result = future.result()
            if not init_of_compound_task:
                update_all_images(
                    history_item_id=history_item_id,
                    status=HistoryItemImageStatus.DONE,
                )
            return result, None
        except Exception as e:
            exception = e
            if not init_of_compound_task:
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
        compute_of_compound_task: bool = False,
        local_backend_config: Optional[LocalBackendConfig] = None,
        **kwargs,
    ):
        self.validate_multisubmit_parameters(
            list_parameters=list_parameters,
            compute_of_compound_task=compute_of_compound_task,
        )

        # Set parallel_tasks_per_job
        n_elements = len(list_parameters)
        if local_backend_config is None:
            local_backend_config = get_default_local_backend_config()
        parallel_tasks_per_job = local_backend_config.parallel_tasks_per_job
        if parallel_tasks_per_job is None:
            parallel_tasks_per_job = n_elements

        # Execute tasks, in chunks of size parallel_tasks_per_job
        results = {}
        exceptions = {}
        for ind_chunk in range(0, n_elements, parallel_tasks_per_job):
            chunk_kwargs = list_parameters[
                ind_chunk : ind_chunk + parallel_tasks_per_job
            ]
            from concurrent.futures import Future

            futures: dict[int, Future] = {}
            for ind_within_chunk, kwargs in enumerate(chunk_kwargs):
                positional_index = ind_chunk + ind_within_chunk
                future = self.executor.submit(
                    func, parameters=kwargs
                )  # FIXME:
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
                        if not compute_of_compound_task:
                            update_single_image(
                                history_item_id=history_item_id,
                                zarr_url=zarr_url,
                                status=HistoryItemImageStatus.DONE,
                            )
                    except Exception as e:
                        print(f"Mark {zarr_url=} as failed, {kwargs} - {e}")
                        exceptions[positional_index] = e
                        if compute_of_compound_task:
                            update_single_image(
                                history_item_id=history_item_id,
                                zarr_url=zarr_url,
                                status=HistoryItemImageStatus.FAILED,
                            )
        if compute_of_compound_task:
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
