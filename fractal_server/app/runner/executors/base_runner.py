from typing import Any

from fractal_server.app.runner.components import _COMPONENT_KEY_


class BaseRunner(object):
    """
    Base class for Fractal runners.
    """

    def shutdown(self, *args, **kwargs):
        raise NotImplementedError()

    def submit(
        self,
        func: callable,
        parameters: dict[str, Any],
        history_item_id: int,
        in_compound_task: bool,
        **kwargs,
    ) -> tuple[Any, BaseException]:
        """
        Run a single fractal task.

        # FIXME: Describe more in detail

        Args:
            func:
                Function to be executed.
            parameters:
                Dictionary of parameters. Must include `zarr_urls` key.
            history_item_id:
                Database ID of the corresponding `HistoryItemV2` entry.
            in_compound_task:
                Whether this is the init part of a compound task.
            kwargs:
                Runner-specific parameters.
        """
        raise NotImplementedError()

    def multisubmit(
        self,
        func: callable,
        list_parameters: list[dict[str, Any]],
        history_item_id: int,
        in_compound_task: bool,
        **kwargs,
    ) -> tuple[dict[int, Any], dict[int, BaseException]]:
        """
        Run a parallel fractal task.

        # FIXME: Describe more in detail

        Args:
            func:
                Function to be executed.
            list_parameters:
                List of dictionaries of parameters. Each one must include a
                `zarr_url` key.
            history_item_id:
                Database ID of the corresponding `HistoryItemV2` entry.
            in_compound_task:
                Whether this is the compute part of a compound task.
            kwargs:
                Runner-specific parameters.
        """
        raise NotImplementedError()

    def validate_submit_parameters(self, parameters: dict[str, Any]) -> None:
        """
        Validate parameters for `submit` method

        Args:
            parameters: Parameters dictionary.
        """
        if not isinstance(parameters, dict):
            raise ValueError("`parameters` must be a dictionary.")
        if "zarr_urls" not in parameters.keys():
            raise ValueError(
                f"No 'zarr_urls' key in in {list(parameters.keys())}"
            )
        if _COMPONENT_KEY_ not in parameters.keys():
            raise ValueError(
                f"No '{_COMPONENT_KEY_}' key in in {list(parameters.keys())}"
            )

    def validate_multisubmit_parameters(
        self,
        list_parameters: list[dict[str, Any]],
        in_compound_task: bool,
    ) -> None:
        """
        Validate parameters for `multi_submit` method

        Args:
            list_parameters: List of parameters dictionaries.
            in_compound_task:
               Whether this is the compute part of a compound task.
        """
        for single_kwargs in list_parameters:
            if not isinstance(single_kwargs, dict):
                raise RuntimeError("kwargs itemt must be a dictionary.")
            if "zarr_url" not in single_kwargs.keys():
                raise RuntimeError(
                    f"No 'zarr_url' key in in {list(single_kwargs.keys())}"
                )
            if _COMPONENT_KEY_ not in single_kwargs.keys():
                raise ValueError(
                    f"No '{_COMPONENT_KEY_}' key "
                    f"in {list(single_kwargs.keys())}"
                )
        if not in_compound_task:
            zarr_urls = [kwargs["zarr_url"] for kwargs in list_parameters]
            if len(zarr_urls) != len(set(zarr_urls)):
                raise RuntimeError("Non-unique zarr_urls")
