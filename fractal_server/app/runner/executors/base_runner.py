from typing import Any

from fractal_server.app.runner.task_files import TaskFiles
from fractal_server.app.schemas.v2.task import TaskTypeType
from fractal_server.logger import set_logger


TASK_TYPES_SUBMIT: list[TaskTypeType] = [
    "compound",
    "converter_compound",
    "non_parallel",
    "converter_non_parallel",
]
TASK_TYPES_MULTISUBMIT: list[TaskTypeType] = [
    "compound",
    "converter_compound",
    "parallel",
]

logger = set_logger(__name__)


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
        history_unit_id: int,
        task_files: TaskFiles,
        task_type: TaskTypeType,
        config: Any,
    ) -> tuple[Any, BaseException]:
        """
        Run a single fractal task.

        # FIXME: Describe more in detail

        Args:
            func: Function to be executed.
            parameters:
                Dictionary of parameters. Must include `zarr_urls` key.
            history_item_id:
                Database ID of the corresponding `HistoryItemV2` entry.
            task_type: Task type.
            config: Runner-specific parameters.
        """
        raise NotImplementedError()

    def multisubmit(
        self,
        func: callable,
        list_parameters: list[dict[str, Any]],
        history_unit_ids: list[int],
        list_task_files: list[TaskFiles],
        task_type: TaskTypeType,
        config: Any,
    ) -> tuple[dict[int, Any], dict[int, BaseException]]:
        """
        Run a parallel fractal task.

        # FIXME: Describe more in detail

        Args:
            func: Function to be executed.
            list_parameters:
                List of dictionaries of parameters. Each one must include a
                `zarr_url` key.
            history_item_id:
                Database ID of the corresponding `HistoryItemV2` entry.
            task_type: Task type.
            config: Runner-specific parameters.
        """
        raise NotImplementedError()

    def validate_submit_parameters(
        self,
        parameters: dict[str, Any],
        task_type: TaskTypeType,
    ) -> None:
        """
        Validate parameters for `submit` method

        Args:
            parameters: Parameters dictionary.
            task_type: Task type.s
        """
        logger.info("[validate_submit_parameters] START")
        if task_type not in TASK_TYPES_SUBMIT:
            raise ValueError(f"Invalid {task_type=} for `submit`.")
        if not isinstance(parameters, dict):
            raise ValueError("`parameters` must be a dictionary.")
        if task_type in ["non_parallel", "compound"]:
            if "zarr_urls" not in parameters.keys():
                raise ValueError(
                    f"No 'zarr_urls' key in in {list(parameters.keys())}"
                )
        elif task_type in ["converter_non_parallel", "converter_compound"]:
            if "zarr_urls" in parameters.keys():
                raise ValueError(
                    f"Forbidden 'zarr_urls' key in {list(parameters.keys())}"
                )
        logger.info("[validate_submit_parameters] END")

    def validate_multisubmit_parameters(
        self,
        list_parameters: list[dict[str, Any]],
        task_type: TaskTypeType,
        list_task_files: list[TaskFiles],
    ) -> None:
        """
        Validate parameters for `multi_submit` method

        Args:
            list_parameters: List of parameters dictionaries.
            task_type: Task type.
        """
        if task_type not in TASK_TYPES_MULTISUBMIT:
            raise ValueError(f"Invalid {task_type=} for `multisubmit`.")

        subfolders = set(
            task_file.wftask_subfolder_local for task_file in list_task_files
        )
        if len(subfolders) != 1:
            raise ValueError(f"More than one subfolders: {subfolders}.")

        if not isinstance(list_parameters, list):
            raise ValueError("`parameters` must be a list.")

        for single_kwargs in list_parameters:
            if not isinstance(single_kwargs, dict):
                raise ValueError("kwargs itemt must be a dictionary.")
            if "zarr_url" not in single_kwargs.keys():
                raise ValueError(
                    f"No 'zarr_url' key in in {list(single_kwargs.keys())}"
                )
        if task_type == "parallel":
            zarr_urls = [kwargs["zarr_url"] for kwargs in list_parameters]
            if len(zarr_urls) != len(set(zarr_urls)):
                raise ValueError("Non-unique zarr_urls")

    def validate_multisubmit_history_unit_ids(
        self,
        *,
        history_unit_ids: list[int],
        task_type: TaskTypeType,
        list_parameters: list[dict[str, Any]],
    ) -> None:
        if task_type in ["compound", "converter_compound"]:
            if len(history_unit_ids) != 1:
                raise NotImplementedError(
                    "We are breaking the assumption that compound/multisubmit "
                    "is associated to a single HistoryUnit. This is not "
                    "supported."
                )
            elif task_type == "parallel" and len(history_unit_ids) != len(
                list_parameters
            ):
                raise ValueError(
                    f"{len(history_unit_ids)=} differs from "
                    f"{len(list_parameters)=}."
                )
