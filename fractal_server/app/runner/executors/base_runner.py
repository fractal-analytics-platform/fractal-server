from enum import StrEnum
from typing import Any

from fractal_server.app.runner.task_files import TaskFiles
from fractal_server.app.schemas.v2.task import TaskType
from fractal_server.logger import set_logger


class SubmitTaskType(StrEnum):
    COMPOUND = TaskType.COMPOUND
    NON_PARALLEL = TaskType.NON_PARALLEL
    CONVERTER_NON_PARALLEL = TaskType.CONVERTER_NON_PARALLEL
    CONVERTER_COMPOUND = TaskType.CONVERTER_COMPOUND


class MultisubmitTaskType(StrEnum):
    PARALLEL = TaskType.PARALLEL
    COMPOUND = TaskType.COMPOUND
    CONVERTER_COMPOUND = TaskType.CONVERTER_COMPOUND


TASK_TYPES_SUBMIT: list[TaskType] = [
    TaskType.COMPOUND,
    TaskType.CONVERTER_COMPOUND,
    TaskType.NON_PARALLEL,
    TaskType.CONVERTER_NON_PARALLEL,
]
TASK_TYPES_MULTISUBMIT: list[TaskType] = [
    TaskType.COMPOUND,
    TaskType.CONVERTER_COMPOUND,
    TaskType.PARALLEL,
]

logger = set_logger(__name__)


class BaseRunner:
    """
    Base class for Fractal runners.
    """

    def submit(
        self,
        base_command: str,
        workflow_task_order: int,
        workflow_task_id: int,
        task_name: str,
        parameters: dict[str, Any],
        history_unit_id: int,
        task_type: TaskType,
        task_files: TaskFiles,
        config: Any,
        user_id: int,
    ) -> tuple[Any, BaseException]:
        """
        Run a single fractal task.

        Args:
            base_command:
            workflow_task_order:
            workflow_task_id:
            task_name:
            parameters: Dictionary of parameters.
            history_unit_id:
                Database ID of the corresponding `HistoryUnit` entry.
            task_type: Task type.
            task_files: `TaskFiles` object.
            config: Runner-specific parameters.
            user_id:
        """
        raise NotImplementedError()

    def multisubmit(
        self,
        base_command: str,
        workflow_task_order: int,
        workflow_task_id: int,
        task_name: str,
        list_parameters: list[dict[str, Any]],
        history_unit_ids: list[int],
        list_task_files: list[TaskFiles],
        task_type: TaskType,
        config: Any,
        user_id: int,
    ) -> tuple[dict[int, Any], dict[int, BaseException]]:
        """
        Run a parallel fractal task.

        Args:
            base_command:
            workflow_task_order:
            workflow_task_id:
            task_name:
            list_parameters:
                List of dictionaries of parameters (each one must include
                `zarr_urls` key).
            history_unit_ids:
                Database IDs of the corresponding `HistoryUnit` entries.
            list_task_files: `TaskFiles` objects.
            task_type: Task type.
            config: Runner-specific parameters.
            user_id:
        """
        raise NotImplementedError()

    def validate_submit_parameters(
        self,
        parameters: dict[str, Any],
        task_type: TaskType,
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
        if task_type in [
            TaskType.NON_PARALLEL,
            TaskType.COMPOUND,
        ]:
            if "zarr_urls" not in parameters.keys():
                raise ValueError(
                    f"No 'zarr_urls' key in in {list(parameters.keys())}"
                )
        elif task_type in [
            TaskType.CONVERTER_NON_PARALLEL,
            TaskType.CONVERTER_COMPOUND,
        ]:
            if "zarr_urls" in parameters.keys():
                raise ValueError(
                    f"Forbidden 'zarr_urls' key in {list(parameters.keys())}"
                )
        logger.info("[validate_submit_parameters] END")

    def validate_multisubmit_parameters(
        self,
        *,
        task_type: TaskType,
        list_parameters: list[dict[str, Any]],
        list_task_files: list[TaskFiles],
        history_unit_ids: list[int],
    ) -> None:
        """
        Validate parameters for `multisubmit` method

        Args:
            task_type: Task type.
            list_parameters: List of parameters dictionaries.
            list_task_files:
            history_unit_ids:
        """
        if task_type not in TASK_TYPES_MULTISUBMIT:
            raise ValueError(f"Invalid {task_type=} for `multisubmit`.")

        if not isinstance(list_parameters, list):
            raise ValueError("`parameters` must be a list.")

        if len(list_parameters) != len(list_task_files):
            raise ValueError(
                f"{len(list_task_files)=} differs from "
                f"{len(list_parameters)=}."
            )
        if len(history_unit_ids) != len(list_parameters):
            raise ValueError(
                f"{len(history_unit_ids)=} differs from "
                f"{len(list_parameters)=}."
            )

        subfolders = {
            task_file.wftask_subfolder_local for task_file in list_task_files
        }
        if len(subfolders) != 1:
            raise ValueError(f"More than one subfolders: {subfolders}.")

        for single_kwargs in list_parameters:
            if not isinstance(single_kwargs, dict):
                raise ValueError("kwargs itemt must be a dictionary.")
            if "zarr_url" not in single_kwargs.keys():
                raise ValueError(
                    f"No 'zarr_url' key in in {list(single_kwargs.keys())}"
                )
        if task_type == TaskType.PARALLEL:
            zarr_urls = [kwargs["zarr_url"] for kwargs in list_parameters]
            if len(zarr_urls) != len(set(zarr_urls)):
                raise ValueError("Non-unique zarr_urls")
