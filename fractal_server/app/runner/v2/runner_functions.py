import functools
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict

from ..exceptions import JobExecutionError
from ..exceptions import TaskOutputValidationError
from .db_tools import update_status_of_history_unit
from .deduplicate_list import deduplicate_list
from .runner_functions_low_level import run_single_task
from .task_interface import InitTaskOutput
from .task_interface import TaskOutput
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.runner.components import _index_to_component
from fractal_server.app.runner.executors.base_runner import BaseRunner
from fractal_server.app.runner.task_files import TaskFiles
from fractal_server.app.runner.v2.db_tools import bulk_upsert_image_cache_fast
from fractal_server.app.runner.v2.task_interface import (
    _cast_and_validate_InitTaskOutput,
)
from fractal_server.app.runner.v2.task_interface import (
    _cast_and_validate_TaskOutput,
)
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.logger import set_logger

__all__ = [
    "run_v2_task_parallel",
    "run_v2_task_non_parallel",
    "run_v2_task_compound",
]


logger = set_logger(__name__)


class SubmissionOutcome(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    task_output: TaskOutput | None = None
    exception: BaseException | None = None


class InitSubmissionOutcome(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    task_output: InitTaskOutput | None = None
    exception: BaseException | None = None


MAX_PARALLELIZATION_LIST_SIZE = 20_000


def _process_task_output(
    *,
    result: dict[str, Any] | None = None,
    exception: BaseException | None = None,
) -> SubmissionOutcome:
    if exception is not None:
        task_output = None
    else:
        if result is None:
            task_output = TaskOutput()
        else:
            try:
                task_output = _cast_and_validate_TaskOutput(result)
            except TaskOutputValidationError as e:
                # FIXME: This should correspond to some status="failed",
                # but it does not
                task_output = None
                exception = e
    return SubmissionOutcome(
        task_output=task_output,
        exception=exception,
    )


def _process_init_task_output(
    *,
    result: dict[str, Any] | None = None,
    exception: BaseException | None = None,
) -> SubmissionOutcome:
    if exception is not None:
        task_output = None
    else:
        if result is None:
            task_output = InitTaskOutput()
        else:
            try:
                task_output = _cast_and_validate_InitTaskOutput(result)
            except TaskOutputValidationError as e:
                # FIXME: This should correspond to some status="failed",
                # but it does not
                task_output = None
                exception = e
    return InitSubmissionOutcome(
        task_output=task_output,
        exception=exception,
    )


def _check_parallelization_list_size(my_list):
    if len(my_list) > MAX_PARALLELIZATION_LIST_SIZE:
        raise JobExecutionError(
            "Too many parallelization items.\n"
            f"   {len(my_list)}\n"
            f"   {MAX_PARALLELIZATION_LIST_SIZE=}\n"
        )


def run_v2_task_non_parallel(
    *,
    images: list[dict[str, Any]],
    zarr_dir: str,
    task: TaskV2,
    wftask: WorkflowTaskV2,
    workflow_dir_local: Path,
    workflow_dir_remote: Path,
    runner: BaseRunner,
    get_runner_config: Callable[
        [
            WorkflowTaskV2,
            Literal["non_parallel", "parallel"],
            Optional[Path],
        ],
        Any,
    ],
    dataset_id: int,
    history_run_id: int,
    task_type: Literal["non_parallel", "converter_non_parallel"],
) -> tuple[dict[int, SubmissionOutcome], int]:
    """
    This runs server-side (see `executor` argument)
    """

    if task_type not in ["non_parallel", "converter_non_parallel"]:
        raise ValueError(
            f"Invalid {task_type=} for `run_v2_task_non_parallel`."
        )

    # Get TaskFiles object
    task_files = TaskFiles(
        root_dir_local=workflow_dir_local,
        root_dir_remote=workflow_dir_remote,
        task_order=wftask.order,
        task_name=wftask.task.name,
        component=_index_to_component(0),
    )

    runner_config = get_runner_config(wftask=wftask, which_type="non_parallel")

    function_kwargs = {
        "zarr_dir": zarr_dir,
        **(wftask.args_non_parallel or {}),
    }
    if task_type == "non_parallel":
        function_kwargs["zarr_urls"] = [img["zarr_url"] for img in images]

    # Database History operations
    with next(get_sync_db()) as db:
        if task_type == "non_parallel":
            zarr_urls = function_kwargs["zarr_urls"]
        elif task_type == "converter_non_parallel":
            zarr_urls = []

        history_unit = HistoryUnit(
            history_run_id=history_run_id,
            status=HistoryUnitStatus.SUBMITTED,
            logfile=None,
            zarr_urls=zarr_urls,
        )
        db.add(history_unit)
        db.commit()
        db.refresh(history_unit)
        history_unit_id = history_unit.id
        bulk_upsert_image_cache_fast(
            db=db,
            list_upsert_objects=[
                dict(
                    workflowtask_id=wftask.id,
                    dataset_id=dataset_id,
                    zarr_url=zarr_url,
                    latest_history_unit_id=history_unit_id,
                )
                for zarr_url in history_unit.zarr_urls
            ],
        )

    result, exception = runner.submit(
        functools.partial(
            run_single_task,
            command=task.command_non_parallel,
            workflow_task_order=wftask.order,
            workflow_task_id=wftask.task_id,
            task_name=wftask.task.name,
        ),
        parameters=function_kwargs,
        task_type=task_type,
        task_files=task_files,
        history_unit_id=history_unit_id,
        config=runner_config,
    )

    positional_index = 0
    num_tasks = 1

    outcome = {
        positional_index: _process_task_output(
            result=result,
            exception=exception,
        )
    }
    return outcome, num_tasks


def run_v2_task_parallel(
    *,
    images: list[dict[str, Any]],
    task: TaskV2,
    wftask: WorkflowTaskV2,
    runner: BaseRunner,
    workflow_dir_local: Path,
    workflow_dir_remote: Path,
    get_runner_config: Callable[
        [
            WorkflowTaskV2,
            Literal["non_parallel", "parallel"],
            Optional[Path],
        ],
        Any,
    ],
    dataset_id: int,
    history_run_id: int,
) -> tuple[dict[int, SubmissionOutcome], int]:
    if len(images) == 0:
        return {}, 0

    _check_parallelization_list_size(images)

    # Get TaskFiles object
    task_files = TaskFiles(
        root_dir_local=workflow_dir_local,
        root_dir_remote=workflow_dir_remote,
        task_order=wftask.order,
        task_name=wftask.task.name,
    )

    runner_config = get_runner_config(
        wftask=wftask,
        which_type="parallel",
    )

    list_function_kwargs = [
        {
            "zarr_url": image["zarr_url"],
            **(wftask.args_parallel or {}),
        }
        for image in images
    ]
    list_task_files = [
        TaskFiles(
            **task_files.model_dump(exclude={"component"}),
            component=_index_to_component(ind),
        )
        for ind in range(len(images))
    ]

    history_units = [
        HistoryUnit(
            history_run_id=history_run_id,
            status=HistoryUnitStatus.SUBMITTED,
            logfile=None,
            zarr_urls=[image["zarr_url"]],
        )
        for ind, image in enumerate(images)
    ]

    with next(get_sync_db()) as db:
        db.add_all(history_units)
        db.commit()

        for history_unit in history_units:
            db.refresh(history_unit)
        history_unit_ids = [history_unit.id for history_unit in history_units]

        history_image_caches = [
            dict(
                workflowtask_id=wftask.id,
                dataset_id=dataset_id,
                zarr_url=history_unit.zarr_urls[0],
                latest_history_unit_id=history_unit.id,
            )
            for history_unit in history_units
        ]

        bulk_upsert_image_cache_fast(
            db=db, list_upsert_objects=history_image_caches
        )

    results, exceptions = runner.multisubmit(
        functools.partial(
            run_single_task,
            command=task.command_parallel,
            workflow_task_order=wftask.order,
            workflow_task_id=wftask.task_id,
            task_name=wftask.task.name,
        ),
        list_parameters=list_function_kwargs,
        task_type="parallel",
        list_task_files=list_task_files,
        history_unit_ids=history_unit_ids,
        config=runner_config,
    )

    outcome = {}
    for ind in range(len(list_function_kwargs)):
        if ind not in results.keys() and ind not in exceptions.keys():
            # FIXME: Could we avoid this branch?
            error_msg = (
                f"Invalid branch: {ind=} is not in `results.keys()` "
                "nor in `exceptions.keys()`."
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        outcome[ind] = _process_task_output(
            result=results.get(ind, None),
            exception=exceptions.get(ind, None),
        )

    num_tasks = len(images)
    return outcome, num_tasks


# FIXME: THIS FOR CONVERTERS:
# if task_type in ["converter_non_parallel"]:
#     run = db.get(HistoryRun, history_run_id)
#     run.status = HistoryUnitStatus.DONE
#     db.merge(run)
#     db.commit()


def run_v2_task_compound(
    *,
    images: list[dict[str, Any]],
    zarr_dir: str,
    task: TaskV2,
    wftask: WorkflowTaskV2,
    runner: BaseRunner,
    workflow_dir_local: Path,
    workflow_dir_remote: Path,
    get_runner_config: Callable[
        [
            WorkflowTaskV2,
            Literal["non_parallel", "parallel"],
            Optional[Path],
        ],
        Any,
    ],
    dataset_id: int,
    history_run_id: int,
    task_type: Literal["compound", "converter_compound"],
) -> tuple[dict[int, SubmissionOutcome], int]:
    # Get TaskFiles object
    task_files_init = TaskFiles(
        root_dir_local=workflow_dir_local,
        root_dir_remote=workflow_dir_remote,
        task_order=wftask.order,
        task_name=wftask.task.name,
        component=_index_to_component(0),
    )

    runner_config_init = get_runner_config(
        wftask=wftask,
        which_type="non_parallel",
    )
    runner_config_compute = get_runner_config(
        wftask=wftask,
        which_type="parallel",
    )

    # 3/A: non-parallel init task
    function_kwargs = {
        "zarr_dir": zarr_dir,
        **(wftask.args_non_parallel or {}),
    }
    if task_type == "compound":
        function_kwargs["zarr_urls"] = [img["zarr_url"] for img in images]
        input_image_zarr_urls = function_kwargs["zarr_urls"]
    elif task_type == "converter_compound":
        input_image_zarr_urls = []

    # Create database History entries
    with next(get_sync_db()) as db:
        # Create a single `HistoryUnit` for the whole compound task
        history_unit = HistoryUnit(
            history_run_id=history_run_id,
            status=HistoryUnitStatus.SUBMITTED,
            logfile=None,
            zarr_urls=input_image_zarr_urls,
        )
        db.add(history_unit)
        db.commit()
        db.refresh(history_unit)
        history_unit_id = history_unit.id
        # Create one `HistoryImageCache` for each input image
        bulk_upsert_image_cache_fast(
            db=db,
            list_upsert_objects=[
                dict(
                    workflowtask_id=wftask.id,
                    dataset_id=dataset_id,
                    zarr_url=zarr_url,
                    latest_history_unit_id=history_unit_id,
                )
                for zarr_url in input_image_zarr_urls
            ],
        )
    result, exception = runner.submit(
        functools.partial(
            run_single_task,
            command=task.command_non_parallel,
            workflow_task_order=wftask.order,
            workflow_task_id=wftask.task_id,
            task_name=wftask.task.name,
        ),
        parameters=function_kwargs,
        task_type=task_type,
        task_files=task_files_init,
        history_unit_id=history_unit_id,
        config=runner_config_init,
    )

    init_outcome = _process_init_task_output(
        result=result,
        exception=exception,
    )
    num_tasks = 1
    if init_outcome.exception is not None:
        positional_index = 0
        return (
            {
                positional_index: SubmissionOutcome(
                    exception=init_outcome.exception
                )
            },
            num_tasks,
        )

    parallelization_list = init_outcome.task_output.parallelization_list
    parallelization_list = deduplicate_list(parallelization_list)

    num_tasks = 1 + len(parallelization_list)

    # 3/B: parallel part of a compound task
    _check_parallelization_list_size(parallelization_list)

    if len(parallelization_list) == 0:
        with next(get_sync_db()) as db:
            update_status_of_history_unit(
                history_unit_id=history_unit_id,
                status=HistoryUnitStatus.DONE,
                db_sync=db,
            )
        positional_index = 0
        init_outcome = {
            positional_index: _process_task_output(
                result=None,
                exception=None,
            )
        }
        return init_outcome, num_tasks

    list_task_files = [
        TaskFiles(
            root_dir_local=workflow_dir_local,
            root_dir_remote=workflow_dir_remote,
            task_order=wftask.order,
            task_name=wftask.task.name,
            component=_index_to_component(ind),
        )
        for ind in range(len(parallelization_list))
    ]
    list_function_kwargs = [
        {
            "zarr_url": parallelization_item.zarr_url,
            "init_args": parallelization_item.init_args,
            **(wftask.args_parallel or {}),
        }
        for parallelization_item in parallelization_list
    ]

    results, exceptions = runner.multisubmit(
        functools.partial(
            run_single_task,
            command=task.command_parallel,
            workflow_task_order=wftask.order,
            workflow_task_id=wftask.task_id,
            task_name=wftask.task.name,
        ),
        list_parameters=list_function_kwargs,
        task_type=task_type,
        list_task_files=list_task_files,
        history_unit_ids=[history_unit_id],
        config=runner_config_compute,
    )

    init_outcome = {}
    failure = False
    for ind in range(len(list_function_kwargs)):
        if ind not in results.keys() and ind not in exceptions.keys():
            # FIXME: Could we avoid this branch?
            error_msg = (
                f"Invalid branch: {ind=} is not in `results.keys()` "
                "nor in `exceptions.keys()`."
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        init_outcome[ind] = _process_task_output(
            result=results.get(ind, None),
            exception=exceptions.get(ind, None),
        )

    # FIXME: In this case, we are performing db updates from here, rather
    # than at lower level.
    with next(get_sync_db()) as db:
        if failure:
            update_status_of_history_unit(
                history_unit_id=history_unit_id,
                status=HistoryUnitStatus.FAILED,
                db_sync=db,
            )
        else:
            update_status_of_history_unit(
                history_unit_id=history_unit_id,
                status=HistoryUnitStatus.DONE,
                db_sync=db,
            )

    return init_outcome, num_tasks
