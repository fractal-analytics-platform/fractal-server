import functools
import logging
from pathlib import Path
from typing import Any
from typing import Literal
from typing import Optional

from pydantic import ValidationError
from sqlmodel import update

from ..exceptions import JobExecutionError
from .deduplicate_list import deduplicate_list
from .merge_outputs import merge_outputs
from .runner_functions_low_level import run_single_task
from .task_interface import InitTaskOutput
from .task_interface import TaskOutput
from fractal_server.app.db import get_sync_db
from fractal_server.app.history.status_enum import XXXStatus
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.runner.components import _COMPONENT_KEY_
from fractal_server.app.runner.components import _index_to_component
from fractal_server.app.runner.executors.base_runner import BaseRunner
from fractal_server.app.runner.v2._db_tools import bulk_upsert_image_cache_fast


__all__ = [
    "run_v2_task_non_parallel",
    "run_v2_task_parallel",
    "run_v2_task_compound",
]

MAX_PARALLELIZATION_LIST_SIZE = 20_000


def _cast_and_validate_TaskOutput(
    task_output: dict[str, Any]
) -> Optional[TaskOutput]:
    try:
        validated_task_output = TaskOutput(**task_output)
        return validated_task_output
    except ValidationError as e:
        raise JobExecutionError(
            "Validation of task output failed.\n"
            f"Original error: {str(e)}\n"
            f"Original data: {task_output}."
        )


def _cast_and_validate_InitTaskOutput(
    init_task_output: dict[str, Any],
) -> Optional[InitTaskOutput]:
    try:
        validated_init_task_output = InitTaskOutput(**init_task_output)
        return validated_init_task_output
    except ValidationError as e:
        raise JobExecutionError(
            "Validation of init-task output failed.\n"
            f"Original error: {str(e)}\n"
            f"Original data: {init_task_output}."
        )


def no_op_submit_setup_call(
    *,
    wftask: WorkflowTaskV2,
    root_dir_local: Path,
    which_type: Literal["non_parallel", "parallel"],
) -> dict[str, Any]:
    """
    Default (no-operation) interface of submit_setup_call in V2.
    """
    return {}


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
    workflow_dir_remote: Optional[Path] = None,
    executor: BaseRunner,
    submit_setup_call: callable = no_op_submit_setup_call,
    dataset_id: int,
    history_run_id: int,
) -> tuple[TaskOutput, int, dict[int, BaseException]]:
    """
    This runs server-side (see `executor` argument)
    """

    if workflow_dir_remote is None:
        workflow_dir_remote = workflow_dir_local
        logging.warning(
            "In `run_single_task`, workflow_dir_remote=None. Is this right?"
        )
        workflow_dir_remote = workflow_dir_local

    executor_options = submit_setup_call(
        wftask=wftask,
        root_dir_local=workflow_dir_local,
        root_dir_remote=workflow_dir_remote,
        which_type="non_parallel",
    )

    function_kwargs = {
        "zarr_urls": [image["zarr_url"] for image in images],
        "zarr_dir": zarr_dir,
        _COMPONENT_KEY_: _index_to_component(0),
        **(wftask.args_non_parallel or {}),
    }

    # Database History operations
    with next(get_sync_db()) as db:
        history_unit = HistoryUnit(
            history_run_id=history_run_id,
            status=XXXStatus.SUBMITTED,
            logfile=None,  # FIXME
            zarr_urls=function_kwargs["zarr_urls"],
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

    result, exception = executor.submit(
        functools.partial(
            run_single_task,
            wftask=wftask,
            command=task.command_non_parallel,
            root_dir_local=workflow_dir_local,
            root_dir_remote=workflow_dir_remote,
        ),
        parameters=function_kwargs,
        task_type="non_parallel",
        **executor_options,
    )

    num_tasks = 1
    with next(get_sync_db()) as db:
        if exception is None:
            db.execute(
                update(HistoryUnit)
                .where(HistoryUnit.id == history_unit_id)
                .values(status=XXXStatus.DONE)
            )
            db.commit()
            if result is None:
                return (TaskOutput(), num_tasks, {})
            else:
                return (_cast_and_validate_TaskOutput(result), num_tasks, {})
        else:
            db.execute(
                update(HistoryUnit)
                .where(HistoryUnit.id == history_unit_id)
                .values(status=XXXStatus.FAILED)
            )
            db.commit()
            return (TaskOutput(), num_tasks, {0: exception})


def run_v2_task_converter_non_parallel(
    *,
    zarr_dir: str,
    task: TaskV2,
    wftask: WorkflowTaskV2,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    executor: BaseRunner,
    submit_setup_call: callable = no_op_submit_setup_call,
    dataset_id: int,
    history_run_id: int,
) -> tuple[TaskOutput, int, dict[int, BaseException]]:
    """
    This runs server-side (see `executor` argument)
    """

    if workflow_dir_remote is None:
        workflow_dir_remote = workflow_dir_local
        logging.warning(
            "In `run_single_task`, workflow_dir_remote=None. Is this right?"
        )
        workflow_dir_remote = workflow_dir_local

    executor_options = submit_setup_call(
        wftask=wftask,
        root_dir_local=workflow_dir_local,
        root_dir_remote=workflow_dir_remote,
        which_type="non_parallel",
    )

    function_kwargs = {
        "zarr_dir": zarr_dir,
        _COMPONENT_KEY_: _index_to_component(0),
        **(wftask.args_non_parallel or {}),
    }

    # Database History operations
    with next(get_sync_db()) as db:
        history_unit = HistoryUnit(
            history_run_id=history_run_id,
            status=XXXStatus.SUBMITTED,
            logfile=None,  # FIXME
            zarr_urls=[],
        )
        db.add(history_unit)
        db.commit()
        db.refresh(history_unit)
        history_unit_id = history_unit.id

    result, exception = executor.submit(
        functools.partial(
            run_single_task,
            wftask=wftask,
            command=task.command_non_parallel,
            root_dir_local=workflow_dir_local,
            root_dir_remote=workflow_dir_remote,
        ),
        task_type="converter_non_parallel",
        parameters=function_kwargs,
        **executor_options,
    )

    num_tasks = 1
    with next(get_sync_db()) as db:
        if exception is None:
            db.execute(
                update(HistoryUnit)
                .where(HistoryUnit.id == history_unit_id)
                .values(status=XXXStatus.DONE)
            )
            db.commit()
            if result is None:
                return (TaskOutput(), num_tasks, {})
            else:
                return (_cast_and_validate_TaskOutput(result), num_tasks, {})
        else:
            db.execute(
                update(HistoryUnit)
                .where(HistoryUnit.id == history_unit_id)
                .values(status=XXXStatus.FAILED)
            )
            db.commit()
            return (TaskOutput(), num_tasks, {0: exception})


def run_v2_task_parallel(
    *,
    images: list[dict[str, Any]],
    task: TaskV2,
    wftask: WorkflowTaskV2,
    executor: BaseRunner,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    submit_setup_call: callable = no_op_submit_setup_call,
    dataset_id: int,
    history_run_id: int,
) -> tuple[TaskOutput, int, dict[int, BaseException]]:
    if len(images) == 0:
        # FIXME: Do something with history units/images?
        return (TaskOutput(), 0, {})

    _check_parallelization_list_size(images)

    executor_options = submit_setup_call(
        wftask=wftask,
        root_dir_local=workflow_dir_local,
        root_dir_remote=workflow_dir_remote,
        which_type="parallel",
    )

    list_function_kwargs = [
        {
            "zarr_url": image["zarr_url"],
            _COMPONENT_KEY_: _index_to_component(ind),
            **(wftask.args_parallel or {}),
        }
        for ind, image in enumerate(images)
    ]
    history_units = [
        HistoryUnit(
            history_run_id=history_run_id,
            status=XXXStatus.SUBMITTED,
            logfile=None,  # FIXME
            zarr_urls=[image["zarr_url"]],
        )
        for image in images
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

    results, exceptions = executor.multisubmit(
        functools.partial(
            run_single_task,
            wftask=wftask,
            command=task.command_parallel,
            root_dir_local=workflow_dir_local,
            root_dir_remote=workflow_dir_remote,
        ),
        list_parameters=list_function_kwargs,
        task_type="parallel",
        **executor_options,
    )

    outputs = []
    history_unit_ids_done: list[int] = []
    history_unit_ids_failed: list[int] = []
    for ind in range(len(list_function_kwargs)):
        if ind in results.keys():
            result = results[ind]
            if result is None:
                output = TaskOutput()
            else:
                output = _cast_and_validate_TaskOutput(result)
            outputs.append(output)
            history_unit_ids_done.append(history_unit_ids[ind])
        elif ind in exceptions.keys():
            print(f"Bad: {exceptions[ind]}")
            history_unit_ids_failed.append(history_unit_ids[ind])
        else:
            print("VERY BAD - should have not reached this point")

    with next(get_sync_db()) as db:
        db.execute(
            update(HistoryUnit)
            .where(HistoryUnit.id.in_(history_unit_ids_done))
            .values(status=XXXStatus.DONE)
        )
        db.execute(
            update(HistoryUnit)
            .where(HistoryUnit.id.in_(history_unit_ids_failed))
            .values(status=XXXStatus.FAILED)
        )
        db.commit()

    num_tasks = len(images)
    merged_output = merge_outputs(outputs)
    return (merged_output, num_tasks, exceptions)


def run_v2_task_compound(
    *,
    images: list[dict[str, Any]],
    zarr_dir: str,
    task: TaskV2,
    wftask: WorkflowTaskV2,
    executor: BaseRunner,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    submit_setup_call: callable = no_op_submit_setup_call,
    dataset_id: int,
    history_run_id: int,
) -> tuple[TaskOutput, int, dict[int, BaseException]]:
    executor_options_init = submit_setup_call(
        wftask=wftask,
        root_dir_local=workflow_dir_local,
        root_dir_remote=workflow_dir_remote,
        which_type="non_parallel",
    )
    executor_options_compute = submit_setup_call(
        wftask=wftask,
        root_dir_local=workflow_dir_local,
        root_dir_remote=workflow_dir_remote,
        which_type="parallel",
    )

    # 3/A: non-parallel init task
    function_kwargs = {
        "zarr_urls": [image["zarr_url"] for image in images],
        "zarr_dir": zarr_dir,
        _COMPONENT_KEY_: f"init_{_index_to_component(0)}",
        **(wftask.args_non_parallel or {}),
    }

    # Create database History entries
    input_image_zarr_urls = function_kwargs["zarr_urls"]
    with next(get_sync_db()) as db:
        # Create a single `HistoryUnit` for the whole compound task
        history_unit = HistoryUnit(
            history_run_id=history_run_id,
            status=XXXStatus.SUBMITTED,
            logfile=None,  # FIXME
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

    result, exception = executor.submit(
        functools.partial(
            run_single_task,
            wftask=wftask,
            command=task.command_non_parallel,
            root_dir_local=workflow_dir_local,
            root_dir_remote=workflow_dir_remote,
        ),
        parameters=function_kwargs,
        task_type="compound",
        **executor_options_init,
    )

    num_tasks = 1
    if exception is None:
        if result is None:
            init_task_output = InitTaskOutput()
        else:
            init_task_output = _cast_and_validate_InitTaskOutput(result)
    else:
        with next(get_sync_db()) as db:
            db.execute(
                update(HistoryUnit)
                .where(HistoryUnit.id == history_unit_id)
                .values(status=XXXStatus.FAILED)
            )
            db.commit()
        return (TaskOutput(), num_tasks, {0: exception})

    parallelization_list = init_task_output.parallelization_list
    parallelization_list = deduplicate_list(parallelization_list)

    num_tasks = 1 + len(parallelization_list)

    # 3/B: parallel part of a compound task
    _check_parallelization_list_size(parallelization_list)

    if len(parallelization_list) == 0:
        with next(get_sync_db()) as db:
            db.execute(
                update(HistoryUnit)
                .where(HistoryUnit.id == history_unit_id)
                .values(status=XXXStatus.DONE)
            )
            db.commit()
        return (TaskOutput(), 0, {})

    list_function_kwargs = [
        {
            "zarr_url": parallelization_item.zarr_url,
            "init_args": parallelization_item.init_args,
            _COMPONENT_KEY_: f"compute_{_index_to_component(ind)}",
            **(wftask.args_parallel or {}),
        }
        for ind, parallelization_item in enumerate(parallelization_list)
    ]

    results, exceptions = executor.multisubmit(
        functools.partial(
            run_single_task,
            wftask=wftask,
            command=task.command_parallel,
            root_dir_local=workflow_dir_local,
            root_dir_remote=workflow_dir_remote,
        ),
        list_parameters=list_function_kwargs,
        task_type="compound",
        **executor_options_compute,
    )

    outputs = []
    failure = False
    for ind in range(len(list_function_kwargs)):
        if ind in results.keys():
            result = results[ind]
            if result is None:
                output = TaskOutput()
            else:
                output = _cast_and_validate_TaskOutput(result)
            outputs.append(output)

        elif ind in exceptions.keys():
            print(f"Bad: {exceptions[ind]}")
            failure = True
        else:
            print("VERY BAD - should have not reached this point")

    with next(get_sync_db()) as db:
        if failure:
            db.execute(
                update(HistoryUnit)
                .where(HistoryUnit.id == history_unit_id)
                .values(status=XXXStatus.FAILED)
            )
        else:
            db.execute(
                update(HistoryUnit)
                .where(HistoryUnit.id == history_unit_id)
                .values(status=XXXStatus.DONE)
            )
        db.commit()

    merged_output = merge_outputs(outputs)
    return (merged_output, num_tasks, exceptions)


def run_v2_task_converter_compound(
    *,
    zarr_dir: str,
    task: TaskV2,
    wftask: WorkflowTaskV2,
    executor: BaseRunner,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    submit_setup_call: callable = no_op_submit_setup_call,
    dataset_id: int,
    history_run_id: int,
) -> tuple[TaskOutput, int, dict[int, BaseException]]:
    executor_options_init = submit_setup_call(
        wftask=wftask,
        root_dir_local=workflow_dir_local,
        root_dir_remote=workflow_dir_remote,
        which_type="non_parallel",
    )
    executor_options_compute = submit_setup_call(
        wftask=wftask,
        root_dir_local=workflow_dir_local,
        root_dir_remote=workflow_dir_remote,
        which_type="parallel",
    )

    # 3/A: non-parallel init task
    function_kwargs = {
        "zarr_dir": zarr_dir,
        _COMPONENT_KEY_: f"init_{_index_to_component(0)}",
        **(wftask.args_non_parallel or {}),
    }

    # Create database History entries
    with next(get_sync_db()) as db:
        # Create a single `HistoryUnit` for the whole compound task
        history_unit = HistoryUnit(
            history_run_id=history_run_id,
            status=XXXStatus.SUBMITTED,
            logfile=None,  # FIXME
            zarr_urls=[],
        )
        db.add(history_unit)
        db.commit()
        db.refresh(history_unit)
        history_unit_id = history_unit.id

    result, exception = executor.submit(
        functools.partial(
            run_single_task,
            wftask=wftask,
            command=task.command_non_parallel,
            root_dir_local=workflow_dir_local,
            root_dir_remote=workflow_dir_remote,
        ),
        parameters=function_kwargs,
        task_type="converter_compound",
        **executor_options_init,
    )

    num_tasks = 1
    if exception is None:
        if result is None:
            init_task_output = InitTaskOutput()
        else:
            init_task_output = _cast_and_validate_InitTaskOutput(result)
    else:
        with next(get_sync_db()) as db:
            db.execute(
                update(HistoryUnit)
                .where(HistoryUnit.id == history_unit_id)
                .values(status=XXXStatus.FAILED)
            )
            db.commit()
        return (TaskOutput(), num_tasks, {0: exception})

    parallelization_list = init_task_output.parallelization_list
    parallelization_list = deduplicate_list(parallelization_list)

    num_tasks = 1 + len(parallelization_list)

    # 3/B: parallel part of a compound task
    _check_parallelization_list_size(parallelization_list)

    if len(parallelization_list) == 0:
        with next(get_sync_db()) as db:
            db.execute(
                update(HistoryUnit)
                .where(HistoryUnit.id == history_unit_id)
                .values(status=XXXStatus.DONE)
            )
            db.commit()
        return (TaskOutput(), 0, {})

    list_function_kwargs = [
        {
            "zarr_url": parallelization_item.zarr_url,
            "init_args": parallelization_item.init_args,
            _COMPONENT_KEY_: f"compute_{_index_to_component(ind)}",
            **(wftask.args_parallel or {}),
        }
        for ind, parallelization_item in enumerate(parallelization_list)
    ]

    results, exceptions = executor.multisubmit(
        functools.partial(
            run_single_task,
            wftask=wftask,
            command=task.command_parallel,
            root_dir_local=workflow_dir_local,
            root_dir_remote=workflow_dir_remote,
        ),
        list_parameters=list_function_kwargs,
        task_type="converter_compound",
        **executor_options_compute,
    )

    outputs = []
    failure = False
    for ind in range(len(list_function_kwargs)):
        if ind in results.keys():
            result = results[ind]
            if result is None:
                output = TaskOutput()
            else:
                output = _cast_and_validate_TaskOutput(result)
            outputs.append(output)

        elif ind in exceptions.keys():
            print(f"Bad: {exceptions[ind]}")
            failure = True
        else:
            print("VERY BAD - should have not reached this point")

    with next(get_sync_db()) as db:
        if failure:
            db.execute(
                update(HistoryUnit)
                .where(HistoryUnit.id == history_unit_id)
                .values(status=XXXStatus.FAILED)
            )
        else:
            db.execute(
                update(HistoryUnit)
                .where(HistoryUnit.id == history_unit_id)
                .values(status=XXXStatus.DONE)
            )
        db.commit()

    merged_output = merge_outputs(outputs)
    return (merged_output, num_tasks, exceptions)
