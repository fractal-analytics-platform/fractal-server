import importlib
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional
from typing import Union

from parsl.app.python import PythonApp
from parsl.dataflow.futures import AppFuture

from ..models.task import Task


def _task_fun(
    *,
    task: Task,
    input_paths: List[Path],
    output_path: Path,
    metadata: Optional[Dict[str, Any]],
    task_args: Optional[Dict[str, Any]],
    inputs,
):
    task_module = importlib.import_module(task.import_path)
    _callable = getattr(task_module, task.callable)
    metadata_update = _callable(
        input_paths=input_paths,
        output_path=output_path,
        metadata=metadata,
        **task_args,
    )
    metadata.update(metadata_update)
    try:
        metadata["history"].append(task.name)
    except KeyError:
        metadata["history"] = [task.name]
    return metadata


def _task_app(
    *,
    task: Task,
    input_paths: List[Path],
    output_path: Path,
    metadata: Optional[Dict[str, Any]],
    task_args: Optional[Dict[str, Any]],
    inputs,
    executors: Union[List[str], Literal["all"]] = "all",
) -> AppFuture:

    app = PythonApp(_task_fun, executors=executors)
    # TODO: can we reassign app.__name__, for clarity in monitoring?
    return app(
        task=task,
        input_paths=input_paths,
        output_path=output_path,
        metadata=metadata,
        task_args=task_args,
        inputs=inputs,
    )


def _task_parallel_fun(
    *,
    task: Task,
    component: str,
    input_paths: List[Path],
    output_path: Path,
    metadata: Optional[Dict[str, Any]],
    task_args: Optional[Dict[str, Any]],
    inputs,
):

    task_module = importlib.import_module(task.import_path)
    _callable = getattr(task_module, task.callable)
    _callable(
        input_paths=input_paths,
        output_path=output_path,
        metadata=metadata,
        component=component,
        **task_args,
    )
    return task.name, component


def _task_parallel_app(
    *,
    task: Task,
    component: str,
    input_paths: List[Path],
    output_path: Path,
    metadata: Optional[Dict[str, Any]],
    task_args: Optional[Dict[str, Any]],
    inputs,
    executors: Union[List[str], Literal["all"]] = "all",
) -> AppFuture:

    app = PythonApp(_task_parallel_fun, executors=executors)
    return app(
        task=task,
        component=component,
        input_paths=input_paths,
        output_path=output_path,
        metadata=metadata,
        task_args=task_args,
        inputs=inputs,
    )


def _collect_results_fun(
    *,
    metadata: Dict[str, Any],
    inputs: List[AppFuture],
):
    task_name = inputs[0][0]
    component_list = [_in[1] for _in in inputs]
    history = f"{task_name}: {component_list}"
    try:
        metadata["history"].append(history)
    except KeyError:
        metadata["history"] = [history]
    return metadata


def _collect_results_app(
    *,
    metadata: Dict[str, Any],
    inputs: List[AppFuture],
    executors: Union[List[str], Literal["all"]] = "all",
) -> AppFuture:
    app = PythonApp(_collect_results_fun, executors=executors)
    return app(metadata=metadata, inputs=inputs)
