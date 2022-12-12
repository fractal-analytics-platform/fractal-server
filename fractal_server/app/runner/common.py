"""
Common utilities and routines for runner backends (public API)

This module includes utilities and routines that are of use to implement
runner backends but that should also be exposed to the other components of
`Fractal Server`.
"""
import asyncio
import json
from functools import partial
from functools import wraps
from json import JSONEncoder
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

from pydantic import BaseModel

from ...utils import close_logger as close_job_logger  # noqa F401
from ...utils import file_opener
from ..models import Dataset
from ..models import Project
from ..models.task import Task


class TaskExecutionError(RuntimeError):
    """
    Forwards any error occurred within the execution of a task

    This error wraps and forwards errors occurred during the execution of
    tasks, together with information that is useful to track down the failing
    task within a workflow.

    Attributes:
        workflow_task_id:
            ID of the workflow task that failed.
        workflow_task_order:
            Order of the task within the workflow.
        task_name:
            Human readable name of the failing task.
    """

    workflow_task_id: Optional[int] = None
    workflow_task_order: Optional[int] = None
    task_name: Optional[str] = None

    def __init__(
        self,
        *args,
        workflow_task_id: Optional[int] = None,
        workflow_task_order: Optional[int] = None,
        task_name: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.workflow_task_id = workflow_task_id
        self.workflow_task_order = workflow_task_order
        self.task_name = task_name


class TaskParameterEncoder(JSONEncoder):
    """
    Convenience JSONEncoder that serialises `Path`s as strings
    """

    def default(self, value):
        if isinstance(value, Path):
            return value.as_posix()
        return JSONEncoder.default(self, value)


class TaskParameters(BaseModel):
    """
    Wrapper for task input parameters

    Instances of this class are used to pass parameters from the output of a
    task to the input of the next one.

    Attributes:
        input_paths:
            Input paths as derived by the input dataset.
        output_paths:
            Output path as derived from the output dataset.
        metadata:
            Dataset metadata, as found in the input dataset or as updated by
            the previous task.
        logger_name:
            Identifier of the workflow logger.
        username:
            User to impersonate to run the workflow.
    """

    input_paths: List[Path]
    output_path: Path
    metadata: Dict[str, Any]
    logger_name: Optional[str] = None
    username: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True
        extra = "forbid"


async def auto_output_dataset(
    *,
    project: Project,
    input_dataset: Dataset,
    workflow: Task,
    overwrite_input: bool = False,
):
    """
    Determine the output dataset if it was not provided explicitly

    Only datasets containing exactly one path can be used as output.

    Note: This routine is still a stub.

    Args:
        project:
            The project that contains the input and output datasets.
        input_dataset:
            The input dataset.
        workflow:
            The workflow to be applied to the input dataset.
        overwrite_input:
            Whether it is allowed to overwrite the input dataset with the
            output data.

    Raises:
        ValueError: If the input dataset is to be overwritten and it provides
        more than one path.

    Returns:
        output_dataset:
            the output dataset
    """
    if overwrite_input and not input_dataset.read_only:
        input_paths = input_dataset.paths
        if len(input_paths) != 1:
            raise ValueError
        output_dataset = input_dataset
    else:
        raise NotImplementedError

    return output_dataset


def validate_workflow_compatibility(
    *,
    input_dataset: Dataset,
    workflow: Task,
    output_dataset: Optional[Dataset] = None,
):
    """
    Check compatibility of workflow and input / ouptut dataset
    """
    if (
        workflow.input_type != "Any"
        and workflow.input_type != input_dataset.type
    ):
        raise TypeError(
            f"Incompatible types `{workflow.input_type}` of workflow "
            f"`{workflow.name}` and `{input_dataset.type}` of dataset "
            f"`{input_dataset.name}`"
        )

    if not output_dataset:
        if input_dataset.read_only:
            raise ValueError("Input dataset is read-only")
        else:
            input_paths = input_dataset.paths
            if len(input_paths) != 1:
                # Only single input can be safely transformed in an output
                raise ValueError(
                    "Cannot determine output path: multiple input "
                    "paths to overwrite"
                )
            else:
                output_path = input_paths[0]
    else:

        if len(output_dataset.paths) != 1:
            raise ValueError(
                "Cannot determine output path: Multiple paths in dataset."
            )
        else:
            output_path = output_dataset.paths[0]
    return output_path


def async_wrap(func: Callable) -> Callable:
    """
    Wrap a synchronous callable in an async task

    Ref: [issue #140](https://github.com/fractal-analytics-platform/fractal-server/issues/140)
    and [this StackOverflow answer](https://stackoverflow.com/q/43241221/19085332).

    Returns:
        async_wrapper:
            A factory that allows wrapping a blocking callable within a
            coroutine.
    """  # noqa: E501

    @wraps(func)
    async def async_wrapper(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return async_wrapper


def write_args_file(*args: Dict[str, Any], path: Path):
    """
    Merge arbitrary dictionaries and write to file

    Args:
        *args:
            One or more dictionaries that will be merged into one respecting
            the order with which they are passed in, i.e., last in overrides
            previous ones.
        path:
            Destination for serialised file.
    """
    out = {}
    for d in args:
        out.update(d)
    with open(path, "w", opener=file_opener) as f:
        json.dump(out, f, cls=TaskParameterEncoder, indent=4)
